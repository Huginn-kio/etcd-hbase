package io.etcdhbase.hbase.connection;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Bounded, identity-scoped manager for official HBase {@link AsyncConnection}s.
 *
 * <p>Entries are retained until this manager closes. The hard limit rejects new identities instead
 * of evicting a connection that may still be serving requests. Failed creations are removed so a
 * later request can retry. The configuration constructor is only for explicit development no-auth
 * identities; target authentication requires a provider-specific connection creator after the
 * target mechanism is known. Each entry serializes connection publication with shutdown while
 * keeping future callbacks outside the global manager lock.
 */
public final class OfficialHBaseConnectionFactory implements HBaseConnectionFactory {
  private static final System.Logger LOGGER =
      System.getLogger(OfficialHBaseConnectionFactory.class.getName());

  private final Object lock = new Object();
  private final int maxConnections;
  private final Duration creationTimeout;
  private final ScheduledThreadPoolExecutor deadlines;
  private final Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
      connectionCreator;
  private final Supplier<CompletableFuture<AsyncConnection>> connectionFutureFactory;
  private final Map<IdentityKey, Entry> connections = new HashMap<>();
  private boolean closed;

  /** Creates connections with the configured process identity for loopback development only. */
  public OfficialHBaseConnectionFactory(
      Configuration configuration, String connectionProfileId, int maxConnections) {
    this(maxConnections, developmentCreator(configuration, connectionProfileId));
  }

  /**
   * Creates a bounded manager with a provider-specific connection creator.
   *
   * <p>The creator receives an immutable identity context. It must not log that context or expose
   * its opaque reference in exceptions.
   */
  public OfficialHBaseConnectionFactory(
      int maxConnections,
      Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
          connectionCreator) {
    this(maxConnections, Duration.ofSeconds(30), connectionCreator, CompletableFuture::new);
  }

  /** Bounds shared creation; one caller's cancellation still does not cancel other borrowers. */
  public OfficialHBaseConnectionFactory(
      int maxConnections,
      Duration creationTimeout,
      Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
          connectionCreator) {
    this(maxConnections, creationTimeout, connectionCreator, CompletableFuture::new);
  }

  /** Package-private future seam lets concurrency tests pause connection publication. */
  OfficialHBaseConnectionFactory(
      int maxConnections,
      Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
          connectionCreator,
      Supplier<CompletableFuture<AsyncConnection>> connectionFutureFactory) {
    this(maxConnections, Duration.ofSeconds(30), connectionCreator, connectionFutureFactory);
  }

  private OfficialHBaseConnectionFactory(
      int maxConnections,
      Duration creationTimeout,
      Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
          connectionCreator,
      Supplier<CompletableFuture<AsyncConnection>> connectionFutureFactory) {
    if (maxConnections < 1) {
      throw new IllegalArgumentException("maxConnections must be positive");
    }
    this.maxConnections = maxConnections;
    this.creationTimeout = Objects.requireNonNull(creationTimeout, "creationTimeout");
    if (creationTimeout.isNegative()
        || creationTimeout.isZero()
        || creationTimeout.compareTo(Duration.ofMinutes(5)) > 0) {
      throw new IllegalArgumentException("creationTimeout must be positive and at most 5 minutes");
    }
    this.connectionCreator = Objects.requireNonNull(connectionCreator, "connectionCreator");
    this.connectionFutureFactory =
        Objects.requireNonNull(connectionFutureFactory, "connectionFutureFactory");
    deadlines =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().daemon().name("hbase-connection-deadline-", 0).factory());
    deadlines.setRemoveOnCancelPolicy(true);
    deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  @Override
  public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    IdentityKey key = IdentityKey.from(identity);
    Entry entry;
    boolean create;
    synchronized (lock) {
      if (closed) {
        return failed(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      entry = connections.get(key);
      if (entry != null) {
        create = false;
      } else if (connections.size() >= maxConnections) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      } else {
        entry = new Entry(connectionFutureFactory.get());
        connections.put(key, entry);
        Entry creating = entry;
        entry.deadline =
            deadlines.schedule(
                () -> expireCreation(key, creating),
                creationTimeout.toNanos(),
                TimeUnit.NANOSECONDS);
        create = true;
      }
    }

    if (create) {
      startConnection(key, entry, identity);
    }
    // Cancelling one caller's dependent future must not cancel creation shared with other callers.
    return entry.connection.thenApply(Function.identity());
  }

  @Override
  public void close() {
    List<Entry> toClose = new ArrayList<>();
    List<Entry> toFail = new ArrayList<>();
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      for (Entry entry : connections.values()) {
        if (entry.state == EntryState.READY) {
          toClose.add(entry);
          entry.state = EntryState.CLOSED;
        } else if (entry.state == EntryState.CREATING || entry.state == EntryState.TIMED_OUT) {
          entry.state = EntryState.CLOSED;
          toFail.add(entry);
        }
      }
      connections.clear();
    }
    deadlines.shutdownNow();

    for (Entry entry : toFail) {
      synchronized (entry.publicationLock) {
        entry.connection.completeExceptionally(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
    }

    int closeFailures = 0;
    for (Entry entry : toClose) {
      synchronized (entry.publicationLock) {
        if (!entry.connection.isDone()) {
          entry.connection.completeExceptionally(
              new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
        if (!closeConnection(entry.readyConnection)) {
          closeFailures++;
        }
      }
    }
    if (closeFailures > 0) {
      throw new IllegalStateException(
          "failed to close " + closeFailures + " managed HBase connection(s)");
    }
  }

  private void startConnection(IdentityKey key, Entry entry, AuthorizedBackendIdentity identity) {
    CompletionStage<AsyncConnection> creation;
    try {
      creation =
          Objects.requireNonNull(connectionCreator.apply(identity), "connection creation stage");
    } catch (RuntimeException exception) {
      completeFailure(key, entry, exception);
      return;
    } catch (Error error) {
      completeFailure(key, entry, error);
      throw error;
    }

    try {
      creation.whenComplete(
          (connection, failure) -> {
            if (failure != null) {
              completeFailure(key, entry, failure);
            } else if (connection == null) {
              completeFailure(
                  key, entry, new IllegalStateException("connection creator returned null"));
            } else {
              completeSuccess(key, entry, connection);
            }
          });
    } catch (RuntimeException exception) {
      completeFailure(key, entry, exception);
    } catch (Error error) {
      completeFailure(key, entry, error);
      throw error;
    }
  }

  private void completeSuccess(IdentityKey key, Entry entry, AsyncConnection connection) {
    synchronized (entry.publicationLock) {
      boolean accepted;
      synchronized (lock) {
        accepted = !closed && connections.get(key) == entry && entry.state == EntryState.CREATING;
        if (accepted) {
          entry.state = EntryState.READY;
          entry.readyConnection = connection;
          entry.deadline.cancel(false);
        }
      }
      if (!accepted) {
        closeConnection(connection);
        synchronized (lock) {
          if (connections.get(key) == entry && entry.state == EntryState.TIMED_OUT) {
            connections.remove(key);
            entry.state = EntryState.FAILED;
          }
        }
        entry.connection.completeExceptionally(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        return;
      }
      if (!entry.connection.complete(connection)) {
        closeConnection(connection);
      }
    }
  }

  private void completeFailure(IdentityKey key, Entry entry, Throwable failure) {
    BackendFailure safeFailure = HBaseFailures.classify(failure);
    synchronized (entry.publicationLock) {
      boolean accepted;
      synchronized (lock) {
        accepted =
            connections.get(key) == entry
                && (entry.state == EntryState.CREATING || entry.state == EntryState.TIMED_OUT);
        if (accepted) {
          connections.remove(key);
          entry.state = EntryState.FAILED;
          entry.deadline.cancel(false);
        }
      }
      if (accepted) {
        entry.connection.completeExceptionally(safeFailure);
      }
    }
  }

  private void expireCreation(IdentityKey key, Entry entry) {
    synchronized (entry.publicationLock) {
      synchronized (lock) {
        if (connections.get(key) != entry || entry.state != EntryState.CREATING) {
          return;
        }
        // Retain its capacity until provider work settles; retries cannot create unlimited orphans.
        entry.state = EntryState.TIMED_OUT;
      }
      entry.connection.completeExceptionally(
          new BackendFailure(BackendFailure.Category.TIMEOUT, new TimeoutException()));
    }
  }

  private boolean closeConnection(AsyncConnection connection) {
    try {
      connection.close();
      return true;
    } catch (IOException | RuntimeException failure) {
      LOGGER.log(System.Logger.Level.WARNING, "Failed to close a managed HBase connection");
      return false;
    }
  }

  private static Function<AuthorizedBackendIdentity, CompletionStage<AsyncConnection>>
      developmentCreator(Configuration configuration, String connectionProfileId) {
    Configuration snapshot =
        new Configuration(Objects.requireNonNull(configuration, "configuration"));
    DevelopmentConnectionConfiguration.validate(snapshot);
    String requiredProfile = requireText(connectionProfileId, "connectionProfileId");
    return identity -> {
      if (!BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID.equals(identity.providerId())) {
        return failed(new BackendFailure(BackendFailure.Category.AUTHENTICATION));
      }
      if (!requiredProfile.equals(identity.scope().connectionProfileId())) {
        return failed(new BackendFailure(BackendFailure.Category.PERMISSION));
      }
      return ConnectionFactory.createAsyncConnection(new Configuration(snapshot));
    };
  }

  private static <T> CompletionStage<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static final class Entry {
    private final CompletableFuture<AsyncConnection> connection;
    private final Object publicationLock = new Object();
    private EntryState state = EntryState.CREATING;
    private AsyncConnection readyConnection;
    private ScheduledFuture<?> deadline;

    private Entry(CompletableFuture<AsyncConnection> connection) {
      this.connection = Objects.requireNonNull(connection, "connection future");
    }
  }

  private enum EntryState {
    CREATING,
    TIMED_OUT,
    READY,
    FAILED,
    CLOSED
  }

  private record IdentityKey(
      String providerId,
      String subject,
      String contextRef,
      String tableGroupId,
      String connectionProfileId,
      long credentialGeneration) {
    private static IdentityKey from(AuthorizedBackendIdentity identity) {
      return new IdentityKey(
          identity.providerId(),
          identity.subject(),
          identity.contextRef(),
          identity.scope().tableGroupId(),
          identity.scope().connectionProfileId(),
          identity.credentialGeneration());
    }

    @Override
    public String toString() {
      return "IdentityKey[redacted]";
    }
  }
}

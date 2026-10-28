package io.etcdhbase.hbase.identity;

import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

/**
 * One configured table group's bounded authentication and connection owner.
 *
 * <p>Credential conversion, connection creation, and backend permission verification must all
 * succeed before publication. The verified connection is then reused by that identity. No etcd
 * sessions or tokens are created here. Contexts remain bounded until provider close; per-session
 * retirement and renewal belong to the session/lifecycle layer.
 */
public final class HBaseBackendIdentityProvider
    implements BackendIdentityProvider, HBaseConnectionFactory {
  @FunctionalInterface
  interface ConnectionOpener {
    CompletionStage<AsyncConnection> open(Configuration configuration, User user);
  }

  private final Object gate = new Object();
  private final String providerId;
  private final AuthenticationScope scope;
  private final Configuration configuration;
  private final HBaseCredentialAuthenticator authenticator;
  private final HBaseAccessVerifier verifier;
  private final ConnectionOpener opener;
  private final int maximumContexts;
  private final Duration timeout;
  private final Map<String, Attempt> contexts = new HashMap<>();
  private final ThreadPoolExecutor workers;
  private final ThreadPoolExecutor completions;
  private final ScheduledThreadPoolExecutor deadlines;
  private volatile boolean closed;

  public HBaseBackendIdentityProvider(
      String providerId,
      AuthenticationScope scope,
      Configuration configuration,
      HBaseCredentialAuthenticator authenticator,
      HBaseAccessVerifier verifier,
      int maximumContexts,
      int authenticationWorkers,
      Duration timeout) {
    this(
        providerId,
        scope,
        configuration,
        authenticator,
        verifier,
        maximumContexts,
        authenticationWorkers,
        timeout,
        ConnectionFactory::createAsyncConnection);
  }

  HBaseBackendIdentityProvider(
      String providerId,
      AuthenticationScope scope,
      Configuration configuration,
      HBaseCredentialAuthenticator authenticator,
      HBaseAccessVerifier verifier,
      int maximumContexts,
      int authenticationWorkers,
      Duration timeout,
      ConnectionOpener opener) {
    if (providerId == null || !providerId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("backend provider ID must be a configured identifier");
    }
    if (maximumContexts < 1
        || maximumContexts > 1024
        || authenticationWorkers < 1
        || authenticationWorkers > Math.min(maximumContexts, 32)) {
      throw new IllegalArgumentException("backend authentication capacity is invalid");
    }
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(5)) > 0) {
      throw new IllegalArgumentException("backend authentication timeout must be within 5 minutes");
    }
    this.providerId = providerId;
    this.scope = Objects.requireNonNull(scope, "scope");
    this.configuration = new Configuration(Objects.requireNonNull(configuration, "configuration"));
    this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
    authenticator.validateConfiguration(this.configuration);
    this.configuration.setBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, false);
    this.verifier = Objects.requireNonNull(verifier, "verifier");
    this.opener = Objects.requireNonNull(opener, "opener");
    this.maximumContexts = maximumContexts;
    this.timeout = timeout;
    workers = pool(authenticationWorkers, maximumContexts, "hbase-auth-");
    completions = pool(1, maximumContexts * 3, "hbase-auth-completion-");
    deadlines =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().daemon().name("hbase-auth-deadline-", 0).factory());
    deadlines.setRemoveOnCancelPolicy(true);
    deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  @Override
  public String providerId() {
    return providerId;
  }

  @Override
  public CompletionStage<AuthorizedBackendIdentity> authenticate(
      BackendCredentials credentials, AuthenticationScope requestedScope) {
    Objects.requireNonNull(credentials, "credentials");
    Objects.requireNonNull(requestedScope, "requestedScope");
    synchronized (gate) {
      if (closed) {
        return rejected(Reason.BACKEND_UNAVAILABLE);
      }
      if (!scope.equals(requestedScope)) {
        return rejected(Reason.INSUFFICIENT_PERMISSION);
      }
      if (contexts.size() >= maximumContexts) {
        return rejected(Reason.RESOURCE_LIMIT);
      }
      BackendCredentials copy;
      try {
        HBaseAuthenticators.requirePrincipal(credentials.principal());
        char[] secret = credentials.copySecret(32_768);
        try {
          copy = new BackendCredentials(credentials.principal(), secret);
        } finally {
          Arrays.fill(secret, '\0');
        }
      } catch (IllegalArgumentException | IllegalStateException failure) {
        return rejected(Reason.INVALID_CREDENTIALS);
      }
      return start(new Attempt(copy, () -> authenticator.authenticate(copy, scope)));
    }
  }

  /**
   * Trusted operator/bootstrap entrypoint for keytab, ticket-cache, or other controlled identity
   * activation. Never expose this as client Authenticate or derive its source from a
   * client-supplied username/path. The source runs on a bounded worker; the provider owns its
   * returned context. The same backend permission check and resource limits apply as for submitted
   * credentials.
   */
  public CompletionStage<AuthorizedBackendIdentity> activateTrusted(
      Callable<? extends HBaseUserContext> source) {
    Objects.requireNonNull(source, "source");
    synchronized (gate) {
      if (closed) {
        return rejected(Reason.BACKEND_UNAVAILABLE);
      }
      if (contexts.size() >= maximumContexts) {
        return rejected(Reason.RESOURCE_LIMIT);
      }
      return start(new Attempt(null, source));
    }
  }

  /** Called while holding gate, so accepted work is submitted before shutdown can start. */
  private CompletionStage<AuthorizedBackendIdentity> start(Attempt attempt) {
    contexts.put(attempt.reference, attempt);
    attempt.deadline =
        deadlines.schedule(
            () -> attempt.stop(Reason.TIMEOUT), timeout.toNanos(), TimeUnit.NANOSECONDS);
    attempt.begin();
    return attempt.result;
  }

  @Override
  public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    synchronized (gate) {
      if (closed) {
        return rejected(Reason.BACKEND_UNAVAILABLE);
      }
      Attempt attempt = contexts.get(identity.contextRef());
      if (attempt == null) {
        return rejected(Reason.INSUFFICIENT_PERMISSION);
      }
      synchronized (attempt) {
        if (attempt.identity != identity || attempt.stopped || attempt.connection == null) {
          return rejected(Reason.INSUFFICIENT_PERMISSION);
        }
        return CompletableFuture.completedFuture(attempt.connection);
      }
    }
  }

  @Override
  public void close() {
    ArrayList<Attempt> snapshot;
    synchronized (gate) {
      closed = true;
      snapshot = new ArrayList<>(contexts.values());
    }
    for (Attempt attempt : snapshot) {
      attempt.stop(Reason.CANCELLED);
    }
    workers.shutdown();
    int failures = 0;
    for (Attempt attempt : snapshot) {
      if (!attempt.cleanup()) {
        failures++;
      }
    }
    shutdownWhenEmpty();
    if (failures != 0) {
      throw new IllegalStateException(
          "backend authentication resource cleanup failed: " + failures);
    }
  }

  private void retire(Attempt attempt) {
    synchronized (gate) {
      contexts.remove(attempt.reference, attempt);
    }
    shutdownWhenEmpty();
  }

  private void shutdownWhenEmpty() {
    synchronized (gate) {
      if (closed && contexts.isEmpty()) {
        completions.shutdown();
        deadlines.shutdown();
      }
    }
  }

  private static ThreadPoolExecutor pool(int threads, int queueSize, String name) {
    var executor =
        new ThreadPoolExecutor(
            threads,
            threads,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueSize),
            Thread.ofPlatform().daemon().name(name, 0).factory(),
            new ThreadPoolExecutor.AbortPolicy());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private static <T> CompletableFuture<T> rejected(Reason reason) {
    return CompletableFuture.failedFuture(new BackendIdentityException(reason));
  }

  private final class Attempt {
    final String reference = UUID.randomUUID().toString();
    final BackendCredentials credentials;
    final Callable<? extends HBaseUserContext> userSource;
    final CompletableFuture<AuthorizedBackendIdentity> result = new CompletableFuture<>();
    volatile ScheduledFuture<?> deadline;
    HBaseUserContext user;
    AsyncConnection connection;
    AuthorizedBackendIdentity identity;
    Thread loginWorker;
    boolean stopped;
    boolean workFinished;
    boolean cleaning;

    Attempt(BackendCredentials credentials, Callable<? extends HBaseUserContext> userSource) {
      this.credentials = credentials;
      this.userSource = userSource;
      result.whenComplete(
          (ignored, failure) -> {
            if (result.isCancelled()) {
              stop(Reason.CANCELLED);
            }
          });
    }

    void begin() {
      CompletableFuture.supplyAsync(this::openUser, workers)
          .thenCompose(
              context -> {
                synchronized (this) {
                  if (stopped) {
                    return HBaseBackendIdentityProvider.<AsyncConnection>rejected(Reason.CANCELLED);
                  }
                }
                // Keep the original future: cancelling it can discard a late connection resource.
                return opener.open(new Configuration(configuration), context.user());
              })
          .thenComposeAsync(
              opened -> {
                synchronized (this) {
                  connection = Objects.requireNonNull(opened, "backend connection");
                  if (stopped) {
                    return HBaseBackendIdentityProvider.<Void>rejected(Reason.CANCELLED);
                  }
                }
                return verifier.verify(opened, scope);
              },
              completions)
          .whenCompleteAsync((ignored, failure) -> finish(failure), completions);
    }

    HBaseUserContext openUser() {
      try {
        synchronized (this) {
          if (stopped) {
            throw new CompletionException(new BackendIdentityException(Reason.CANCELLED));
          }
          loginWorker = Thread.currentThread();
        }
        HBaseUserContext opened = userSource.call();
        synchronized (this) {
          user = Objects.requireNonNull(opened, "backend user context");
        }
        return opened;
      } catch (IllegalArgumentException failure) {
        throw new CompletionException(new BackendIdentityException(Reason.INVALID_CREDENTIALS));
      } catch (Exception failure) {
        throw new CompletionException(failure);
      } finally {
        if (credentials != null) {
          credentials.close();
        }
        synchronized (this) {
          loginWorker = null;
        }
      }
    }

    void finish(Throwable failure) {
      AuthorizedBackendIdentity published = null;
      synchronized (this) {
        workFinished = true;
        if (failure != null || closed) {
          stopped = true;
        }
        if (!stopped) {
          try {
            published =
                new AuthorizedBackendIdentity(
                    providerId,
                    user.user().getName(),
                    reference,
                    scope,
                    Set.of(scope.tableGroupId()),
                    0);
            identity = published;
          } catch (RuntimeException invalidContext) {
            stopped = true;
            failure = new BackendIdentityException(Reason.INVALID_CREDENTIALS);
          }
        }
      }
      deadline.cancel(false);
      if (published != null) {
        if (!result.complete(published)) {
          stop(Reason.CANCELLED);
        }
      } else if (failure != null) {
        Reason reason =
            switch (HBaseFailures.classify(failure).category()) {
              case AUTHENTICATION -> Reason.INVALID_CREDENTIALS;
              case PERMISSION -> Reason.INSUFFICIENT_PERMISSION;
              case RESOURCE_LIMIT -> Reason.RESOURCE_LIMIT;
              case TIMEOUT -> Reason.TIMEOUT;
              case CANCELLED -> Reason.CANCELLED;
              default -> Reason.BACKEND_UNAVAILABLE;
            };
        result.completeExceptionally(new BackendIdentityException(reason));
      } else {
        result.completeExceptionally(new BackendIdentityException(Reason.CANCELLED));
      }
      cleanup();
    }

    void stop(Reason reason) {
      synchronized (this) {
        if (reason == Reason.TIMEOUT && identity != null) {
          return;
        }
        stopped = true;
        if (loginWorker != null) {
          loginWorker.interrupt();
        }
      }
      result.completeExceptionally(new BackendIdentityException(reason));
    }

    /** Retains capacity and credentials if a connection could not be closed; close() retries it. */
    boolean cleanup() {
      synchronized (this) {
        if (!stopped || !workFinished || cleaning) {
          return true;
        }
        cleaning = true;
      }
      boolean success = false;
      try {
        if (connection != null) {
          connection.close();
          connection = null;
        }
        if (user != null) {
          user.close();
          user = null;
        }
        success = true;
      } catch (IOException | RuntimeException failure) {
        // Keep the owned resources available for a later close retry, without publishing details.
      } finally {
        synchronized (this) {
          cleaning = false;
        }
      }
      if (success) {
        retire(this);
      }
      return success;
    }
  }
}

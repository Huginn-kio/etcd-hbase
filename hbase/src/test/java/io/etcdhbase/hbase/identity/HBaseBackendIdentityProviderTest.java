package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.sasl.SaslException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

class HBaseBackendIdentityProviderTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "backend");
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  @Test
  void verifiesBackendBeforePublicationAndReusesOnlyTheIssuedIdentity() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var verification = new CompletableFuture<Void>();
    var verified = new CountDownLatch(1);
    var copiedCredentials = new AtomicReference<BackendCredentials>();
    HBaseCredentialAuthenticator authenticator =
        (credentials, scope) -> {
          copiedCredentials.set(credentials);
          assertArrayEquals("password".toCharArray(), credentials.copySecret());
          return context;
        };
    try (var provider =
        provider(
            authenticator,
            (opened, scope) -> {
              assertSame(connection, opened);
              verified.countDown();
              return verification;
            },
            (configuration, user) -> {
              assertFalse(
                  configuration.getBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, true));
              assertEquals("alice", user.getName());
              return CompletableFuture.completedFuture(connection);
            },
            TIMEOUT)) {
      CompletableFuture<AuthorizedBackendIdentity> result;
      try (var credentials = new BackendCredentials("alice", "password".toCharArray())) {
        result = provider.authenticate(credentials, SCOPE).toCompletableFuture();
      }
      assertTrue(verified.await(5, TimeUnit.SECONDS));
      assertFalse(result.isDone(), "creating a User/connection must not publish an identity");
      assertTrue(copiedCredentials.get().isClosed());
      verification.complete(null);
      AuthorizedBackendIdentity identity = result.get(5, TimeUnit.SECONDS);
      assertSame(connection, provider.connectionFor(identity).toCompletableFuture().get());
      assertSame(connection, provider.connectionFor(identity).toCompletableFuture().get());
      var forged =
          new AuthorizedBackendIdentity(
              identity.providerId(),
              identity.subject(),
              identity.contextRef(),
              identity.scope(),
              Set.of("group"),
              identity.credentialGeneration());
      assertReason(
          Reason.INSUFFICIENT_PERMISSION, provider.connectionFor(forged).toCompletableFuture());
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
    }
    verify(connection).close();
    verify(context).close();
  }

  @Test
  void rejectsWrongScopeWithoutOpeningCredentialsOrConnections() throws Exception {
    var authenticator = mock(HBaseCredentialAuthenticator.class);
    try (var provider = provider(authenticator, (c, s) -> null, (c, u) -> null, TIMEOUT);
        var credentials = new BackendCredentials("alice", new char[0])) {
      assertReason(
          Reason.INSUFFICIENT_PERMISSION,
          provider
              .authenticate(credentials, new AuthenticationScope("other", "backend"))
              .toCompletableFuture());
      verify(authenticator, never()).authenticate(any(), any());
    }
  }

  @Test
  void authenticationFailureDoesNotFallBackToAnotherMechanism() throws Exception {
    var opens = new java.util.concurrent.atomic.AtomicInteger();
    try (var provider =
        provider(
            (credentials, scope) -> {
              throw new SaslException("fixture rejected password");
            },
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              opens.incrementAndGet();
              return null;
            },
            TIMEOUT)) {
      assertReason(Reason.INVALID_CREDENTIALS, authenticate(provider));
      assertEquals(0, opens.get());
    }
  }

  @Test
  void permissionDenialClosesConnectionBeforeCredentials() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var closed = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    try (var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) ->
                CompletableFuture.failedFuture(
                    new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION)),
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT)) {
      assertReason(Reason.INSUFFICIENT_PERMISSION, authenticate(provider));
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      var order = inOrder(connection, context);
      order.verify(connection).close();
      order.verify(context).close();
    }
  }

  @Test
  void timeoutKeepsCapacityUntilLateConnectionAndCredentialsHaveBeenClosed() throws Exception {
    var context = context("alice");
    var rawConnection = new CompletableFuture<AsyncConnection>();
    var connectionOpened = new CountDownLatch(1);
    var closed = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    var connection = mock(AsyncConnection.class);
    try (var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              connectionOpened.countDown();
              return rawConnection;
            },
            Duration.ofSeconds(1))) {
      var result = authenticate(provider);
      assertTrue(connectionOpened.await(5, TimeUnit.SECONDS));
      assertReason(Reason.TIMEOUT, result);
      assertFalse(rawConnection.isCancelled());
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      rawConnection.complete(connection);
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      verify(connection).close();
    }
  }

  @Test
  void cancellationAndShutdownCleanLateConnectionsWithoutCancellingTheResourceFuture()
      throws Exception {
    var context = context("alice");
    var rawConnection = new CompletableFuture<AsyncConnection>();
    var opened = new CountDownLatch(1);
    var closed = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    var connection = mock(AsyncConnection.class);
    var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              opened.countDown();
              return rawConnection;
            },
            TIMEOUT);
    try {
      var result = authenticate(provider);
      assertTrue(opened.await(5, TimeUnit.SECONDS));
      assertTrue(result.cancel(true));
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      provider.close();
      assertFalse(rawConnection.isCancelled());
      rawConnection.complete(connection);
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      assertReason(Reason.BACKEND_UNAVAILABLE, authenticate(provider));
      verify(connection).close();
    } finally {
      provider.close();
    }
  }

  @Test
  void closeFailureRetainsCredentialsAndIsRetriedAtShutdown() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    doThrow(new IOException("fixture close failure")).doNothing().when(connection).close();
    var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT);
    authenticate(provider).get(5, TimeUnit.SECONDS);
    assertThrows(IllegalStateException.class, provider::close);
    verify(context, never()).close();
    provider.close();
    provider.close();
    verify(connection, times(2)).close();
    verify(context).close();
  }

  @Test
  void trustedActivationUsesTheSameBackendVerificationWithoutPretendingToVerifyClientCredentials()
      throws Exception {
    var authenticator = mock(HBaseCredentialAuthenticator.class);
    var context = context("operator");
    var connection = mock(AsyncConnection.class);
    var verified = new java.util.concurrent.atomic.AtomicBoolean();
    try (var provider =
        provider(
            authenticator,
            (c, s) -> {
              verified.set(true);
              return CompletableFuture.completedFuture(null);
            },
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT)) {
      var identity =
          provider.activateTrusted(() -> context).toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertTrue(verified.get());
      assertEquals("operator", identity.subject());
      assertSame(connection, provider.connectionFor(identity).toCompletableFuture().get());
      verify(authenticator, never()).authenticate(any(), any());
    }
    verify(connection).close();
    verify(context).close();
  }

  @Test
  void cancellationDuringBlockingLoginRetainsCapacityAndClosesTheLateUser() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var interrupted = new CountDownLatch(1);
    var closed = new CountDownLatch(1);
    var context = context("alice");
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    var opens = new java.util.concurrent.atomic.AtomicInteger();
    var provider =
        provider(
            (credentials, scope) -> {
              entered.countDown();
              boolean done = false;
              while (!done) {
                try {
                  done = release.await(5, TimeUnit.SECONDS);
                  if (!done) throw new IOException("fixture login was not released");
                } catch (InterruptedException expected) {
                  interrupted.countDown();
                }
              }
              return context;
            },
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              opens.incrementAndGet();
              return null;
            },
            TIMEOUT);
    try {
      var result = authenticate(provider);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertTrue(result.cancel(true));
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      provider.close();
      release.countDown();
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      assertEquals(0, opens.get());
    } finally {
      release.countDown();
      provider.close();
    }
  }

  @Test
  void shutdownDuringPermissionRpcWaitsForCompletionBeforeDestroyingUserCredentials()
      throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var verification = new CompletableFuture<Void>();
    var entered = new CountDownLatch(1);
    var cleaned = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              cleaned.countDown();
              return null;
            })
        .when(context)
        .close();
    var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> {
              entered.countDown();
              return verification;
            },
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT);
    try {
      var result = authenticate(provider);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      provider.close();
      assertReason(Reason.CANCELLED, result);
      verify(connection, never()).close();
      verify(context, never()).close();
      assertFalse(verification.isCancelled());
      verification.complete(null);
      assertTrue(cleaned.await(5, TimeUnit.SECONDS));
      verify(connection).close();
    } finally {
      verification.complete(null);
      provider.close();
    }
  }

  @Test
  void malformedCustomUserContextCannotLeaveAuthenticationUncompleted() throws Exception {
    var context = context("a".repeat(257));
    var connection = mock(AsyncConnection.class);
    var cleaned = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              cleaned.countDown();
              return null;
            })
        .when(context)
        .close();
    try (var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT)) {
      assertReason(Reason.INVALID_CREDENTIALS, authenticate(provider));
      assertTrue(cleaned.await(5, TimeUnit.SECONDS));
      verify(connection).close();
    }
  }

  @Test
  void secureAdaptersRejectSimpleConfigurationAndExplicitFallback() {
    for (var authenticator :
        new HBaseCredentialAuthenticator[] {
          HBaseAuthenticators.kerberosPassword(),
          HBaseAuthenticators.delegationToken("cluster"),
          HBaseAuthenticators.digestPassword("cluster")
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> authenticator.validateConfiguration(new Configuration(false)));
      var configuration = secureConfiguration();
      configuration.setBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, true);
      assertThrows(
          IllegalArgumentException.class, () -> authenticator.validateConfiguration(configuration));
    }
  }

  private static HBaseBackendIdentityProvider provider(
      HBaseCredentialAuthenticator authenticator,
      HBaseAccessVerifier verifier,
      HBaseBackendIdentityProvider.ConnectionOpener opener,
      Duration timeout) {
    return new HBaseBackendIdentityProvider(
        "backend-auth",
        SCOPE,
        secureConfiguration(),
        authenticator,
        verifier,
        1,
        1,
        timeout,
        opener);
  }

  private static Configuration secureConfiguration() {
    var configuration = new Configuration(false);
    configuration.set("hbase.security.authentication", "kerberos");
    return configuration;
  }

  private static HBaseUserContext context(String name) {
    var context = mock(HBaseUserContext.class);
    when(context.user()).thenReturn(User.create(UserGroupInformation.createRemoteUser(name)));
    return context;
  }

  private static CompletableFuture<AuthorizedBackendIdentity> authenticate(
      HBaseBackendIdentityProvider provider) {
    try (var credentials = new BackendCredentials("alice", "password".toCharArray())) {
      return provider.authenticate(credentials, SCOPE).toCompletableFuture();
    }
  }

  private static void assertReason(Reason reason, CompletableFuture<?> future) {
    var failure = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    var identityFailure = assertInstanceOf(BackendIdentityException.class, failure.getCause());
    assertEquals(reason, identityFailure.reason());
  }
}

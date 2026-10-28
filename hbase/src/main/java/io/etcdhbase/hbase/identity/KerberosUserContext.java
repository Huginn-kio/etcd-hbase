package io.etcdhbase.hbase.identity;

import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Isolated JAAS login; never installs a process-wide login user or changes global configuration.
 */
public final class KerberosUserContext implements HBaseUserContext {
  private final LoginContext login;
  private final Subject subject;
  private final User user;
  private boolean closed;

  private KerberosUserContext(LoginContext login, Subject subject, User user) {
    this.login = login;
    this.subject = subject;
    this.user = user;
  }

  public static KerberosUserContext fromPassword(BackendCredentials credentials)
      throws IOException {
    String principal = credentials.principal();
    requireKerberosPrincipal(principal);
    char[] secret = credentials.copySecret(4096);
    var callbacks = new ArrayList<PasswordCallback>();
    try {
      if (secret.length == 0) {
        throw new IllegalArgumentException("Kerberos password must not be empty");
      }
      CallbackHandler handler =
          values -> {
            for (var callback : values) {
              if (callback instanceof NameCallback name) {
                name.setName(principal);
              } else if (callback instanceof PasswordCallback password) {
                callbacks.add(password);
                password.setPassword(secret);
              } else {
                throw new UnsupportedCallbackException(callback);
              }
            }
          };
      return login(principal, Map.of("doNotPrompt", "false"), handler);
    } finally {
      callbacks.forEach(PasswordCallback::clearPassword);
      Arrays.fill(secret, '\0');
    }
  }

  /**
   * Trusted operator/bootstrap activation only. A client-supplied username or filename is not proof
   * of access to this server-held keytab; do not use this as an Authenticate credential adapter.
   */
  public static KerberosUserContext fromKeytab(String principal, Path keytab) throws IOException {
    return login(
        principal,
        Map.of("useKeyTab", "true", "keyTab", credentialFile(keytab), "doNotPrompt", "true"),
        null);
  }

  /**
   * Trusted operator/bootstrap activation only, with an explicit cache and no fallback to keytab.
   */
  public static KerberosUserContext fromTicketCache(String principal, Path ticketCache)
      throws IOException {
    return login(
        principal,
        Map.of(
            "useTicketCache", "true",
            "ticketCache", credentialFile(ticketCache),
            "doNotPrompt", "true"),
        null);
  }

  private static KerberosUserContext login(
      String principal, Map<String, String> sourceOptions, CallbackHandler handler)
      throws IOException {
    requireKerberosPrincipal(principal);
    var options = new HashMap<String, String>();
    options.put("principal", principal);
    options.put("isInitiator", "true");
    options.put("useTicketCache", "false");
    options.put("useKeyTab", "false");
    options.put("storeKey", "false");
    options.putAll(sourceOptions);
    var subject = new Subject();
    var configuration =
        new javax.security.auth.login.Configuration() {
          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            return new AppConfigurationEntry[] {
              new AppConfigurationEntry(
                  "com.sun.security.auth.module.Krb5LoginModule",
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  Map.copyOf(options))
            };
          }
        };
    LoginContext login = null;
    try {
      login = new LoginContext("etcd-hbase", subject, handler, configuration);
      login.login();
      User user = User.create(UserGroupInformation.getUGIFromSubject(subject));
      // The JDK may apply a JVM principal override. Never let that change the authenticated user.
      if (!principal.equals(user.getName()) || !user.getUGI().hasKerberosCredentials()) {
        throw new LoginException("backend Kerberos identity did not match");
      }
      return new KerberosUserContext(login, subject, user);
    } catch (LoginException | IOException failure) {
      if (login != null) {
        try {
          login.logout();
        } catch (LoginException ignored) {
          // A failed login need not have committed; destroy any remaining owned credentials below.
        }
      }
      destroyCredentials(subject);
      throw new IOException("backend Kerberos login failed", failure);
    }
  }

  @Override
  public synchronized User user() {
    if (closed) {
      throw new IllegalStateException("backend user context is closed");
    }
    return user;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    boolean failed = false;
    try {
      login.logout();
    } catch (LoginException failure) {
      failed = true;
    }
    failed |= !destroyCredentials(subject);
    if (failed) {
      throw new IOException("backend Kerberos context cleanup failed");
    }
    closed = true;
  }

  @Override
  public String toString() {
    return "KerberosUserContext[redacted]";
  }

  private static boolean destroyCredentials(Subject subject) {
    boolean success = true;
    for (Object credential : new ArrayList<>(subject.getPrivateCredentials())) {
      if (credential instanceof Destroyable destroyable) {
        try {
          if (!destroyable.isDestroyed()) {
            destroyable.destroy();
          }
        } catch (DestroyFailedException failure) {
          success = false;
        }
      }
    }
    if (success) {
      subject.getPrivateCredentials().clear();
    }
    return success;
  }

  private static String credentialFile(Path path) throws IOException {
    if (path == null
        || !path.isAbsolute()
        || !Files.isRegularFile(path)
        || !Files.isReadable(path)
        || Files.size(path) == 0
        || Files.size(path) > 1_048_576) {
      throw new IOException("backend credential file is unavailable or exceeds its limit");
    }
    return path.toString();
  }

  private static void requireKerberosPrincipal(String principal) {
    HBaseAuthenticators.requirePrincipal(principal);
    if (principal.indexOf('@') <= 0 || principal.endsWith("@")) {
      throw new IllegalArgumentException("Kerberos principal requires an explicit realm");
    }
  }
}

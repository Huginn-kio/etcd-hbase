package io.etcdhbase.server.commit;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLPeerUnverifiedException;

/** Extracts exactly one configured DNS SAN from the mutually authenticated TLS peer. */
final class OwnerPeerIdentityInterceptor implements ServerInterceptor {
  private static final Context.Key<String> AUTHENTICATED_PEER = Context.key("owner-peer-identity");

  private final Map<String, String> canonicalIdentities;

  OwnerPeerIdentityInterceptor(Collection<String> identities) {
    Objects.requireNonNull(identities, "identities");
    var indexed = new java.util.HashMap<String, String>();
    for (String identity : identities) {
      Objects.requireNonNull(identity, "identity");
      if (indexed.putIfAbsent(identity.toLowerCase(Locale.ROOT), identity) != null) {
        throw new IllegalArgumentException("owner peer identities must be unique");
      }
    }
    canonicalIdentities = Map.copyOf(indexed);
  }

  static String currentPeerIdentity() {
    return AUTHENTICATED_PEER.get();
  }

  @Override
  public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
      ServerCall<RequestT, ResponseT> call, Metadata headers,
      ServerCallHandler<RequestT, ResponseT> next) {
    String identity = authenticatedIdentity(call);
    if (identity == null) {
      call.close(Status.PERMISSION_DENIED.withDescription("internal peer identity is not trusted"),
          new Metadata());
      return new ServerCall.Listener<>() {};
    }
    Context context = Context.current().withValue(AUTHENTICATED_PEER, identity);
    return Contexts.interceptCall(context, call, headers, next);
  }

  private String authenticatedIdentity(ServerCall<?, ?> call) {
    var session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
    if (session == null) return null;
    try {
      Certificate[] certificates = session.getPeerCertificates();
      if (certificates.length == 0 || !(certificates[0] instanceof X509Certificate certificate)) return null;
      return identityFromCertificate(certificate);
    } catch (SSLPeerUnverifiedException | java.security.cert.CertificateParsingException failure) {
      return null;
    }
  }

  String identityFromCertificate(X509Certificate certificate)
      throws java.security.cert.CertificateParsingException {
    Objects.requireNonNull(certificate, "certificate");
    Collection<List<?>> names = certificate.getSubjectAlternativeNames();
    if (names == null) return null;
    String dnsName = null;
    for (List<?> entry : names) {
      if (entry.size() < 2 || !Integer.valueOf(2).equals(entry.getFirst())) continue;
      if (!(entry.get(1) instanceof String value) || dnsName != null) return null;
      dnsName = value;
    }
    return dnsName == null ? null : canonicalIdentities.get(dnsName.toLowerCase(Locale.ROOT));
  }
}

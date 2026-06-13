use super::auth::{authorize_request_scope, extract_bearer_token};
#[cfg(test)]
use super::ensure_rustls_crypto_provider;
#[cfg(test)]
use super::ServerConfig;
use super::{ServerContext, HANDSHAKE_TIMEOUT, KEEP_ALIVE_TIMEOUT};
use crate::cluster;
use crate::handlers;
use crate::http::{read_http_request, text_response, write_http_response};
use crate::rbac;
use crate::tenant;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use x509_parser::extensions::GeneralName;

#[cfg(test)]
pub(super) fn build_tls_acceptor(
    config: &ServerConfig,
) -> Result<Option<tokio_rustls::TlsAcceptor>, String> {
    ensure_rustls_crypto_provider();
    let internal_mtls = config.cluster.resolved_internal_mtls()?;
    let Some((cert_path, key_path)) = config.resolved_listener_tls_paths()? else {
        return Ok(None);
    };

    let cert_file = std::fs::File::open(&cert_path)
        .map_err(|e| format!("failed to open TLS cert file {}: {e}", cert_path.display()))?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<_, _>>()
        .map_err(|e| format!("failed to parse TLS certs: {e}"))?;

    if certs.is_empty() {
        return Err("TLS cert file contains no certificates".to_string());
    }

    let key_file = std::fs::File::open(&key_path)
        .map_err(|e| format!("failed to open TLS key file {}: {e}", key_path.display()))?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| format!("failed to parse TLS key: {e}"))?
        .ok_or_else(|| "TLS key file contains no private key".to_string())?;

    let tls_config = if let Some(mtls) = &internal_mtls {
        let ca_file = std::fs::File::open(&mtls.ca_cert).map_err(|e| {
            format!(
                "failed to open internal mTLS CA cert file {}: {e}",
                mtls.ca_cert.display()
            )
        })?;
        let mut ca_reader = std::io::BufReader::new(ca_file);
        let ca_certs: Vec<_> = rustls_pemfile::certs(&mut ca_reader)
            .collect::<Result<_, _>>()
            .map_err(|e| format!("failed to parse internal mTLS CA certs: {e}"))?;
        if ca_certs.is_empty() {
            return Err("internal mTLS CA cert file contains no certificates".to_string());
        }

        let mut roots = rustls::RootCertStore::empty();
        for cert in ca_certs {
            roots
                .add(cert)
                .map_err(|e| format!("failed to add internal mTLS CA cert to root store: {e}"))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()
            .map_err(|e| format!("failed to build internal mTLS client verifier: {e}"))?;
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| format!("failed to build TLS config: {e}"))?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("failed to build TLS config: {e}"))?
    };

    Ok(Some(tokio_rustls::TlsAcceptor::from(Arc::new(tls_config))))
}

pub(super) async fn handle_connection(
    stream: tokio::net::TcpStream,
    app_context: &ServerContext,
    tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    let sock_ref = socket2::SockRef::from(&stream);
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
    let _ = sock_ref.set_tcp_keepalive(&keepalive);

    let tls_acceptor = app_context
        .security_manager
        .listener_acceptor()
        .or_else(|| tls_acceptor.cloned());

    if let Some(acceptor) = tls_acceptor.as_ref() {
        let tls_stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream))
            .await
            .map_err(|_| "TLS handshake timed out".to_string())?
            .map_err(|e| format!("TLS handshake failed: {e}"))?;
        let tls_peer_node_id = extract_peer_node_id_from_tls_stream(&tls_stream);
        let (mut reader, mut writer) = tokio::io::split(tls_stream);
        handle_http_loop(
            &mut reader,
            &mut writer,
            app_context,
            tls_peer_node_id.as_deref(),
            shutdown_rx,
        )
        .await
    } else {
        let (mut reader, mut writer) = tokio::io::split(stream);
        handle_http_loop(&mut reader, &mut writer, app_context, None, shutdown_rx).await
    }
}

pub(super) async fn handle_http_loop<R, W>(
    reader: &mut R,
    writer: &mut W,
    app_context: &ServerContext,
    tls_peer_node_id: Option<&str>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut read_buffer = Vec::with_capacity(8 * 1024);
    loop {
        let mut request = tokio::select! {
            result = tokio::time::timeout(KEEP_ALIVE_TIMEOUT, read_http_request(reader, &mut read_buffer)) => {
                match result {
                    Ok(Ok(req)) => req,
                    Ok(Err(err)) => {
                        if err.contains("connection closed") {
                            return Ok(());
                        }
                        let response = text_response(400, &err);
                        let _ = write_http_response(writer, &response).await;
                        return Ok(());
                    }
                    Err(_) => {
                        return Ok(());
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                return Ok(());
            }
        };

        clear_verified_request_headers(&mut request);
        attach_tls_peer_identity(&mut request, tls_peer_node_id);
        annotate_public_auth_state(&mut request, app_context);

        if let Err(response) = authorize_request_scope(
            &mut request,
            Some(app_context.security_manager.as_ref()),
            app_context.auth_token.as_deref(),
            app_context.admin_auth_token.as_deref(),
            app_context.admin_api_enabled,
            app_context.tenant_registry.as_deref(),
            app_context.rbac_registry.as_deref(),
        ) {
            write_http_response(writer, &response).await?;
            return Ok(());
        }
        let close = request
            .header("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));

        let response = handlers::handle_request_with_context(
            app_context.handler_app_context(),
            app_context.handler_request_context(request),
        )
        .await;

        let response = if close {
            response.with_header("Connection", "close")
        } else {
            response.with_header("Connection", "keep-alive")
        };

        write_http_response(writer, &response).await?;

        if close {
            return Ok(());
        }
    }
}

fn clear_verified_request_headers(request: &mut crate::http::HttpRequest) {
    // Verified headers are transport-owned trust state, never client input.
    request
        .headers
        .remove(cluster::rpc::INTERNAL_RPC_VERIFIED_NODE_ID_HEADER);
    request.headers.remove(tenant::PUBLIC_AUTH_REQUIRED_HEADER);
    request.headers.remove(tenant::PUBLIC_AUTH_VERIFIED_HEADER);
    request.headers.remove(rbac::RBAC_AUTH_VERIFIED_HEADER);
    request.headers.remove(rbac::RBAC_AUTH_PRINCIPAL_ID_HEADER);
    request.headers.remove(rbac::RBAC_AUTH_ROLE_HEADER);
    request.headers.remove(rbac::RBAC_AUTH_METHOD_HEADER);
    request.headers.remove(rbac::RBAC_AUTH_PROVIDER_HEADER);
    request.headers.remove(rbac::RBAC_AUTH_SUBJECT_HEADER);
}

fn attach_tls_peer_identity(
    request: &mut crate::http::HttpRequest,
    tls_peer_node_id: Option<&str>,
) {
    if let Some(node_id) = tls_peer_node_id {
        request.headers.insert(
            cluster::rpc::INTERNAL_RPC_VERIFIED_NODE_ID_HEADER.to_string(),
            node_id.to_string(),
        );
    }
}

fn annotate_public_auth_state(request: &mut crate::http::HttpRequest, app_context: &ServerContext) {
    if app_context.tenant_registry.is_none() || tenant::public_request_access(request).is_none() {
        return;
    }

    let public_auth_required =
        app_context.security_manager.public_auth_configured() || app_context.auth_token.is_some();
    if public_auth_required {
        request.headers.insert(
            tenant::PUBLIC_AUTH_REQUIRED_HEADER.to_string(),
            "true".to_string(),
        );
    }

    let public_auth_verified = app_context
        .security_manager
        .public_auth_matches(extract_bearer_token(request))
        || app_context
            .auth_token
            .as_deref()
            .is_some_and(|token| extract_bearer_token(request) == Some(token));
    if public_auth_verified {
        request.headers.insert(
            tenant::PUBLIC_AUTH_VERIFIED_HEADER.to_string(),
            "true".to_string(),
        );
    }
}

fn extract_peer_node_id_from_tls_stream(
    tls_stream: &tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Option<String> {
    let (_, connection) = tls_stream.get_ref();
    let certs = connection.peer_certificates()?;
    let cert = certs.first()?;
    extract_node_id_from_certificate(cert.as_ref())
}

fn extract_node_id_from_certificate(der: &[u8]) -> Option<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).ok()?;

    for cn in cert.subject().iter_common_name() {
        if let Ok(value) = cn.as_str() {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }

    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let GeneralName::DNSName(value) = name {
                let value = value.trim();
                if let Some((first_label, _)) = value.split_once('.') {
                    if !first_label.is_empty() {
                        return Some(first_label.to_string());
                    }
                }
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }

    None
}

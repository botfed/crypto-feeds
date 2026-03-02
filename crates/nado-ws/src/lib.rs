use anyhow::{Result, Context};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;

// Re-export types that nado.rs needs.
pub use tokio_tungstenite::tungstenite::Message;
pub use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
pub use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect to a WebSocket endpoint with permessage-deflate enabled.
pub async fn connect(url: &str) -> Result<WsStream> {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http};

    let mut request = url.into_client_request().context("build ws request")?;
    request.headers_mut().insert(
        http::header::HeaderName::from_static("origin"),
        http::header::HeaderValue::from_static("https://app.nado.xyz"),
    );
    request.headers_mut().insert(
        http::header::HeaderName::from_static("user-agent"),
        http::header::HeaderValue::from_static("Mozilla/5.0"),
    );

    let mut ws_config = WebSocketConfig::default();
    ws_config.extensions.permessage_deflate = Some(DeflateConfig::default());

    let (stream, _) = tokio_tungstenite::connect_async_with_config(
        request,
        Some(ws_config),
        false,
    )
    .await
    .context("websocket connect with deflate")?;

    Ok(stream)
}

use std::io;

use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
};

const MAX_HEADER: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdioMode {
    Lsp,
    JsonLine,
}

pub struct StdioMessage {
    pub body: Vec<u8>,
    pub mode: StdioMode,
}

pub async fn read_stdio<R>(reader: &mut R) -> io::Result<Option<StdioMessage>>
where
    R: AsyncBufRead + Unpin,
{
    let mut first = [0_u8; 1];
    let n = reader.read(&mut first).await?;
    if n == 0 {
        return Ok(None);
    }

    if first[0] == b'{' {
        let mut body = vec![first[0]];
        reader.read_until(b'\n', &mut body).await?;
        while body.last().is_some_and(|b| *b == b'\n' || *b == b'\r') {
            body.pop();
        }
        return Ok(Some(StdioMessage {
            body,
            mode: StdioMode::JsonLine,
        }));
    }

    read_lsp_after_first(reader, first[0]).await.map(|body| {
        body.map(|body| StdioMessage {
            body,
            mode: StdioMode::Lsp,
        })
    })
}

pub async fn read_lsp<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut first = [0_u8; 1];
    let n = reader.read(&mut first).await?;
    if n == 0 {
        return Ok(None);
    }
    read_lsp_after_first(reader, first[0]).await
}

async fn read_lsp_after_first<R>(reader: &mut R, first: u8) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut header = Vec::new();
    header.push(first);
    let mut byte = [0_u8; 1];

    loop {
        let n = reader.read(&mut byte).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF while reading LSP headers",
            ));
        }

        header.push(byte[0]);

        if header.len() > MAX_HEADER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "LSP header block exceeds limit",
            ));
        }

        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }

    let header_text = std::str::from_utf8(&header).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("LSP header is not UTF-8: {err}"),
        )
    })?;

    let mut content_length = None;
    for line in header_text.split("\r\n") {
        if line.is_empty() {
            continue;
        }

        let Some((name, value)) = line.split_once(':') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed LSP header line",
            ));
        };

        if name.trim().eq_ignore_ascii_case("Content-Length") {
            if content_length.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate Content-Length header",
                ));
            }

            let len = value.trim().parse::<usize>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Content-Length: {err}"),
                )
            })?;

            if len > crate::framing::MAX_FRAME - 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "LSP body exceeds maximum frame size",
                ));
            }

            content_length = Some(len);
        }
    }

    let len = content_length.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "missing Content-Length header")
    })?;

    let mut body = vec![0_u8; len];
    reader.read_exact(&mut body).await?;
    Ok(Some(body))
}

pub async fn write_stdio<W>(writer: &mut W, body: &[u8], mode: StdioMode) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match mode {
        StdioMode::Lsp => write_lsp(writer, body).await,
        StdioMode::JsonLine => {
            writer.write_all(body).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await
        }
    }
}

pub async fn write_lsp<W>(writer: &mut W, body: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await
}

pub fn peek_id_method(body: &[u8]) -> (Option<serde_json::Value>, Option<String>) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return (None, None);
    };

    let id = value.get("id").cloned();
    let method = value
        .get("method")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);

    (id, method)
}

pub fn peek_tool_call(body: &[u8]) -> Option<(String, serde_json::Value)> {
    let value = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    if value.get("method").and_then(serde_json::Value::as_str) != Some("tools/call") {
        return None;
    }

    let params = value.get("params")?;
    let name = params
        .get("name")
        .and_then(serde_json::Value::as_str)?
        .to_owned();
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    Some((name, arguments))
}

#[derive(Debug, Clone, Copy)]
pub struct StdinGate {
    open: bool,
}

impl StdinGate {
    pub fn new() -> Self {
        Self { open: false }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn open(&mut self) {
        self.open = true;
    }
}

impl Default for StdinGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::peek_tool_call;
    use serde_json::json;

    #[test]
    fn peek_tool_call_returns_name_and_arguments() {
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "a2a-inbox-messages",
                "arguments": { "action": "wait", "timeout_ms": 150000 }
            }
        }))
        .unwrap();

        let (name, arguments) = peek_tool_call(&body).expect("tools/call");
        assert_eq!(name, "a2a-inbox-messages");
        assert_eq!(arguments["action"], json!("wait"));
        assert_eq!(arguments["timeout_ms"], json!(150000));
    }

    #[test]
    fn peek_tool_call_ignores_other_shapes() {
        let initialize = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let notification = br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let malformed = br#"{"jsonrpc":"2.0","id":1"#;

        assert!(peek_tool_call(initialize).is_none());
        assert!(peek_tool_call(notification).is_none());
        assert!(peek_tool_call(malformed).is_none());
    }
}

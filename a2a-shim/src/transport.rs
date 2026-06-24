//! Unix-domain socket transport helpers and ping probe.

use std::error::Error;
use std::io::{self, ErrorKind};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::ctrl::Ctrl;
use crate::framing::{read_frame, write_frame, FrameType};

const PING_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn connect(sock: &str) -> io::Result<UnixStream> {
    UnixStream::connect(sock).await
}

pub async fn ping(sock: &str) -> Result<(), Box<dyn Error>> {
    let op = async {
        let mut stream = connect(sock).await?;

        let payload = Ctrl::Ping {
            ts: Some(unix_ms_or_0()),
        }
        .to_json()?;
        write_frame(&mut stream, FrameType::Ctrl, &payload).await?;

        let frame = read_frame(&mut stream).await?;
        if frame.frame_type != FrameType::Ctrl {
            return Err(invalid_data("non_pong").into());
        }

        match Ctrl::from_json(&frame.payload)? {
            Ctrl::Pong { .. } => {}
            _ => return Err(invalid_data("non_pong").into()),
        }

        Ok::<(), Box<dyn Error>>(())
    };

    timeout(PING_TIMEOUT, op)
        .await
        .map_err(|err| -> Box<dyn Error> { Box::new(err) })?
}

fn unix_ms_or_0() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => match i64::try_from(duration.as_millis()) {
            Ok(ms) => ms,
            Err(_) => 0,
        },
        Err(_) => 0,
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

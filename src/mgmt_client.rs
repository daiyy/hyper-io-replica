use std::collections::HashMap;
use std::io::Result;
use smol::io::AsyncWriteExt;
use smol::net::unix::UnixStream;
use smol::io::{BufReader, AsyncBufReadExt};
use crate::mgmt_proto::*;

pub(crate) struct MgmtClient {
    stream: UnixStream,
}

impl MgmtClient {
    pub(crate) fn new(sock_path: &str) -> Result<Self> {
        let stream = smol::block_on(async {
            UnixStream::connect(sock_path).await
        })?;
        Ok(Self { stream })
    }

    pub fn set_mode(&mut self, mode: RunMode) {
        let mut params = HashMap::new();
        params.insert("mode".to_string(), serde_json::json!(mode as usize));
        let cmd = Command { op: CommandOp::Mode, dir: CommandDir::Set, params: params };
        let _ = self.exec_cmd(cmd);
    }

    pub(crate) fn exec_cmd(&mut self, cmd: Command) -> Vec<u8> {
        let b = smol::block_on(async {
            let buf = serde_json::ser::to_vec(&cmd).expect("unable to serialize cmd");
            let _ = self.stream.write_all(&buf).await;
            let _ = self.stream.write(b"\0").await;
            let mut buf = Vec::new();
            let mut reader = BufReader::new(self.stream.clone());
            let _ = reader.read_until(b'\0', &mut buf).await;
            buf.pop(); // remove tailing \0
            buf
        });
        b
    }

    #[allow(dead_code)]
    pub(crate) fn logging_disable(&mut self) {
        self.set_mode(RunMode::LoggingDisable);
    }

    pub(crate) fn forward_full(&mut self) {
        self.set_mode(RunMode::ForwardFull);
    }

    #[allow(dead_code)]
    pub(crate) fn forward_part(&mut self) {
        self.set_mode(RunMode::ForwardPart);
    }

    pub(crate) fn reverse_full(&mut self) {
        self.set_mode(RunMode::ReverseFull);
    }

    pub(crate) fn grace_shutdown(&mut self) {
        self.set_mode(RunMode::GraceShutdown);
    }
}

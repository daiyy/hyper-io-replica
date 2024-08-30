use std::rc::Rc;
use std::collections::HashMap;
use smol::net::unix::UnixListener;
use smol::stream::StreamExt;
use smol::io::{BufReader, AsyncReadExt, AsyncWriteExt};
use smol::io::AsyncBufReadExt;
use serde::{Serialize, Deserialize};
use crate::state::GlobalTgtState;
use crate::region::Region;
use crate::recover::RecoverCtrl;

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
enum CommandOp {
    Mode,
    Region,
    Recover,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
enum CommandDir {
    Get,
    Set,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub(crate) struct Command {
    op: CommandOp,
    dir: CommandDir,
    params: HashMap<String, serde_json::Value>,
}

impl Command {
    pub async fn execute(self, state: Rc<GlobalTgtState>, region: Rc<Region>, recover: Rc<RecoverCtrl>) -> Option<String> {
        match self.op {
            CommandOp::Mode => {
                match self.dir {
                    CommandDir::Get => {
                        return Some(format!("{:?}", state));
                    },
                    CommandDir::Set => {
                        if let Some(mode) = self.params.get("mode") {
                            let msg = match mode.as_u64().expect("invalid value of 'mode'") {
                                3 => {
                                    recover.rebuild_mode_forward_part(region);
                                    recover.kickoff();
                                    state.set_logging_enable();
                                    format!("state set to {:?}", state)
                                },
                                v @ _ => {
                                    format!("unkown argument 'mode' {}", v)
                                }
                            };
                            return Some(msg);
                        }
                        return Some(format!("incomplete argument: 'mode' missed"));
                    },
                }
            },
            CommandOp::Region => {
                match self.dir {
                    CommandDir::Get => {
                        if region.is_dirty() {
                            return Some(format!("g_region is DIRTY {:?}", region.collect()));
                        } else {
                            return Some(format!("g_region is CLEAN"));
                        }
                    },
                    CommandDir::Set => {
                    },
                }
            },
            CommandOp::Recover => {
                match self.dir {
                    CommandDir::Get => {
                        return Some(format!("{:?}", recover));
                    },
                    CommandDir::Set => {
                    },
                }
            },
        }
        None
    }
}

pub(crate) struct CommandChannel {
    listener: UnixListener,
}

impl CommandChannel {
    pub fn new() -> Self {
        let listener = UnixListener::bind("/tmp/.hior_mgmt").expect("unable to bind unix sock");
        Self {
            listener: listener,
        }
    }

    pub async fn main_handler(&self, state: Rc<GlobalTgtState>, region: Rc<Region>, recover: Rc<RecoverCtrl>) {
        let mut incoming = self.listener.incoming();
        while let Some(stream) = incoming.next().await {
            let mut stream = stream.expect("failed to get unix stream");
            let mut reader = BufReader::new(stream.clone());
            let mut buf_in = Vec::new();
            let _ = reader.read_until(b'\0', &mut buf_in).await;
            buf_in.pop(); // remove tailing \0
            let cmd: Command = serde_json::from_slice(&buf_in).expect("unable to deser Command bytes");
            match cmd.execute(state.clone(), region.clone(), recover.clone()).await {
                Some(resp) => {
                    let _ = stream.write_all(resp.as_bytes()).await;
                },
                None => {},
            }
        }
    }
}

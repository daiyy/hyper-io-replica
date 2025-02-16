use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use log::warn;
use smol::LocalExecutor;
use smol::net::unix::{UnixListener, UnixStream};
use smol::stream::StreamExt;
use smol::io::{BufReader, AsyncWriteExt};
use smol::io::AsyncBufReadExt;
use serde::{Serialize, Deserialize};
use crate::state::GlobalTgtState;
use crate::region::Region;
use crate::recover::RecoverCtrl;
use crate::pool::TgtPendingBlocksPool;
use crate::stats::Stats;
use crate::mgmt_proto::*;

pub(crate) struct Global<T> {
    pub(crate) state: Rc<GlobalTgtState>,
    pub(crate) region: Rc<Region>,
    pub(crate) recover: Rc<RecoverCtrl>,
    pub(crate) pool: Rc<RefCell<TgtPendingBlocksPool<T>>>,
}

impl<T> Clone for Global<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            region: self.region.clone(),
            recover: self.recover.clone(),
            pool: self.pool.clone(),
        }
    }
}

impl<T> Global<T> {
    fn new(g_state: Rc<GlobalTgtState>, g_region: Rc<Region>,
        g_recover: Rc<RecoverCtrl>, g_pool: Rc<RefCell<TgtPendingBlocksPool<T>>>) -> Self
    {
        Self {
            state: g_state,
            region: g_region,
            recover: g_recover,
            pool: g_pool,
        }
    }
}

impl Command {
    pub async fn execute<T>(self, global: Global<T>) -> Option<String> {
        let state = global.state.clone();
        let recover = global.recover.clone();
        let region = global.region.clone();
        match self.op {
            CommandOp::Mode => {
                match self.dir {
                    CommandDir::Get => {
                        return Some(format!("{:?}", state));
                    },
                    CommandDir::Set => {
                        if let Some(mode) = self.params.get("mode") {
                            // TODO: pre-transition check
                            let msg = match mode.as_u64().expect("invalid value of 'mode'") {
                                0 => {
                                    state.set_logging_disable();
                                    warn!("state set to {:?}, logging disabled", state);
                                    format!("state set to {:?}", state)
                                },
                                2 => {
                                    recover.rebuild_mode_forward_full();
                                    recover.kickoff();
                                    state.set_logging_enable();
                                    format!("state set to {:?}", state)
                                },
                                3 => {
                                    recover.rebuild_mode_forward_part(region);
                                    recover.kickoff();
                                    state.set_logging_enable();
                                    format!("state set to {:?}", state)
                                },
                                4 => {
                                    recover.rebuild_mode_reverse_full();
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
                        let (inflight, pending, total) = recover.stat();
                        let mode = recover.mode();
                        return Some(format!("mode: {}, inflight/pending/total {}/{}/{}", mode, inflight, pending, total));
                    },
                    CommandDir::Set => {
                    },
                }
            },
            CommandOp::Stat => {
                match self.dir {
                    CommandDir::Get => {
                        let stats = Stats::new(global).collect();
                        return Some(serde_json::to_string(&stats).expect("failed to encode stat to json string"));
                    },
                    CommandDir::Set => {
                        return None;
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
    pub fn new(unix_sock: &Path) -> Self {
        let listener = UnixListener::bind(unix_sock).expect("unable to bind unix sock");
        Self {
            listener: listener,
        }
    }

    pub async fn main_handler<'a, T: 'a>(&self, state: Rc<GlobalTgtState>, region: Rc<Region>,
            recover: Rc<RecoverCtrl>, pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, exec: Rc<LocalExecutor<'a>>)
    {
        let global = Global::new(state.clone(), region.clone(), recover.clone(), pool.clone());
        let mut incoming = self.listener.incoming();
        while let Some(stream) = incoming.next().await {
            let stream = stream.expect("failed to get unix stream");
            let c_global = global.clone();
            let task = exec.spawn(async move {
                Self::cmd_handler(stream, c_global).await;
            });
            task.detach();
        }
    }

    // handle command stream in loop until EOF received
    pub async fn cmd_handler<T>(mut stream: UnixStream, global: Global<T>) {
        loop {
            let mut reader = BufReader::new(stream.clone());
            let mut buf_in = Vec::new();
            let _ = reader.read_until(b'\0', &mut buf_in).await;
            buf_in.pop(); // remove tailing \0
            if buf_in.len() == 0 {
                // EOF received
                stream.shutdown(smol::net::Shutdown::Both).expect("failed to close stream");
                return;
            }
            let cmd: Command = serde_json::from_slice(&buf_in).expect("unable to deser Command bytes");
            match cmd.execute(global.clone()).await {
                Some(resp) => {
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.write(b"\0").await;
                },
                None => {},
            }
        }
    }
}

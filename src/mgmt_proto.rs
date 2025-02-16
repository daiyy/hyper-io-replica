use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub enum CommandOp {
    Mode,
    Region,
    Recover,
    Stat,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub enum CommandDir {
    Get,
    Set,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct Command {
    pub op: CommandOp,
    pub dir: CommandDir,
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum RunMode {
    ForwardFull = 2,
    ForwardPart = 3,
    ReverseFull = 4,
    GraceShutdown = 255,
}

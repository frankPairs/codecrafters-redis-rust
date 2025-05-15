use std::io::Write;
use std::net::TcpStream;

use crate::commands::Command;
use crate::data_types::{RespDataType, RespEncoder};

pub trait Response {
    fn reply(self, stream: &mut TcpStream);
}

#[derive(Debug)]
pub struct PingResponse;

impl Response for PingResponse {
    fn reply(self, stream: &mut TcpStream) {
        let res = RespEncoder::encode(RespDataType::SimpleString(Command::Pong.to_string()));

        let _ = stream.write_all(res.as_bytes());
    }
}

pub struct ResponseBuilder {
    command: Command,
}

impl ResponseBuilder {
    pub fn new(command: Command) -> Self {
        ResponseBuilder { command }
    }

    pub fn build(self) -> impl Response {
        match self.command {
            Command::Ping => PingResponse,
            Command::Pong => PingResponse,
        }
    }
}

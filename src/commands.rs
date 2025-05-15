use std::io::Read;
use std::net::TcpStream;

use crate::data_types::RespDataType;

const MAX_BYTES_STREAM_BUFFER: usize = 256;

#[derive(Debug)]
pub enum CommandDecoderError {
    InvalidType,
    EmptyCommand,
}

impl std::fmt::Display for CommandDecoderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandDecoderError::InvalidType => {
                write!(f, "Invalid RESP data type. Commands must be arrays.")
            }
            CommandDecoderError::EmptyCommand => {
                write!(f, "Empty command error.")
            }
        }
    }
}

pub struct CommandReader<'a> {
    stream: &'a mut TcpStream,
}

impl<'a> CommandReader<'a> {
    pub fn new(stream: &'a mut TcpStream) -> Self {
        CommandReader { stream }
    }

    pub fn read(&mut self) -> String {
        let mut bytes_received: Vec<u8> = vec![];
        let mut buffer = [0u8; MAX_BYTES_STREAM_BUFFER];

        loop {
            let bytes_read = self
                .stream
                .read(&mut buffer)
                .expect("An error ocurred while reading from stream");

            bytes_received.extend_from_slice(&buffer[..bytes_read]);

            if bytes_read < MAX_BYTES_STREAM_BUFFER {
                break;
            }
        }

        String::from_utf8_lossy(&bytes_received).to_string()
    }
}

#[derive(Debug)]
pub enum Command {
    Ping,
    Pong,
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping => write!(f, "PING"),
            Command::Pong => write!(f, "PONG"),
        }
    }
}

impl TryFrom<RespDataType> for Command {
    type Error = CommandDecoderError;

    fn try_from(values: RespDataType) -> Result<Self, Self::Error> {
        let stringify_values = values.to_string();

        match stringify_values {
            v if v.starts_with("PING") => Ok(Command::Ping),
            v if v.starts_with("PONG") => Ok(Command::Pong),
            _ => Err(CommandDecoderError::InvalidType),
        }
    }
}

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

pub struct CommandDecoder;

impl CommandDecoder {
    pub fn decode(resp: RespDataType) -> Result<Command, CommandDecoderError> {
        match resp {
            RespDataType::Array(v) => {
                let values: Vec<&str> = v.iter().filter_map(|dt| dt.as_string()).collect();

                if values.is_empty() {
                    return Err(CommandDecoderError::EmptyCommand);
                }

                if values.len() == 1 {
                    return values.first().unwrap().to_string().try_into();
                }

                todo!("Implement logic when command contains more than 1 value");
            }
            _ => Err(CommandDecoderError::InvalidType),
        }
    }
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
    PING,
    PONG,
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::PING => write!(f, "PING"),
            Command::PONG => write!(f, "PONG"),
        }
    }
}

impl TryFrom<String> for Command {
    type Error = CommandDecoderError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "PING" => Ok(Command::PING),
            "PONG" => Ok(Command::PONG),
            _ => Err(CommandDecoderError::InvalidType),
        }
    }
}

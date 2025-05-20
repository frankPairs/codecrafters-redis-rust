use std::io::Write;
use std::net::TcpStream;

use crate::data_types::{RespDataType, RespEncoder};

#[derive(Debug)]
pub enum CommandError {
    InvalidCommand(String),
    InvalidFormat(String),
    EmptyCommand,
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::InvalidCommand(command_name) => {
                write!(f, "Invalid Command: {}", command_name)
            }
            CommandError::InvalidFormat(err) => {
                write!(f, "Invalid Format Error: {}", err)
            }
            CommandError::EmptyCommand => {
                write!(f, "Empty command error.")
            }
        }
    }
}

pub struct CommandConverter {
    values: Vec<String>,
}

impl CommandConverter {
    pub fn convert(&self) -> Result<Box<dyn Command>, CommandError> {
        let command_name = self.get_command_name().ok_or(CommandError::EmptyCommand)?;

        match command_name {
            name if name.starts_with("PING") => Ok(Box::new(PingCommand)),
            name if name.starts_with("ECHO") => Ok(Box::new(EchoCommand(self.values.clone()))),
            _ => Err(CommandError::InvalidCommand(
                "Command does not exists".to_string(),
            )),
        }
    }

    /// The first (and sometimes also the second) bulk string in the array is the command's name.
    fn get_command_name(&self) -> Option<String> {
        match self.values.len() {
            0 => None,
            1 => self.values.first().cloned(),
            _ => {
                let command_values = self.values.iter().take(2).cloned().collect::<String>();

                Some(command_values)
            }
        }
    }
}

impl TryFrom<RespDataType> for CommandConverter {
    type Error = CommandError;

    fn try_from(value: RespDataType) -> Result<Self, Self::Error> {
        match value {
            RespDataType::Array(values) => {
                let str_values = values
                    .iter()
                    .filter_map(|value| match value {
                        RespDataType::BulkString(value) => Some(value.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<String>>();

                Ok(CommandConverter { values: str_values })
            }
            _ => Err(CommandError::InvalidCommand(String::from(
                "Command must be an array",
            ))),
        }
    }
}

pub trait Command {
    fn generate(&self) -> String;

    fn reply(&self, stream: &mut TcpStream) {
        let buf = self.generate();

        let _ = stream.write_all(buf.as_bytes());
    }
}

#[derive(Debug)]
struct PingCommand;

impl Command for PingCommand {
    fn generate(&self) -> String {
        RespEncoder::encode(RespDataType::SimpleString("PONG".to_string()))
    }
}

#[derive(Debug)]
struct EchoCommand(Vec<String>);

impl Command for EchoCommand {
    fn generate(&self) -> String {
        let arg = self.0.iter().skip(1).take(1).cloned().collect::<String>();

        RespEncoder::encode(RespDataType::BulkString(arg.to_string()))
    }
}

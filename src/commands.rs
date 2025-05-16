use crate::data_types::RespDataType;

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

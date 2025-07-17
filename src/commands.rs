use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;

use chrono::{DateTime, Duration, Utc};

use crate::resp::data_types::{RespDataType, RespEncoder};
use crate::resp::reader::RespReader;
use crate::server::{ServerConfig, ServerInfo};
use crate::store::{Store, StoreValueBuilder};

#[derive(Debug)]
pub enum CommandError {
    InvalidCommand(String),
    InvalidFormat(String),
    InvalidCommandOptionName(String),
    InvalidCommandOptionValue(String),
    InvalidInfoArg(String),
    EmptyCommand,
    Store(String),
    Reply(String),
    Request(io::Error),
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
            CommandError::InvalidCommandOptionName(err) => {
                write!(f, "Invalid Command Option Name Error: {}", err)
            }
            CommandError::InvalidCommandOptionValue(err) => {
                write!(f, "Invalid Command Option Value Error: {}", err)
            }
            CommandError::EmptyCommand => {
                write!(f, "Empty command error.")
            }
            CommandError::Reply(err) => {
                write!(f, "Command reply error: {}", err)
            }
            CommandError::Request(err) => {
                write!(f, "Command reply error: {}", err)
            }
            CommandError::Store(err) => {
                write!(f, "Store error: {}", err)
            }
            CommandError::InvalidInfoArg(err) => {
                write!(f, "Invalid Info Arg error: {}", err)
            }
        }
    }
}

impl std::error::Error for CommandError {}

pub struct CommandWriter<'a> {
    args: Vec<String>,
    stream: &'a mut TcpStream,
}

impl<'a> CommandWriter<'a> {
    pub fn new(stream: &'a mut TcpStream) -> Self {
        Self {
            args: Vec::new(),
            stream,
        }
    }

    pub fn from_resp_data_type(
        value: RespDataType,
        stream: &'a mut TcpStream,
    ) -> Result<CommandWriter<'a>, CommandError> {
        match value {
            RespDataType::Array(values) => {
                let str_values = values
                    .iter()
                    .filter_map(|value| match value {
                        RespDataType::BulkString(value) => Some(value.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<String>>();

                Ok(CommandWriter {
                    args: str_values,
                    stream,
                })
            }
            _ => Err(CommandError::InvalidCommand(String::from(
                "Command must be an array",
            ))),
        }
    }

    pub async fn write(
        self,
        store: Arc<Mutex<Store>>,
        server_config: Arc<ServerConfig>,
        server_info: Arc<ServerInfo>,
    ) -> Result<(), CommandError> {
        let command_name = self.get_command_name().ok_or(CommandError::EmptyCommand)?;

        let command: Result<Box<dyn Command>, CommandError> = match command_name.to_uppercase() {
            name if name.starts_with("PING") => Ok(Box::new(PingCommand)),
            name if name.starts_with("ECHO") => Ok(Box::new(EchoCommand::new(self.args.clone()))),
            name if name.starts_with("SET") => {
                Ok(Box::new(SetCommand::new(self.args.clone(), store)))
            }
            name if name.starts_with("GET") => {
                Ok(Box::new(GetCommand::new(self.args.clone(), store)))
            }
            name if name.starts_with("CONFIG GET") => Ok(Box::new(ConfigGetCommand::new(
                self.args.clone(),
                server_config,
            ))),
            name if name.starts_with("KEYS") => {
                Ok(Box::new(KeysCommand::new(self.args.clone(), store)))
            }
            name if name.starts_with("INFO") => {
                Ok(Box::new(InfoCommand::new(self.args.clone(), server_info)))
            }
            _ => Err(CommandError::InvalidCommand(
                "Command does not exists".to_string(),
            )),
        };

        let buf = command?.generate_reply()?;

        self.stream
            .write_all(buf.as_bytes())
            .await
            .map_err(|err| CommandError::Reply(err.to_string()))
    }

    pub async fn write_request(
        &mut self,
        command: Box<dyn Command>,
    ) -> Result<Option<RespDataType>, CommandError> {
        let buf = command.generate_request()?;

        let _ = self
            .stream
            .write_all(buf.as_bytes())
            .await
            .map_err(|err| CommandError::Reply(err.to_string()));

        let mut reader = RespReader::new(self.stream);

        reader
            .read()
            .await
            .map_err(|err| CommandError::Reply(err.to_string()))
    }

    /// The first (and sometimes also the second) bulk string in the array is the command's name.
    fn get_command_name(&self) -> Option<String> {
        match self.args.len() {
            0 => None,
            1 => self.args.first().cloned(),
            _ => {
                let command_values = self.args.iter().take(2).cloned().collect::<Vec<String>>();

                Some(command_values.join(" "))
            }
        }
    }
}

pub trait Command: Send + Sync {
    fn generate_reply(&self) -> Result<String, CommandError>;
    fn generate_request(&self) -> Result<String, CommandError> {
        unimplemented!("This command does not implement a request");
    }
}

#[derive(Debug)]
pub struct PingCommand;

impl Command for PingCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        Ok(RespEncoder::encode(RespDataType::SimpleString(
            "PONG".to_string(),
        )))
    }

    fn generate_request(&self) -> Result<String, CommandError> {
        Ok(RespEncoder::encode(RespDataType::Array(vec![
            RespDataType::SimpleString("PING".to_string()),
        ])))
    }
}

#[derive(Debug)]
struct EchoCommand {
    args: Vec<String>,
}

impl EchoCommand {
    fn new(args: Vec<String>) -> Self {
        Self { args }
    }
}

impl Command for EchoCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        let arg = self.args.get(1).ok_or(CommandError::InvalidFormat(
            "ECHO command is missing a value".to_string(),
        ))?;

        Ok(RespEncoder::encode(RespDataType::BulkString(
            arg.to_string(),
        )))
    }
}

enum SetCommandOption {
    PX(DateTime<Utc>),
}

struct SetCommandOptionParser;

impl SetCommandOptionParser {
    fn parse(args: Vec<String>) -> Result<Vec<SetCommandOption>, CommandError> {
        let mut options: Vec<SetCommandOption> = vec![];
        let chunks = args.chunks(2);

        for chunk in chunks {
            let option_name = chunk.first().ok_or(CommandError::InvalidCommandOptionName(
                "Command option cannot be None.".to_string(),
            ))?;

            match option_name.to_uppercase().as_str() {
                "PX" => {
                    let option_value =
                        chunk.get(1).ok_or(CommandError::InvalidCommandOptionValue(
                            "PX option must contain a number.".to_string(),
                        ))?;
                    let option_value: i64 = option_value.parse().map_err(|_| {
                        CommandError::InvalidCommandOptionValue(
                            "PX option must contain a positive number.".to_string(),
                        )
                    })?;

                    let exp = Utc::now() + Duration::milliseconds(option_value);

                    options.push(SetCommandOption::PX(exp));
                }
                option => {
                    return Err(CommandError::InvalidCommandOptionName(format!(
                        "Command option {} is not valid.",
                        option
                    )))
                }
            };
        }

        Ok(options)
    }
}

#[derive(Debug)]
struct SetCommand {
    store: Arc<Mutex<Store>>,
    args: Vec<String>,
}

impl SetCommand {
    fn new(args: Vec<String>, store: Arc<Mutex<Store>>) -> Self {
        Self { args, store }
    }
}

impl Command for SetCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        let mut args = self.args.iter().skip(1);
        let key = args.next().ok_or(CommandError::InvalidFormat(
            "SET command must contain a key".to_string(),
        ))?;
        let value = args.next().ok_or(CommandError::InvalidFormat(
            "SET command must contain a value".to_string(),
        ))?;

        let args: Vec<String> = args.cloned().collect();

        let mut store_value_builder = StoreValueBuilder::new();

        store_value_builder.with_value(value);

        let options = SetCommandOptionParser::parse(args)?;

        for option in options {
            match option {
                SetCommandOption::PX(exp) => {
                    store_value_builder.with_exp(exp);
                }
            };
        }

        let mut store = self.store.lock().map_err(|_| {
            CommandError::Store(format!(
                "Error when trying the set the value {} to {}",
                value, key
            ))
        })?;

        let store_value = store_value_builder.build();

        store.set(key, store_value);

        Ok(RespEncoder::encode(RespDataType::SimpleString(
            "OK".to_string(),
        )))
    }
}

#[derive(Debug)]
struct GetCommand {
    store: Arc<Mutex<Store>>,
    args: Vec<String>,
}

impl GetCommand {
    fn new(args: Vec<String>, store: Arc<Mutex<Store>>) -> Self {
        Self { args, store }
    }
}

impl Command for GetCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        let mut args = self.args.iter().skip(1);
        let key = args.next().ok_or(CommandError::InvalidFormat(
            "GET command must contain a key".to_string(),
        ))?;

        let store = self.store.lock().map_err(|_| {
            CommandError::Store(format!("Error when trying the get value from {}", key))
        })?;

        match store.get(key) {
            Some(store_value) => match store_value.exp {
                Some(exp) => {
                    let now = Utc::now();

                    if exp < now {
                        Ok(RespEncoder::encode(RespDataType::NullBulkString))
                    } else {
                        Ok(RespEncoder::encode(RespDataType::BulkString(
                            store_value.value.clone(),
                        )))
                    }
                }
                None => Ok(RespEncoder::encode(RespDataType::BulkString(
                    store_value.value.clone(),
                ))),
            },
            None => Ok(RespEncoder::encode(RespDataType::NullBulkString)),
        }
    }
}

#[derive(Debug)]
struct ConfigGetCommand {
    args: Vec<String>,
    server_config: Arc<ServerConfig>,
}

impl ConfigGetCommand {
    fn new(args: Vec<String>, server_config: Arc<ServerConfig>) -> Self {
        Self {
            args,
            server_config,
        }
    }
}

impl Command for ConfigGetCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        let mut args = self.args.iter().skip(2);
        let config_key = args.next().ok_or(CommandError::InvalidFormat(
            "CONFIG GET command must contain at least one configuration key".to_string(),
        ))?;

        let config_value = match config_key.as_str() {
            "dir" => self.server_config.dir.clone(),
            "dbfilename" => self.server_config.dbfilename.clone(),
            _ => None,
        };

        match config_value {
            Some(value) => Ok(RespEncoder::encode(RespDataType::Array(vec![
                RespDataType::BulkString(config_key.to_string()),
                RespDataType::BulkString(value.to_string_lossy().to_string()),
            ]))),
            None => Ok(RespEncoder::encode(RespDataType::NullBulkString)),
        }
    }
}

#[derive(Debug)]
struct KeysCommand {
    args: Vec<String>,
    store: Arc<Mutex<Store>>,
}

impl KeysCommand {
    fn new(args: Vec<String>, store: Arc<Mutex<Store>>) -> Self {
        Self { args, store }
    }
}

impl Command for KeysCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        let pattern = self.args.get(1).ok_or(CommandError::InvalidFormat(
            "Args command is missing a value".to_string(),
        ))?;

        match pattern.as_str() {
            "*" => {
                let store = self.store.lock().map_err(|_| {
                    CommandError::Store("Error when trying lock store for getting keys".to_string())
                })?;

                let keys = store.get_all_keys();

                Ok(RespEncoder::encode(RespDataType::Array(
                    keys.into_iter().map(RespDataType::BulkString).collect(),
                )))
            }
            _ => {
                unimplemented!("At the moment, keys only supports * as argument");
            }
        }
    }
}

#[derive(Debug)]
enum InfoSection {
    Replication,
}

impl FromStr for InfoSection {
    type Err = CommandError;

    fn from_str(arg: &str) -> Result<Self, Self::Err> {
        match arg {
            "replication" => Ok(InfoSection::Replication),
            value => Err(CommandError::InvalidInfoArg(format!(
                "Info section {} is not supported",
                value
            ))),
        }
    }
}

#[derive(Debug)]
struct ServerInfoFormatter {
    info: Arc<ServerInfo>,
}

impl ServerInfoFormatter {
    fn new(info: Arc<ServerInfo>) -> Self {
        Self { info }
    }
}

impl std::fmt::Display for ServerInfoFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut info_stringify = String::new();

        info_stringify.push_str(format!("{}:{}\n", "role", self.info.role).as_str());
        info_stringify.push_str(format!("{}:{}\n", "master_replid", self.info.id).as_str());
        info_stringify
            .push_str(format!("{}:{}\n", "master_repl_offset", self.info.offset).as_str());

        write!(f, "{}", info_stringify)
    }
}

#[derive(Debug)]
struct InfoCommand {
    args: Vec<String>,
    info: Arc<ServerInfo>,
}

impl InfoCommand {
    fn new(args: Vec<String>, info: Arc<ServerInfo>) -> Self {
        Self { args, info }
    }
}

impl Command for InfoCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        let section_name = self.args.get(1).ok_or(CommandError::InvalidFormat(
            "Section name is missing".to_string(),
        ))?;
        let section = InfoSection::from_str(section_name.as_str())?;

        match section {
            InfoSection::Replication => {
                let info = ServerInfoFormatter::new(self.info.clone());

                Ok(RespEncoder::encode(RespDataType::BulkString(
                    info.to_string(),
                )))
            }
            _ => {
                unimplemented!("At the moment, keys only supports * as argument");
            }
        }
    }
}

#[derive(Debug)]
pub enum ReplconfCommandArg {
    ListeningPort(u16),
    Capa(String),
}

#[derive(Debug)]
pub struct ReplconfCommand {
    arg: ReplconfCommandArg,
}

impl ReplconfCommand {
    pub fn new(arg: ReplconfCommandArg) -> Self {
        Self { arg }
    }
}
impl Command for ReplconfCommand {
    fn generate_reply(&self) -> Result<String, CommandError> {
        Ok(RespEncoder::encode(RespDataType::SimpleString(
            "OK".to_string(),
        )))
    }

    fn generate_request(&self) -> Result<String, CommandError> {
        match &self.arg {
            ReplconfCommandArg::ListeningPort(port) => {
                Ok(RespEncoder::encode(RespDataType::Array(vec![
                    RespDataType::BulkString("REPLCONF".to_string()),
                    RespDataType::BulkString("listening-port".to_string()),
                    RespDataType::BulkString(port.to_string()),
                ])))
            }
            ReplconfCommandArg::Capa(value) => Ok(RespEncoder::encode(RespDataType::Array(vec![
                RespDataType::BulkString("REPLCONF".to_string()),
                RespDataType::BulkString("capa".to_string()),
                RespDataType::BulkString(value.clone()),
            ]))),
        }
    }
}

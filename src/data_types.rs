use std::str::Lines;

#[derive(Debug)]

pub enum RespDataType {
    Array(Vec<RespDataType>),
    BulkString(String),
    SimpleString(String),
}

impl std::fmt::Display for RespDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespDataType::BulkString(value) => write!(f, "{}", value),
            RespDataType::SimpleString(value) => write!(f, "{}", value),
            RespDataType::Array(values) => {
                let values_str = values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .join(" ");

                write!(f, "{}", values_str)
            }
        }
    }
}

pub struct RespDecoder;

impl RespDecoder {
    pub fn decode(lines: &mut Lines) -> Result<RespDataType, RespDecoderError> {
        match lines.next() {
            Some(line) if line.starts_with('*') => {
                let mut values: Vec<RespDataType> = vec![];
                let size = line
                    .strip_prefix("*")
                    .map(|value| value.parse::<usize>().unwrap_or(0))
                    .unwrap_or(0);

                for _ in 0..size {
                    match RespDecoder::decode(lines) {
                        Ok(value) => values.push(value),
                        Err(err) => return Err(err),
                    }
                }

                Ok(RespDataType::Array(values))
            }
            Some(line) if line.starts_with("$") => {
                let value = lines.next().unwrap_or("");

                Ok(RespDataType::BulkString(value.to_string()))
            }
            Some(line) if line.starts_with("+") => match line.strip_prefix("+") {
                Some(value) => Ok(RespDataType::SimpleString(value.to_string())),
                None => Ok(RespDataType::SimpleString("".to_string())),
            },
            Some(_) => Err(RespDecoderError::InvalidRespDataType),
            None => Err(RespDecoderError::InvalidRespDataType),
        }
    }
}

#[derive(Debug)]
pub enum RespDecoderError {
    InvalidRespDataType,
}

impl std::fmt::Display for RespDecoderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespDecoderError::InvalidRespDataType => write!(f, "Invalid RESP data type"),
        }
    }
}

pub struct RespEncoder;

impl RespEncoder {
    pub fn encode(data_type: RespDataType) -> String {
        match data_type {
            RespDataType::SimpleString(value) => {
                format!("+{}\r\n", value)
            }
            RespDataType::BulkString(value) => {
                format!("${}\r\n{}\r\n", value.len(), value)
            }
            RespDataType::Array(data_types) => {
                let size = data_types.len();
                let mut encoded_value = format!("*{}\r\n", size);

                for dt in data_types {
                    encoded_value = format!("{}{}", encoded_value, RespEncoder::encode(dt));
                }

                encoded_value
            }
        }
    }
}

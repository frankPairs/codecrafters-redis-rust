use tokio::net::TcpStream;

use super::data_types::RespDecoder;
use crate::{resp::data_types::RespDataType, server::ServerError, tcp::TcpStreamReader};

pub struct RespReader<'a> {
    stream: &'a mut TcpStream,
}

impl<'a> RespReader<'a> {
    pub fn new(stream: &'a mut TcpStream) -> Self {
        Self { stream }
    }

    pub async fn read(&mut self) -> Result<Option<RespDataType>, ServerError> {
        let mut reader = TcpStreamReader::new(self.stream);
        let message = reader
            .read()
            .await
            .expect("Error when reading from TCPStream");

        if message.is_empty() {
            return Ok(None);
        }

        let mut lines = message.lines();

        let resp_data_type =
            RespDecoder::decode(&mut lines).expect("Error when decoding string to RESP");

        Ok(Some(resp_data_type))
    }
}

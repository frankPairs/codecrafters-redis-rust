use std::net::TcpListener;

use codecrafters_redis::commands::{CommandDecoder, CommandReader};
use codecrafters_redis::data_types::RespDecoder;
use codecrafters_redis::response::{Response, ResponseBuilder};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut reader = CommandReader::new(&mut stream);
                let message = reader.read();
                let mut lines = message.lines();

                let resp_data_type =
                    RespDecoder::decode(&mut lines).expect("Error when decoding string to RESP");
                let command =
                    CommandDecoder::decode(resp_data_type).expect("Error when decoding a command");

                let response = ResponseBuilder::new(command).build();

                response.reply(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

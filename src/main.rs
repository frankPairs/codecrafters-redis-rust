use std::net::TcpListener;

use codecrafters_redis::commands::CommandConverter;
use codecrafters_redis::data_types::RespDecoder;
use codecrafters_redis::tcp::TcpStreamReader;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                std::thread::spawn(move || loop {
                    let mut reader = TcpStreamReader::new(&mut stream);
                    let message = reader.read();

                    if message.is_empty() {
                        break;
                    }

                    let mut lines = message.lines();

                    let resp_data_type = RespDecoder::decode(&mut lines)
                        .expect("Error when decoding string to RESP");

                    let command = CommandConverter::try_from(resp_data_type)
                        .expect("Error when decoding a command")
                        .convert()
                        .expect("Invalid command");

                    command.reply(&mut stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

use codecrafters_redis::commands::CommandBuilder;
use codecrafters_redis::data_types::RespDecoder;
use codecrafters_redis::store::Store;
use codecrafters_redis::tcp::TcpStreamReader;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store = Arc::new(Mutex::new(Store::default()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store_cloned = Arc::clone(&store);

                std::thread::spawn(move || loop {
                    let mut reader = TcpStreamReader::new(&mut stream);
                    let message = reader.read();

                    if message.is_empty() {
                        break;
                    }

                    let mut lines = message.lines();

                    let resp_data_type = RespDecoder::decode(&mut lines)
                        .expect("Error when decoding string to RESP");

                    let command = CommandBuilder::from_resp_data_type(resp_data_type)
                        .expect("Error when decoding a command")
                        .build(Arc::clone(&store_cloned))
                        .expect("Invalid command");

                    command.reply(&mut stream).unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

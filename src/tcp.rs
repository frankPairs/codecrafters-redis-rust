use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
};

pub struct TcpStreamReader<'a> {
    stream: &'a mut TcpStream,
}

impl<'a> TcpStreamReader<'a> {
    pub fn new(stream: &'a mut TcpStream) -> Self {
        TcpStreamReader { stream }
    }

    pub fn read(&mut self) -> String {
        let mut reader = BufReader::new(&mut self.stream);
        let bytes_received = reader
            .fill_buf()
            .expect("An error ocurred while reading from stream")
            .to_vec();

        reader.consume(bytes_received.len());

        String::from_utf8_lossy(&bytes_received).to_string()
    }
}

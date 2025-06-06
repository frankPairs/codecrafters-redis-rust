use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
};

pub struct TcpStreamReader<'a> {
    stream: &'a mut TcpStream,
}

impl<'a> TcpStreamReader<'a> {
    pub fn new(stream: &'a mut TcpStream) -> Self {
        TcpStreamReader { stream }
    }

    pub async fn read(&mut self) -> anyhow::Result<String> {
        let mut reader = BufReader::new(&mut self.stream);
        let buf = reader
            .fill_buf()
            .await
            .expect("An error ocurred while reading from stream")
            .to_vec();

        Ok(String::from_utf8_lossy(&buf).to_string())
    }
}

use std::net::{TcpStream, TcpListener};
use std::error::Error;

const CHUNKTPS_MAGIC: [u8; 4] = [0xde, 0xad, 0xbe, 0xef];

pub struct ChunktpsConnection {
    tcp_stream: TcpStream
}

impl ChunktpsConnection {
    pub fn new(tcp_stream: TcpStream) -> Self {
        ChunktpsConnection{ tcp_stream }
    }

    pub fn read_chunk() -> Result<Vec<u8>, Box<dyn Error>> {
        unimplemented!()
    }

    pub fn write_chunk(data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }
}

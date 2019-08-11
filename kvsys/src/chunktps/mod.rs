use std::net::TcpStream;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};

const CHUNKTPS_MAGIC: [u8; 4] = [0xde, 0xad, 0xbe, 0xef];

const CHUNKTPS_READER_OK: [u8; 5] = [0xde, 0xad, 0xbe, 0xef, 0xac];
const CHUNKTPS_READER_TE: [u8; 5] = [0xca, 0xfe, 0xba, 0xbe, 0xff];

#[derive(Debug)]
pub struct ChunktpsError {
    description: String
}

impl ChunktpsError {
    pub fn new(description: &str) -> Self {
        ChunktpsError { description: description.to_owned() }
    }
}

impl Display for ChunktpsError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "chunktps error: {}", self.description)
    }
}

impl Error for ChunktpsError {
}

pub struct ChunktpsConnection {
    tcp_stream: TcpStream
}

impl ChunktpsConnection {
    pub fn new(tcp_stream: TcpStream) -> Self {
        ChunktpsConnection{ tcp_stream }
    }

    pub fn read_chunk(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut magic = [0u8; 4];
        let mut size = [0u8; 2];

        self.tcp_stream.read_exact(&mut magic)?;
        self.tcp_stream.read_exact(&mut size)?;
        if magic != CHUNKTPS_MAGIC {
            let _ = self.tcp_stream.write(&CHUNKTPS_READER_TE);
            return Err(Box::new(ChunktpsError::new("incorrect chunktps magic!")));
        }
        let size = size[0] as usize * 256 + size[1] as usize;

        let mut recv_buffer = Vec::with_capacity(size);
        recv_buffer.resize_with(size, Default::default);
        self.tcp_stream.read_exact(recv_buffer.as_mut_slice())?;

        self.tcp_stream.write(&CHUNKTPS_READER_OK)?;
        Ok(recv_buffer)
    }

    pub fn write_chunk(&mut self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let size = data.len();
        let size = [(size / 256) as u8, (size % 256) as u8];

        self.tcp_stream.write(&CHUNKTPS_MAGIC)?;
        self.tcp_stream.write(&size)?;
        self.tcp_stream.write(data.as_slice())?;

        let mut client_reply = [0u8; 5];
        self.tcp_stream.read_exact(&mut client_reply)?;

        if client_reply == CHUNKTPS_READER_OK {
            Ok(())
        } else {
            Err(Box::new(ChunktpsError::new("client requested terminate")))
        }
    }
}

#[cfg!(test)]
mod test {
    use crate::chunktps::ChunktpsConnection;

    #[test]
    fn test_basic_rw() {

    }
}

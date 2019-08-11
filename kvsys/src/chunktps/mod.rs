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
        assert!(size <= 65535);
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

#[cfg(test)]
mod test {
    use crate::chunktps::ChunktpsConnection;
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_rw() {
        for _ in 0..10 {
            let data: [&[u8]; 8] = [
                b"this is the first message to send",
                b"deadbeef,cafebabe",
                b"       the   C O N N E C T I O N should have been established",
                b"cubical type theory and ice1000 are both perfect",
                b"Haskell considered harmful",
                b"\x32\x33\xff\xff\xfe\x7c\xde\xad\xbe\xef",
                b"",
                b"this is the last message to send"
            ];

            let t = thread::spawn(
                move || {
                    let listener = TcpListener::bind("127.0.0.1:8964").unwrap();
                    let (stream, _) = listener.accept().unwrap();
                    let mut chunktps = ChunktpsConnection::new(stream);
                    for &piece in data.iter() {
                        chunktps.write_chunk(piece.to_vec()).unwrap();
                    }
                }
            );

            thread::sleep(Duration::from_secs(1));
            let stream = TcpStream::connect("127.0.0.1:8964").unwrap();
            let mut chunktps = ChunktpsConnection::new(stream);
            for i in 0..data.len() {
                assert_eq!(chunktps.read_chunk().unwrap(), data[i].to_vec());
            }

            t.join().unwrap();
        }
    }
}

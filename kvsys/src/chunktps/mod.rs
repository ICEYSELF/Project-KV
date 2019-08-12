//! CHunked Transport Protocol (chunktp) is a project-specific data transporting protocol.
//!
//! Chunktps allows server to explicitly chunk their data, and wait for client to resolve the chunk.
//! It is up to the user to decide the binary data format and terminating condition. By the time
//! this document gets written, the only user of chunktp, Project-KV Protocol, uses empty chunk as
//! termination.
//!
//! Chunktps is based on TCP now. It is possible to port it to KCP or UDP, however.
//!
//! A typical echo-server, based on chunktp:
//! ```no_run
//!     use std::net::{TcpStream, TcpListener};
//!     use std::thread;
//!     use kvsys::chunktps::ChunktpConnection;
//!     // ...
//!     let tcp_listener = TcpListener::bind("127.0.0.1:4000").unwrap();
//!     for tcp_stream in tcp_listener.incoming() {
//!         let tcp_stream = tcp_stream.unwrap();
//!         let mut chunktps = ChunktpConnection::new(tcp_stream);
//!         thread::spawn(move || {
//!             loop {
//!                 let chunk = chunktps.read_chunk().unwrap();
//!                 // use empty chunk as termination
//!                 if chunk.len() == 0 {
//!                     break;
//!                 }
//!                 chunktps.write_chunk(chunk);
//!             }
//!         });
//!     }
//! ```

use std::net::TcpStream;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};

// The chunktp chunk format
//   - 4 bytes magic (0xdeadbeef)
//   - 2 bytes size, in big endian
//   - (size) bytes data
//
// After reading a chunk from sender, the receiver must send back a 5 bytes message
//   - OK means the message is successfully received by the client
//   - TE means a critical error occurred during transport, and the transport must shutdown
const CHUNKTPS_MAGIC: [u8; 4] = [0xde, 0xad, 0xbe, 0xef];
const CHUNKTPS_READER_OK: [u8; 5] = [0xde, 0xad, 0xbe, 0xef, 0xac];
const CHUNKTPS_READER_TE: [u8; 5] = [0xca, 0xfe, 0xba, 0xbe, 0xff];

/// Max size of a chunk, it is 65535 at this moment
pub const CHUNK_MAX_SIZE: usize = 65535;

/// The error type used by chunktp
#[derive(Debug)]
pub struct ChunktpError {
    description: String
}

impl ChunktpError {
    pub fn new(description: &str) -> Self {
        ChunktpError { description: description.to_owned() }
    }
}

impl Display for ChunktpError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "chunktps error: {}", self.description)
    }
}

impl Error for ChunktpError {
}

/// A chunktp connection, now chunktps connection supports TCP only
pub struct ChunktpConnection {
    tcp_stream: TcpStream
}

impl ChunktpConnection {
    /// Creates a chunktp connection over a TCP stream. It does not make any assumption, check or
    /// operation on the stream
    pub fn new(tcp_stream: TcpStream) -> Self {
        ChunktpConnection { tcp_stream }
    }

    /// Try reading a chunk from the chunktp connection, returns Err type if the TCP stream fails
    /// or the received buffer is ill-formed
    pub fn read_chunk(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut magic = [0u8; 4];
        let mut size = [0u8; 2];

        self.tcp_stream.read_exact(&mut magic)?;
        self.tcp_stream.read_exact(&mut size)?;
        if magic != CHUNKTPS_MAGIC {
            let _ = self.tcp_stream.write(&CHUNKTPS_READER_TE);
            return Err(Box::new(ChunktpError::new("incorrect chunktps magic!")));
        }
        let size = size[0] as usize * 256 + size[1] as usize;

        let mut recv_buffer = Vec::with_capacity(size);
        recv_buffer.resize_with(size, Default::default);
        self.tcp_stream.read_exact(recv_buffer.as_mut_slice())?;

        self.tcp_stream.write(&CHUNKTPS_READER_OK)?;
        Ok(recv_buffer)
    }

    /// Try writing a chunk into the chunktp connection, returns Err type if the TCP stream fails
    /// or the received buffer is ill-formed
    pub fn write_chunk(&mut self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let size = data.len();
        assert!(size <= CHUNK_MAX_SIZE);
        let size = [(size / 256) as u8, (size % 256) as u8];

        self.tcp_stream.write(&CHUNKTPS_MAGIC)?;
        self.tcp_stream.write(&size)?;
        self.tcp_stream.write(data.as_slice())?;

        let mut client_reply = [0u8; 5];
        self.tcp_stream.read_exact(&mut client_reply)?;

        match client_reply {
            CHUNKTPS_READER_OK => Ok(()),
            CHUNKTPS_READER_TE => Err(Box::new(ChunktpError::new("client requested terminate"))),
            _ => Err(Box::new(ChunktpError::new("client reply not understood")))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::chunktps::ChunktpConnection;
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
                    let mut chunktps = ChunktpConnection::new(stream);
                    for &piece in data.iter() {
                        chunktps.write_chunk(piece.to_vec()).unwrap();
                    }
                }
            );

            thread::sleep(Duration::from_secs(1));
            let stream = TcpStream::connect("127.0.0.1:8964").unwrap();
            let mut chunktps = ChunktpConnection::new(stream);
            for i in 0..data.len() {
                assert_eq!(chunktps.read_chunk().unwrap(), data[i].to_vec());
            }

            t.join().unwrap();
        }
    }
}

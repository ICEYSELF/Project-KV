use std::fmt;
use std::error::Error;

use crate::chunktps::ChunktpConnection;
use crate::kvstorage::{Key, Value};
use crate::kvserver::protocol::{Request, ReplyChunk};
use std::net::TcpStream;

#[derive(Debug)]
pub struct ServerError {
    description: String
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "client error: {}", self.description)
    }
}

impl Error for ServerError {
}

impl ServerError {
    pub fn new(description: &str) -> Self {
        ServerError { description: description.to_owned() }
    }
}

pub struct KVClient {
    chunktps: ChunktpConnection
}

impl KVClient {
    pub fn new(tcp_stream: TcpStream) -> Self {
        KVClient { chunktps: ChunktpConnection::new(tcp_stream) }
    }

    pub fn do_get<F, T>(&mut self, key: Key, result_handler: F) -> Result<T, Box<dyn Error>>
        where F: Fn(Option<Value>) -> T {
        self.chunktps.write_chunk(Request::Get(key).serialize())?;
        let reply = ReplyChunk::deserialize(self.chunktps.read_chunk()?)?;
        match reply {
            ReplyChunk::SingleValue(value ) => {
                Ok(result_handler(value))
            }
            _ => Err(Box::new(ServerError::new("unexpected reply chunk kind")))
        }
    }

    pub fn do_put(&mut self, key: Key, value: Value) -> Result<(), Box<dyn Error>> {
        self.chunktps.write_chunk(Request::Put(key, value).serialize())?;
        let reply = ReplyChunk::deserialize(self.chunktps.read_chunk()?)?;
        match reply {
            ReplyChunk::Success => {
                Ok(())
            },
            ReplyChunk::Error => {
                Err(Box::new(ServerError::new("error inserting kv pair")))
            }
            _ => Err(Box::new(ServerError::new("unexpected reply chunk kind")))
        }
    }

    pub fn do_scan<F, T>(&mut self, key1: Key, key2: Key, chunk_handler: F) -> Result<Vec<T>, Box<dyn Error>>
        where F: Fn(Vec<(Key, Value)>) -> T {
        self.chunktps.write_chunk(Request::Scan(key1, key2).serialize())?;
        let mut ret = Vec::new();
        loop {
            let chunk = self.chunktps.read_chunk()?;
            if chunk.len() == 0 {
                return Ok(ret)
            }
            let reply = ReplyChunk::deserialize(chunk)?;
            match reply {
                ReplyChunk::KVPairs(kv_pairs) => {
                    ret.push(chunk_handler(kv_pairs));
                },
                _ => return Err(Box::new(ServerError::new("unexpected reply chunk kind")))
            }
        }
    }

    pub fn do_delete<F, T>(&mut self, key: Key, result_handler: F) -> Result<T, Box<dyn Error>>
        where F: Fn(usize) -> T {
        self.chunktps.write_chunk(Request::Del(key).serialize())?;
        let reply = ReplyChunk::deserialize(self.chunktps.read_chunk()?)?;
        match reply {
            ReplyChunk::Number(number ) => {
                Ok(result_handler(number))
            },
            ReplyChunk::Error => {
                Err(Box::new(ServerError::new("error deleting kv pair")))
            }
            _ => Err(Box::new(ServerError::new("unexpected reply chunk kind")))
        }
    }

    pub fn do_close(&mut self) {
        let _ = self.chunktps.write_chunk(Request::Close.serialize());
    }
}

//! Client API of Project-KV

use std::fmt;
use std::error::Error;

use crate::chunktps::ChunktpConnection;
use crate::kvstorage::{Key, Value};
use crate::kvserver::protocol::{Request, ReplyChunk};
use std::net::TcpStream;

/// Error occurred on server, and received by client
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

/// A key-value storage client, basically a wrapper for `ChunktpConnection`
///
/// `KVClient` relies on callback functions to handle server returned results since server can
/// send reply in multi-chunk form, while caching all these chunks is somewhat expensive. If
/// there's an error when reading and parsing server reply, the callback function will not be
/// called. Read documentation of `do_xx` functions for further information
pub struct KVClient {
    chunktps: ChunktpConnection
}

impl KVClient {
    /// Creates a `KVClient` using the given `TcpStream`
    pub fn new(tcp_stream: TcpStream) -> Self {
        KVClient { chunktps: ChunktpConnection::new(tcp_stream) }
    }

    /// Trying get a value corresponding to the given `Key`
    ///
    /// The result handler function should accept an `Option<Value>` (since there may be no value
    /// corresponding to the key).
    ///
    /// An example result handler:
    /// ```no_run
    /// use kvsys::kvstorage::Value;
    /// fn handle_get_result(value: Option<Value>) -> () {
    ///    if let Some(value) = value {
    ///        println!("  {}", value);
    ///    } else {
    ///        println!("  Nil");
    ///    }
    ///}
    /// ```
    ///
    /// Returns `Err` if TCP connection fails or Chunktp fails
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

    /// Trying putting a `Key` - `Value` pair into server's storage
    ///
    /// This function does not require handler, instead, it returns `()` silently if everything
    /// goes on well.
    ///
    /// Returns `Err` if TCP connection fails, Chunktp fails or server fails.
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

    /// Trying scan all `Key` - `Value` pairs within interval [`key1`, `key2`), according to
    /// dictionary order
    ///
    /// The result handler function should accept a `Vec<(Key, Value)>`.  If the result handler
    /// returns `T`, this function returns `Result<Vec<T>, Box<dyn Error>>`
    ///
    /// An example result handler:
    /// ```no_run
    /// use kvsys::kvstorage::{Key, Value};
    /// fn handle_scan_result(kv_pairs: Vec<(Key, Value)>) -> () {
    ///     for (key, value) in kv_pairs.iter() {
    ///         println!("  {} => {}", key, value)
    ///     }
    /// }
    /// ```
    ///
    /// Returns `Err` if TCP connection fails or Chunktp fails
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

    /// Trying delete the `key` from storage
    ///
    /// The result handler function should accept a `usize`, rows affected by the delete operation
    ///
    /// An example result handler:
    /// ```no_run
    /// fn handle_delete_result(rows_affected: usize) -> () {
    ///     println!("  Ok, {} rows affected", rows_affected)
    /// }
    /// ```
    ///
    /// Returns `Err` if TCP connection fails, Chunktp fails or server fails
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

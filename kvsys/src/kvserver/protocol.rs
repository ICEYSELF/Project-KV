//! Describes the protocol used by KV-Server and KV-Client
//!
//! The client should use the `Request` APIs to serialize its request, and then send it to server.
//! The server will then make use of `Request` APIs to deserialize the request, use
//! `ServerReplyChunk` APIs to serialize its reply chunks. The client can then use `ReplyChunk` APIs
//! to deserialize a server reply chunk.

use crate::kvstorage::{Key, Value, KEY_SIZE, VALUE_SIZE};

use std::sync::Arc;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::error::Error;

/// The error type used by protocol module
#[derive(Debug)]
pub struct ProtocolError {
    description: String
}

impl ProtocolError {
    pub fn new(description: &str) -> Self {
        ProtocolError { description: description.to_owned() }
    }
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "protocol error: {}", self.description)
    }
}

impl Error for ProtocolError {
}

/// Size of a `Key` - `Value` pair, basically an alias to `KEY_SIZE + VALUE_SIZE`.
///
/// The transmission protocol (for example, chunktp) may have limits on the data size. This
/// constant can thus be used for "data per chunk" evaluation conveniently.
pub const KV_PAIR_SERIALIZED_SIZE: usize = KEY_SIZE + VALUE_SIZE;

const SCAN: u8 = b'S';
const PUT: u8 = b'P';
const GET: u8 = b'G';
const DEL: u8 = b'D';
const CLOSE: u8 = b'C';

// Request format
//  -- 1 byte functionality
//     'S'
//     -- KEY_SIZE key1
//     -- KEY_SIZE key2
//     'P'
//     -- KEY_SIZE key
//     -- VALUE_SIZE value
//     'G'
//     -- KEY_SIZE key
//     'D'
//     -- KEY_SIZE key
//     'C'

/// A request sent by client or received by server, see its enumerators for further information
pub enum Request {
    Scan(Key, Key),
    Put(Key, Value),
    Get(Key),
    Del(Key),
    Close
}

impl Request {
    /// Serialize a `Request` into a byte buffer
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Request::Scan(key1, key2) => {
                let mut ret = vec![SCAN];
                ret.append(&mut key1.serialize());
                ret.append(&mut key2.serialize());
                ret
            },
            Request::Put(key, value) => {
                let mut ret = vec![PUT];
                ret.append(&mut key.serialize());
                ret.append(&mut value.serialize());
                ret
            },
            Request::Get(key) => {
                let mut ret = vec![GET];
                ret.append(&mut key.serialize());
                ret
            },
            Request::Del(key) => {
                let mut ret = vec![DEL];
                ret.append(&mut key.serialize());
                ret
            },
            Request::Close => {
                vec![CLOSE]
            }
        }
    }

    /// Deserialize a byte buffer and construct a `Request` enum.
    ///
    /// Fails if the buffer does not meet the format of a `Request`, panics if the buffer is empty
    pub fn deserialize_from(raw: Vec<u8>) -> Result<Self, ProtocolError> {
        assert!(raw.len() > 0);
        match raw[0] {
            SCAN => {
                if raw.len() != 1 + KEY_SIZE * 2 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let key1 = Key::from_slice(&raw[1..1+KEY_SIZE]);
                    let key2 = Key::from_slice(&raw[1+KEY_SIZE..1+KEY_SIZE*2]);
                    Ok(Request::Scan(key1, key2))
                }
            },
            PUT => {
                if raw.len() != 1 + KEY_SIZE + VALUE_SIZE {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let key = Key::from_slice(&raw[1..1+KEY_SIZE]);
                    let value = Value::from_slice(&raw[1+KEY_SIZE..1+KEY_SIZE+VALUE_SIZE]);
                    Ok(Request::Put(key, value))
                }
            },
            GET => {
                if raw.len() != 1 + KEY_SIZE {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let key = Key::from_slice(&raw[1..1+KEY_SIZE]);
                    Ok(Request::Get(key))
                }
            },
            DEL => {
                if raw.len() != 1 + KEY_SIZE {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let key = Key::from_slice(&raw[1..1+KEY_SIZE]);
                    Ok(Request::Del(key))
                }
            },
            CLOSE=> {
                Ok(Request::Close)
            }
            _ => {
                Err(ProtocolError::new("incorrect response chunk identifier"))
            }
        }
    }
}

// Reply format
// -- 1 byte data kind
//    'S'
//    -- VALUE_SIZE value
//    'N'
//    -- 8 bytes number
//    'P'
//    -- multiple KEY_SIZE + VALUE_SIZE key-value pairs
//    'E'
//    'A'

const SINGLE_VALUE: u8 = b'S';
const NUMBER: u8 = b'N';
const KV_PAIRS: u8 = b'P';
const ERROR: u8 = b'E';
const SUCCESS: u8 = b'A';

/// A reply chunk sent by server, see its enumerators for further information
///
/// The `ServerReplyChunk` is specially optimized for server side program to serialize and send
/// data (without copying or blocking anything). To deserialize chunks, use `ReplyChunk` instead
pub enum ServerReplyChunk<'a> {
    SingleValue(Option<Arc<Value>>),
    Number(usize),
    KVPairs(&'a [(Key, Arc<Value>)]),
    Error,
    Success
}

impl ServerReplyChunk<'_> {
    /// Serialize a `ServerReplyBuffer` into a byte buffer
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            ServerReplyChunk::SingleValue(value) => {
                let mut ret = vec![SINGLE_VALUE];
                if let Some(value) = value {
                    ret.append(&mut value.serialize());
                }
                ret
            },
            ServerReplyChunk::Number(number) => {
                let mut ret = vec![NUMBER];
                let mut number = *number;
                let mut arr = [0u8; 8];
                for i in (0..8).rev() {
                    arr[i] = (number % 256) as u8;
                    number /= 256;
                }
                ret.append(&mut arr.to_vec());
                ret
            },
            ServerReplyChunk::KVPairs(pairs) => {
                let mut ret = vec![KV_PAIRS];
                for (key, value) in pairs.iter() {
                    ret.append(&mut key.serialize());
                    ret.append(&mut value.serialize());
                }
                ret
            },
            ServerReplyChunk::Success => {
                vec![SUCCESS]
            },
            ServerReplyChunk::Error => {
                vec![ERROR]
            }
        }
    }
}

/// A reply chunk received by client, see its enumerators for further information
///
/// The `ReplyChunk` is specially created by client side program to deserialize and resolve reply
/// data. To serialize chunks, use `ServerReplyChunk` instead
pub enum ReplyChunk {
    SingleValue(Option<Value>),
    Number(usize),
    KVPairs(Vec<(Key, Value)>),
    Success,
    Error
}

impl ReplyChunk {
    /// Deserialize a byte buffer and construct a `ReplyChunk` enum.
    ///
    /// Fails if the buffer does not meet the format of a `ReplyChunk`,
    /// panics if the buffer is empty
    pub fn deserialize(raw: Vec<u8>) -> Result<Self, ProtocolError> {
        assert!(!raw.is_empty());
        match raw[0] {
            SINGLE_VALUE => {
                if raw.len() == 1 {
                  Ok(ReplyChunk::SingleValue(None))
                } else if raw.len() == 1 + VALUE_SIZE {
                    let ret = Value::from_slice(&raw[1..1+VALUE_SIZE]);
                    Ok(ReplyChunk::SingleValue(Some(ret)))
                } else {
                    Err(ProtocolError::new("incorrect content length"))
                }
            },
            NUMBER => {
                if raw.len() != 1 + KEY_SIZE {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut ret = 0;
                    for &byte in raw[1..1+KEY_SIZE].iter() {
                        ret *= 256;
                        ret += byte as usize;
                    }
                    Ok(ReplyChunk::Number(ret))
                }
            },
            KV_PAIRS => {
                if (raw.len() - 1) % (KEY_SIZE + VALUE_SIZE) != 0 {
                    return Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut ret = Vec::new();
                    for i in (1..raw.len()).step_by(KEY_SIZE + VALUE_SIZE) {
                        let key = Key::from_slice(&raw[i..i + KEY_SIZE]);
                        let value = Value::from_slice(&raw[i+KEY_SIZE..i+KEY_SIZE+VALUE_SIZE]);
                        ret.push((key, value))
                    }
                    Ok(ReplyChunk::KVPairs(ret))
                }
            },
            SUCCESS => {
                if raw.len() != 1 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    Ok(ReplyChunk::Success)
                }
            }
            ERROR => {
                if raw.len() != 1 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    Ok(ReplyChunk::Error)
                }
            }
            _ => {
                Err(ProtocolError::new("incorrect reply chunk identifier"))
            }
        }
    }
}

#[cfg(test)]
mod test_request {
    use crate::kvserver::protocol::Request;
    use crate::util::{gen_key, gen_value};

    #[test]
    fn request_serialize_scan() {
        for _ in 1..10 {
            let key1 = gen_key();
            let key2 = gen_key();
            let req = Request::Scan(key1, key2);
            let req1 = Request::deserialize_from(req.serialize()).unwrap();
            match req1 {
                Request::Scan(k1, k2) => {
                    assert_eq!(k1, key1);
                    assert_eq!(k2, key2);
                },
                _ => panic!()
            }
        }
    }

    #[test]
    fn request_serialize_put() {
        for _ in 1..10 {
            let key = gen_key();
            let value = gen_value();
            let req = Request::Put(key, value);
            let req1 = Request::deserialize_from(req.serialize()).unwrap();
            match req1 {
                Request::Put(k, v) => {
                    assert_eq!(k, key);
                    assert_eq!(v, value);
                },
                _ => panic!()
            }
        }
    }

    #[test]
    fn request_serialize_get() {
        for _ in 1..10 {
            let key = gen_key();
            let req = Request::Get(key);
            let req1 = Request::deserialize_from(req.serialize()).unwrap();
            match req1 {
                Request::Get(k) => {
                    assert_eq!(k, key);
                },
                _ => panic!()
            }
        }
    }

    #[test]
    fn request_serialize_delete() {
        for _ in 1..10 {
            let key = gen_key();
            let req = Request::Del(key);
            let req1 = Request::deserialize_from(req.serialize()).unwrap();
            match req1 {
                Request::Del(k) => {
                    assert_eq!(k, key);
                },
                _ => panic!()
            }
        }
    }

    #[test]
    fn request_serialize_close() {
        for _ in 1..10 {
            let req = Request::Close;
            let req1 = Request::deserialize_from(req.serialize()).unwrap();
            match req1 {
                Request::Close => (),
                _ => panic!()
            }
        }
    }
}

#[cfg(test)]
mod test_reply_chunk {
    use crate::kvserver::protocol::{ReplyChunk, ServerReplyChunk};
    use crate::util::{gen_key, gen_value};
    use std::sync::Arc;
    use std::ops::Deref;

    #[test]
    fn reply_serialize_single_value() {
        for _ in 0..10 {
            let value = Arc::new(gen_value());
            let chunk =
                ReplyChunk::deserialize(ServerReplyChunk::SingleValue(Some(value.clone())).serialize()).unwrap();
            match chunk {
                ReplyChunk::SingleValue(v) => {
                    assert_eq!(v.unwrap(), *value)
                },
                _ => panic!()
            }
        }
    }

    #[test]
    fn reply_serialize_number() {
        for _ in 0..10 {
            let num = rand::random();
            let chunk =
                ReplyChunk::deserialize(ServerReplyChunk::Number(num).serialize()).unwrap();
            match chunk {
                ReplyChunk::Number(n) => {
                    assert_eq!(n, num);
                },
                _ => panic!()
            }
        }
    }

    #[test]
    fn reply_serialize_kv_pairs() {
        for _ in 0..10 {
            let mut pairs = Vec::new();
            for _ in 0..16 {
                let key = gen_key();
                let value = Arc::new(gen_value());
                pairs.push((key, value.clone()));
            }
            let chunk =
                ReplyChunk::deserialize(ServerReplyChunk::KVPairs(&pairs).serialize()).unwrap();
            match chunk {
                ReplyChunk::KVPairs(ps) => {
                    assert_eq!(ps.len(), pairs.len());
                    for ((k1, v1), (k2, v2)) in ps.iter().zip(pairs.iter()) {
                        assert_eq!(k1, k2);
                        assert_eq!(v1, v2.deref());
                    }
                },
                _ => panic!()
            }
        }
    }
}

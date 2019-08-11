pub const SCAN: u8 = b'S';
pub const PUT: u8 = b'P';
pub const GET: u8 = b'G';
pub const DEL: u8 = b'D';
pub const CLOSE: u8 = b'C';

pub use crate::kvstorage::Key;
pub use crate::kvstorage::Value;

use std::sync::Arc;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::error::Error;

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

pub enum Request {
    Scan(Key, Key),
    Put(Key, Value),
    Get(Key),
    Del(Key),
    Close
}

impl Request {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Request::Scan(key1, key2) => {
                let mut ret = vec![SCAN];
                ret.append(&mut key1.to_vec());
                ret.append(&mut key2.to_vec());
                ret
            },
            Request::Put(key, value) => {
                let mut ret = vec![PUT];
                ret.append(&mut key.to_vec());
                ret.append(&mut value.to_vec());
                ret
            },
            Request::Get(key) => {
                let mut ret = vec![GET];
                ret.append(&mut key.to_vec());
                ret
            },
            Request::Del(key) => {
                let mut ret = vec![DEL];
                ret.append(&mut key.to_vec());
                ret
            },
            Request::Close => {
                vec![CLOSE]
            }
        }
    }

    pub fn deserialize_from(raw: Vec<u8>) -> Result<Self, ProtocolError> {
        assert!(raw.len() > 0);
        match raw[0] {
            SCAN => {
                if raw.len() != 17 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut key1 = [0; 8];
                    let mut key2 = [0; 8];
                    key1.copy_from_slice(&raw[1..9]);
                    key2.copy_from_slice(&raw[9..17]);
                    Ok(Request::Scan(key1, key2))
                }
            },
            PUT => {
                if raw.len() != 265 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut key = [0; 8];
                    let mut value = [0; 256];
                    key.copy_from_slice(&raw[1..9]);
                    value.copy_from_slice(&raw[9..265]);
                    Ok(Request::Put(key, value))
                }
            },
            GET => {
                if raw.len() != 9 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut key = [0; 8];
                    key.copy_from_slice(&raw[1..9]);
                    Ok(Request::Get(key))
                }
            },
            DEL => {
                if raw.len() != 9 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut key = [0; 8];
                    key.copy_from_slice(&raw[1..9]);
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

pub const SINGLE_VALUE: u8 = b'S';
pub const NUMBER: u8 = b'N';
pub const KV_PAIRS: u8 = b'P';

pub enum ServerReplyChunk {
    SingleValue(Arc<Value>),
    Number(usize),
    KVPairs(Vec<(Key, Arc<Value>)>)
}

impl ServerReplyChunk {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            ServerReplyChunk::SingleValue(value) => {
                let mut ret = vec![SINGLE_VALUE];
                ret.append(&mut value.to_vec());
                ret
            },
            ServerReplyChunk::Number(number) => {
                let mut ret = vec![NUMBER];
                let mut number = *number;
                let mut arr = [0u8; 8];
                for i in (0..8_usize).rev() {
                    arr[i] = (number % 256) as u8;
                    number /= 256;
                }
                ret.append(&mut arr.to_vec());
                ret
            },
            ServerReplyChunk::KVPairs(pairs) => {
                let mut ret = vec![KV_PAIRS];
                for (key, value) in pairs {
                    ret.append(&mut key.to_vec());
                    ret.append(&mut value.to_vec());
                }
                ret
            }
        }
    }
}

pub enum ReplyChunk {
    SingleValue(Value),
    Number(usize),
    KVPairs(Vec<(Key, Value)>)
}

impl ReplyChunk {
    pub fn deserialize(raw: Vec<u8>) -> Result<Self, ProtocolError> {
        assert!(raw.len() > 0);
        match raw[0] {
            SINGLE_VALUE => {
                if raw.len() != 257 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut ret = [0u8; 256];
                    ret.copy_from_slice(&raw[1..257]);
                    Ok(ReplyChunk::SingleValue(ret))
                }
            },
            NUMBER => {
                if raw.len() != 9 {
                    Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut ret = 0;
                    for &byte in raw[1..9].iter() {
                        ret *= 256;
                        ret += byte as usize;
                    }
                    Ok(ReplyChunk::Number(ret))
                }
            },
            KV_PAIRS => {
                if (raw.len() - 1) % 264 != 0 {
                    return Err(ProtocolError::new("incorrect content length"))
                } else {
                    let mut ret = Vec::new();
                    for i in (1..raw.len()).step_by(264) {
                        let mut key = [0u8; 8];
                        let mut value = [0u8; 256];
                        key.copy_from_slice(&raw[i..i + 8]);
                        value.copy_from_slice(&raw[i+8..i + 264]);
                        ret.push((key, value))
                    }
                    Ok(ReplyChunk::KVPairs(ret))
                }
            },
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
                    assert_eq!(k1.to_vec(), key1.to_vec());
                    assert_eq!(k2.to_vec(), key2.to_vec());
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
                    assert_eq!(k.to_vec(), key.to_vec());
                    assert_eq!(v.to_vec(), value.to_vec());
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
                    assert_eq!(k.to_vec(), key.to_vec());
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
                    assert_eq!(k.to_vec(), key.to_vec());
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

    #[test]
    fn reply_serialize_single_value() {
        for _ in 0..10 {
            let value = Arc::new(gen_value());
            let chunk =
                ReplyChunk::deserialize(ServerReplyChunk::SingleValue(value.clone()).serialize()).unwrap();
            match chunk {
                ReplyChunk::SingleValue(v) => {
                    assert_eq!(v.to_vec(), value.to_vec())
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
                ReplyChunk::deserialize(ServerReplyChunk::KVPairs(pairs.to_owned()).serialize()).unwrap();
            match chunk {
                ReplyChunk::KVPairs(ps) => {
                    assert_eq!(ps.len(), pairs.len());
                    for ((k1, v1), (k2, v2)) in ps.iter().zip(pairs.iter()) {
                        assert_eq!(k1.to_vec(), k2.to_vec());
                        assert_eq!(v1.to_vec(), v2.to_vec());
                    }
                },
                _ => panic!()
            }
        }
    }
}

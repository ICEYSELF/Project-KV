pub const SCAN: u8 = b'S';
pub const PUT: u8 = b'P';
pub const GET: u8 = b'G';
pub const DEL: u8 = b'D';
pub const CLOSE: u8 = b'C';

pub use crate::kvstorage::Key;
pub use crate::kvstorage::Value;

use std::sync::Arc;

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

    pub fn deserialize_from(raw: Vec<u8>) -> Option<Self> {
        assert!(raw.len() > 0);
        match raw[0] {
            SCAN => {
                if raw.len() != 17 {
                    None
                } else {
                    let mut key1 = [0; 8];
                    let mut key2 = [0; 8];
                    key1.copy_from_slice(&raw[1..9]);
                    key2.copy_from_slice(&raw[9..17]);
                    Some(Request::Scan(key1, key2))
                }
            },
            PUT => {
                if raw.len() != 265 {
                    None
                } else {
                    let mut key = [0; 8];
                    let mut value = [0; 256];
                    key.copy_from_slice(&raw[1..9]);
                    value.copy_from_slice(&raw[9..265]);
                    Some(Request::Put(key, value))
                }
            },
            GET => {
                if raw.len() != 9 {
                    None
                } else {
                    let mut key = [0; 8];
                    key.copy_from_slice(&raw[1..9]);
                    Some(Request::Get(key))
                }
            },
            DEL => {
                if raw.len() != 9 {
                    None
                } else {
                    let mut key = [0; 8];
                    key.copy_from_slice(&raw[1..9]);
                    Some(Request::Del(key))
                }
            },
            CLOSE=> {
                Some(Request::Close)
            }
            _ => {
                None
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

#[allow(unused_variables)]
impl ServerReplyChunk {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            ServerReplyChunk::SingleValue(value) => {
                unimplemented!()
            },
            ServerReplyChunk::Number(number) => {
                unimplemented!()
            },
            ServerReplyChunk::KVPairs(paris) => {
                unimplemented!()
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
    #[allow(unused_variables)]
    pub fn deserialize(raw: Vec<u8>) -> Self {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use crate::kvserver::protocol::Request;

    fn gen_key() -> [u8; 8] {
        let mut ret = [0u8; 8];
        for v in ret.iter_mut() {
            *v = rand::random();
        }
        ret
    }

    fn gen_value() -> [u8; 256] {
        let mut ret = [0u8; 256];
        for v in ret.iter_mut() {
            *v = rand::random();
        }
        ret
    }

    #[test]
    fn request_serialize_scan() {
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

    #[test]
    fn request_serialize_put() {
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

    #[test]
    fn request_serialize_get() {
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

    #[test]
    fn request_serialize_delete() {
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

    #[test]
    fn request_serialize_close() {
        let req = Request::Close;
        let req1 = Request::deserialize_from(req.serialize()).unwrap();
        match req1 {
            Request::Close => (),
            _ => panic!()
        }
    }
}

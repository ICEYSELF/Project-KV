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
        #[allow(unused_variables)]
        match self {
            Request::Scan(key1, key2) => {
                unimplemented!()
            },
            Request::Put(key, value) => {
                unimplemented!()
            },
            Request::Get(key) => {
                unimplemented!()
            },
            Request::Del(key) => {
                unimplemented!()
            },
            Request::Close => {
                unimplemented!()
            }
        }
    }

    pub fn deserialize_from(raw: Vec<u8>) -> Self {
        unimplemented!()
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
    pub fn deserialize(raw: Vec<u8>) -> Self {
        unimplemented!()
    }
}

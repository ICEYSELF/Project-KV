pub const SCAN: u8 = b'S';
pub const PUT: u8 = b'P';
pub const GET: u8 = b'G';
pub const DEL: u8 = b'D';

pub use crate::kvstorage::Key;
pub use crate::kvstorage::Value;

pub fn do_get(key: &Key) -> Value {
    unimplemented!()
}

pub fn do_put(key: &Key, value: &Value) {
    unimplemented!()
}

pub fn do_del(key: &Key) {
    unimplemented!()
}

pub fn do_scan(key1: &Key, key2: &Key) -> Vec<Value> {
    unimplemented!()
}

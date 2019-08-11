use rand;
use crate::kvstorage::{Key, Value, KEY_SIZE, VALUE_SIZE};

pub fn gen_key() -> Key {
    let mut ret = [0u8; KEY_SIZE];
    for v in ret.iter_mut() {
        *v = rand::random();
    }
    ret
}

pub fn gen_key_n(n: u8) -> Key {
    let mut ret = [0u8; KEY_SIZE];
    ret[KEY_SIZE - 1] = n;
    ret
}

pub fn gen_value() -> Value {
    let mut ret = [0u8; VALUE_SIZE];
    for v in ret.iter_mut() {
        *v = rand::random();
    }
    ret
}

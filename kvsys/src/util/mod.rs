//! Utility functions for test case generation
//!
//! Please note that all functions in this module should only be used for tests. They are not
//! guaranteed to be random, neither guaranteed to meet any certain distribution. Using these
//! functions outside of test cases may increase the risk of being cracked.
use rand;
use crate::kvstorage::{Key, Value, KEY_SIZE, VALUE_SIZE};

/// generates a random `Key`
pub fn gen_key() -> Key {
    let mut ret = [0u8; KEY_SIZE];
    for v in ret.iter_mut() {
        *v = rand::random();
    }
    Key::from_slice(&ret)
}

/// generates a `Key` according to number `n`
///
/// ```no_run
///     use kvsys::util::gen_key_n;
///     use kvsys::kvstorage::Key;
///     let key = gen_key_n(0x40490fd0cafebabe);
///     assert_eq!(key, Key::from_slice(&[0x40, 0x49, 0x0f, 0xd0, 0xca, 0xfe, 0xba, 0xbe]));
/// ```
pub fn gen_key_n(n: u64) -> Key {
    Key::decode(n)
}

/// generates a random `Value`
pub fn gen_value() -> Value {
    let mut ret = [0u8; VALUE_SIZE];
    for v in ret.iter_mut() {
        *v = rand::random();
    }
    Value::from_slice(&ret)
}

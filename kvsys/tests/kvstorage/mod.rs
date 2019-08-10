#![allow(dead_code)]
#[cfg(test)]
mod test {
    use kvstorage::KVStorage;
    use std::fs;

    fn gen_key() -> [u8; 8] {
        [0; 8]
    }

    fn gen_value() -> [u8; 256] {
        [0; 256]
    }

    #[test]
    fn test_basic_rw() {
        let f: fs::File = tempfile::tempfile().unwrap();
        let _kv = KVStorage::new(f);
    }
}

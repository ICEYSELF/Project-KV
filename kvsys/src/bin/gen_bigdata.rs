use kvsys::kvstorage::KVStorage;
use std::fs;
use kvsys::util::{gen_key_n, gen_value};
use std::sync::{Arc, RwLock};

fn main() {
    let _ = fs::remove_file("bigdata.kv");
    let log_file = fs::File::create("bigdata.kv").unwrap();
    let storage_engine = Arc::new(RwLock::new(KVStorage::new(log_file)));
    for i in 0..524288 {
        let key = gen_key_n(i);
        let value = gen_value();
        storage_engine.write().unwrap().put(&key, &value).unwrap();
    }
}

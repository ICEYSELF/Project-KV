#[cfg(test)]
mod test {
    use kvsys::kvstorage::KVStorage;
    use std::{fs, thread};
    use kvsys::util::{gen_key, gen_key_n, gen_value};
    use std::ops::Deref;
    use std::sync::{Arc, RwLock};

    fn from_existing_file(path: &str) -> KVStorage {
        let content;
        {
            let f = fs::File::open(path).unwrap();
            content = KVStorage::read_log_file(f).unwrap();
        }

        {
            let f = fs::OpenOptions::new().write(true).append(true).open(path).unwrap();
            KVStorage::with_content(content, f)
        }
    }

    #[test]
    fn test_basic_rw() {
        let f = tempfile::tempfile().unwrap();
        let mut kv = KVStorage::new(f);

        let (key, value) = (gen_key(), gen_value());
        kv.put(&key, &value).unwrap();

        assert_eq!(kv.get(&key).unwrap().deref(), &value);
    }

    #[test]
    fn test_persist_storage() {
        let _ = fs::remove_file("test1.kv");
        let (key, value) = (gen_key(), gen_value());

        {
            let f = fs::File::create("test1.kv").unwrap();
            let mut kv = KVStorage::new(f);
            kv.put(&key, &value).unwrap();
        }

        {
            let kv = from_existing_file("test1.kv");
            assert_eq!(kv.get(&key).unwrap().deref(), &value);
        }
    }

    #[test]
    fn test_rw_some() {
        let _ = fs::remove_file("test2.kv");
        let f = fs::File::create("test2.kv").unwrap();
        let mut kv = KVStorage::new(f);

        let mut keys = Vec::new();
        let mut values = Vec::new();
        for i in 0..255 {
            let (key, value) = (gen_key_n(i), gen_value());
            keys.push(key);
            values.push(value);
            kv.put(&key, &value).unwrap();
        }

        for i in 0..255 {
            let key = keys[i];
            let value = values[i];
            assert_eq!(kv.get(&key).unwrap().deref(), &value);
        }
    }

    #[test]
    fn test_persist_some() {
        let _ = fs::remove_file("test3.kv");

        let mut keys = Vec::new();
        let mut values = Vec::new();
        {
            let f = fs::File::create("test3.kv").unwrap();
            let mut kv = KVStorage::new(f);
            for i in 0..255 {
                let (key, value) = (gen_key_n(i), gen_value());
                keys.push(key);
                values.push(value);
                kv.put(&key, &value).unwrap();
            }
        }

        {
            let kv = from_existing_file("test3.kv");
            for i in 0..255 {
                let key = keys[i];
                let value = values[i];
                assert_eq!(kv.get(&key).unwrap().deref(), &value);
            }
        }
    }

    #[test]
    fn test_persist_with_delete() {
        let _ = fs::remove_file("test4.kv");

        let mut keys = Vec::new();
        let mut keys_to_delete = Vec::new();
        let mut values = Vec::new();
        {
            let f = fs::File::create("test4.kv").unwrap();
            let mut kv = KVStorage::new(f);
            for i in 0..255 {
                let (key, value) = (gen_key_n(i), gen_value());
                keys.push(key);
                if rand::random() {
                    values.push(Some(value));
                } else {
                    values.push(None);
                    keys_to_delete.push(key)
                }
                kv.put(&key, &value).unwrap();
            }

            for key in keys_to_delete.iter() {
                kv.delete(key).unwrap();
            }
        }

        {
            let kv = from_existing_file("test4.kv");
            for i in 0..255 {
                let key = keys[i];
                if let Some(value) = values[i] {
                    assert_eq!(kv.get(&key).unwrap().deref(), &value);
                } else {
                    assert!(kv.get(&key).is_none());
                }
            }
        }
    }

    #[test]
    fn test_multi_thread_rw() {
        let _ = fs::remove_file("test5.kv");
        let f = fs::File::create("test5.kv").unwrap();
        let mut kv = KVStorage::new(f);
        let mut values1 = Vec::new();
        let mut values2 = Vec::new();

        for _ in 0..65536 {
            values1.push(gen_value());
            values2.push(gen_value());
        }
        for i in 0..65536 {
            kv.put(&gen_key_n(i), &values1[i as usize]).unwrap();
        }

        let kv = Arc::new(RwLock::new(kv));
        let values1 = Arc::new(RwLock::new(values1));
        let values2 = Arc::new(RwLock::new(values2));

        let mut readers = Vec::new();
        for _ in 0..4 {
            let kv = kv.clone();
            let values1 = values1.clone();
            let values2 = values2.clone();
            let reader = thread::spawn(move || {
                for i in 0..65536 {
                    let value = kv.read().unwrap().get(&gen_key_n(i)).unwrap();
                    assert!(value.deref() == values1.read().unwrap().get(i as usize).unwrap()
                            || value.deref() == values2.read().unwrap().get(i as usize).unwrap());
                }
            });
            readers.push(reader);
        }
        let writer;
        {
            let kv = kv.clone();
            let values2 = values2.clone();
            writer = thread::spawn(move || {
                for i in 0..65536 {
                    kv.write().unwrap().put(&gen_key_n(i),
                                            values2.read().unwrap().get(i as usize).unwrap()).unwrap();
                }
            });
        }

        for reader in readers {
            let _ = reader.join();
        }
        let _ = writer.join();

        for i in 0..65536 {
            let value = kv.read().unwrap().get(&gen_key_n(i)).unwrap();
            assert!(value.deref() == values2.read().unwrap().get(i as usize).unwrap());
        }
    }
}

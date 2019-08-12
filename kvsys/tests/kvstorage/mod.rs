#[cfg(test)]
mod test {
    use kvsys::kvstorage::KVStorage;
    use std::fs;
    use kvsys::util::{gen_key, gen_key_n, gen_value};
    use std::ops::Deref;

    #[test]
    fn test_basic_rw() {
        let f = tempfile::tempfile().unwrap();
        let mut kv = KVStorage::new(f);

        let (key, value) = (gen_key(), gen_value());
        kv.put(&key, &value);

        assert_eq!(kv.get(&key).unwrap().deref(), &value);
    }

    #[test]
    fn test_persist_storage() {
        let _ = fs::remove_file("test1.kv");
        let (key, value) = (gen_key(), gen_value());

        {
            let f = fs::File::create("test1.kv").unwrap();
            let mut kv = KVStorage::new(f);
            kv.put(&key, &value);
        }

        {
            let f = fs::File::open("test1.kv").unwrap();
            let kv = KVStorage::from_existing_file(f).unwrap();
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
            kv.put(&key, &value);
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
                kv.put(&key, &value);
            }
        }

        {
            let f = fs::File::open("test3.kv").unwrap();
            let kv = KVStorage::from_existing_file(f).unwrap();
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
                kv.put(&key, &value);
            }

            for key in keys_to_delete.iter() {
                kv.delete(key);
            }
        }

        {
            let f = fs::File::open("test4.kv").unwrap();
            let kv = KVStorage::from_existing_file(f).unwrap();
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
}

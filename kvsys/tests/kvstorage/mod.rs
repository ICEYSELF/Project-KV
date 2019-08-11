#[cfg(test)]
mod test {
    use kvsys::kvstorage::KVStorage;
    use std::fs;

    // TODO this piece of utility function duplicates, remove duplicates whenever possible.
    fn gen_key() -> [u8; 8] {
        let mut ret = [0u8; 8];
        for v in ret.iter_mut() {
            *v = rand::random();
        }
        ret
    }

    fn gen_key_n(n: u8) -> [u8; 8] {
        let mut ret = [0u8; 8];
        ret[7] = n;
        ret
    }

    fn gen_value() -> [u8; 256] {
        let mut ret = [0u8; 256];
        for v in ret.iter_mut() {
            *v = rand::random();
        }
        ret
    }

    fn check_value_eq(lhs: &[u8], rhs: &[u8]) -> bool {
        lhs.len() == rhs.len() &&
        lhs.iter()
           .zip(rhs.iter())
           .fold(true, |x, (x1, x2)| { x && (*x1 == *x2) })
    }

    #[test]
    fn test_utilities() {
        assert!(!check_value_eq(b"code", b"cod"));
        assert!(!check_value_eq(b"code", b"coda"));
        assert!(check_value_eq(b"code", b"code"));
    }

    #[test]
    fn test_basic_rw() {
        let f = tempfile::tempfile().unwrap();
        let mut kv = KVStorage::new(f);

        let (key, value) = (gen_key(), gen_value());
        kv.put(&key, &value);

        let result = kv.get(&key);
        assert!(check_value_eq(&(result.unwrap())[..], &value));

        kv.shutdown();
    }

    #[test]
    fn test_persist_storage() {
        let _ = fs::remove_file("test1.kv");
        let (key, value) = (gen_key(), gen_value());

        {
            let f = fs::File::create("test1.kv").unwrap();
            let mut kv = KVStorage::new(f);
            kv.put(&key, &value);
            kv.shutdown();
        }

        {
            let f = fs::File::open("test1.kv").unwrap();
            let kv = match KVStorage::from_existing_file(f) {
                Ok(kv) => kv,
                Err(e) => {
                    eprintln!("{}", e.description());
                    panic!()
                }
            };
            eprintln!("{:?}", kv);
            assert!(check_value_eq(&(kv.get(&key).unwrap())[..], &value));
            kv.shutdown();
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
            assert!(check_value_eq(&(kv.get(&key).unwrap())[..], &value));
        }

        kv.shutdown();
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
            kv.shutdown();
        }

        {
            let f = fs::File::open("test3.kv").unwrap();
            let kv = KVStorage::from_existing_file(f).unwrap();
            for i in 0..255 {
                let key = keys[i];
                let value = values[i];
                assert!(check_value_eq(&(kv.get(&key).unwrap())[..], &value));
            }
            kv.shutdown();
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

            kv.shutdown();
        }

        {
            let f = fs::File::open("test4.kv").unwrap();
            let kv = KVStorage::from_existing_file(f).unwrap();
            for i in 0..255 {
                let key = keys[i];
                if let Some(value) = values[i] {
                    assert!(check_value_eq(&(kv.get(&key).unwrap())[..], &value));
                } else {
                    assert!(kv.get(&key).is_none());
                }
            }
            kv.shutdown();
        }
    }
}

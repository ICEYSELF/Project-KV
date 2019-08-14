#![feature(test)]
extern crate test;

#[cfg(test)]
mod bench {
    use kvsys::kvstorage::KVStorage;
    use std::{fs, thread};
    use kvsys::util::{gen_key_n, gen_value};
    use std::sync::{Arc, RwLock};
    use test::Bencher;

    #[bench]
    fn test_multi_thread_rw(b: &mut Bencher) {
        let _ = fs::remove_file("bench_mtrw.kv");
        let f = fs::File::create("bench_mtrw.kv").unwrap();
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
        let values2 = Arc::new(RwLock::new(values2));

        b.iter(move || {
            let mut readers = Vec::new();
            for _ in 0..4 {
                let kv = kv.clone();
                let reader = thread::spawn(move || {
                    for i in 0..65536 {
                        let _ = kv.read().unwrap().get(&gen_key_n(i)).unwrap();
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
        });
    }
}
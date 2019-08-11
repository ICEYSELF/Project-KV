use rand;

pub fn gen_key() -> [u8; 8] {
    let mut ret = [0u8; 8];
    for v in ret.iter_mut() {
        *v = rand::random();
    }
    ret
}

pub fn gen_key_n(n: u8) -> [u8; 8] {
    let mut ret = [0u8; 8];
    ret[7] = n;
    ret
}

pub fn gen_value() -> [u8; 256] {
    let mut ret = [0u8; 256];
    for v in ret.iter_mut() {
        *v = rand::random();
    }
    ret
}

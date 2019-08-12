use std::net::TcpStream;
use std::{io, process};
use std::io::Write;
use std::error::Error;
use std::fmt;

use kvsys::chunktps::ChunktpsConnection;
use kvsys::kvstorage::{Key, Value};
use kvsys::kvserver::protocol::{Request, ReplyChunk};

#[derive(Debug)]
struct ClientError {
    description: String
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "client error: {}", self.description)
    }
}

impl Error for ClientError {
}

impl ClientError {
    fn new(description: &str) -> Self {
        ClientError { description: description.to_owned() }
    }
}

fn main() {
    env_logger::init();

    println!("KV storage client -- v0.1");
    print!("server IP:PORT to connect: ");
    io::stdout().flush().unwrap();

    let mut ip_addr = String::new();
    io::stdin().read_line(&mut ip_addr).unwrap();
    match TcpStream::connect(ip_addr.trim()) {
        Ok(tcp_stream) => {
            if let Err(e) = mainloop(tcp_stream) {
                eprintln!("critical error occurred in client mainloop, client shutting down");
                eprintln!("detailed error info: {}", e);
            }
        }
        Err(e) => {
            eprintln!("critical error occurred while opening TCP connection, client shutting down");
            eprintln!("detailed error info: {}", e);
        }
    }
}

fn mainloop(tcp_stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut chunktps = ChunktpsConnection::new(tcp_stream);
    loop {
        print!("kv-client> ");
        io::stdout().flush().unwrap();

        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        match parse_input(command) {
            Ok(request) => {
                chunktps.write_chunk(request.serialize())?;
                handle_server_reply(&mut chunktps, request)?;
            },
            Err(e) => {
                eprintln!("{}", e)
            }
        }
    }
}

fn handle_server_reply(chunktps: &mut ChunktpsConnection, request: Request) -> Result<(), Box<dyn Error>> {
    match request {
        Request::Close => {
            process::exit(0)
        },
        Request::Del(_) => {
            let reply = ReplyChunk::deserialize(chunktps.read_chunk()?)?;
            match reply {
                ReplyChunk::Number(number ) => {
                    println!("Ok, {} rows affected.", number);
                    Ok(())
                },
                ReplyChunk::Error => {
                    eprintln!("deletion failed, server error");
                    Ok(())
                }
                _ => Err(Box::new(ClientError::new("unexpected reply chunk kind")))
            }
        },
        Request::Scan(_, _) => {
            loop {
                let chunk = chunktps.read_chunk()?;
                if chunk.len() == 0 {
                    return Ok(())
                }
                let reply = ReplyChunk::deserialize(chunk)?;
                match reply {
                    ReplyChunk::KVPairs(kv_pairs) => {
                        for (key, value) in kv_pairs {
                            println!("{:?} => {:?}", key, value)
                        }
                    },
                    _ => return Err(Box::new(ClientError::new("unexpected reply chunk kind")))
                }
            }
        },
        Request::Get(key) => {
            let reply = ReplyChunk::deserialize(chunktps.read_chunk()?)?;
            match reply {
                ReplyChunk::SingleValue(value ) => {
                    if let Some(value) = value {
                        println!("{:?} => {:?}", key, value);
                    } else {
                        println!("{:?} => nil", key);
                    }
                    Ok(())
                }
                _ => Err(Box::new(ClientError::new("unexpected reply chunk kind")))
            }
        },
        Request::Put(_, _) => {
            let reply = ReplyChunk::deserialize(chunktps.read_chunk()?)?;
            match reply {
                ReplyChunk::Success => {
                    println!("data successfully inserted.");
                    Ok(())
                },
                ReplyChunk::Error => {
                    eprintln!("insertion failed, server error");
                    Ok(())
                }
                _ => Err(Box::new(ClientError::new("unexpected reply chunk kind")))
            }
        }
    }

}

fn parse_input(command: String) -> Result<Request, ClientError> {
    let check_key_size = | slice: &[u8] | {
        Key::from_slice_checked(slice).ok_or(ClientError::new("incorrect key size"))
    };

    let check_value_size = | slice: &[u8] | {
        if slice.len() < 256 {
            let mut ret = [0; 256];
            for i in 0..slice.len() {
                ret[i] = slice[i];
            }
            Ok(Value::from_slice(&ret))
        } else {
            Value::from_slice_checked(slice).ok_or(ClientError::new("incorrect value size"))
        }
    };

    let parts = command.trim().split_whitespace().collect::<Vec<_>>();
    if parts.len() == 0 {
        return Err(ClientError::new("no command given!"))
    }
    match parts[0] {
        "get" => {
            if parts.len() != 2 {
                return Err(ClientError::new("`get` requires exactly 1 argument"))
            }
            let key = check_key_size(&parts[1].as_bytes())?;
            Ok(Request::Get(key))
        },
        "put" => {
            if parts.len() != 3 {
                return Err(ClientError::new("put requires exactly 2 arguments"))
            }

            let key = check_key_size(parts[1].as_bytes())?;
            let value = check_value_size(parts[2].as_bytes())?;
            Ok(Request::Put(key, value))
        },
        "scan" => {
            if parts.len() != 3 {
                return Err(ClientError::new("scan requires exactly 2 arguments"))
            }

            let key1 = check_key_size(parts[1].as_bytes())?;
            let key2 = check_key_size(parts[2].as_bytes())?;
            Ok(Request::Scan(key1, key2))
        },
        "del" | "delete" => {
            if parts.len() != 2 {
                return Err(ClientError::new("delete requires exactly 1 argument"))
            }

            let key = check_key_size(parts[1].as_bytes())?;
            Ok(Request::Del(key))
        },
        "close" => {
            if parts.len() != 1 {
                eprintln!("close requires no argument");
            }
            Ok(Request::Close)
        }
        _ => {
            Err(ClientError::new("unknown command"))
        }
    }
}

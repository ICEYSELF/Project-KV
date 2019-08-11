use std::net::TcpStream;
use std::io;
use std::io::Write;
use std::error::Error;

#[allow(unused_variables)]
fn mainloop(tcp_stream: TcpStream) -> Result<(), Box<dyn Error>> {
    loop {
        print!("kv-client> ");
        io::stdout().flush().unwrap();

        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        let parts = command.split_whitespace().collect::<Vec<_>>();
        if parts.len() == 0 {
            eprintln!("please, input at least one command");
        }
        match parts[0] {
            "get" => {
                if parts.len() != 2 {
                    eprintln!("get requires exactly one argument");
                    continue;
                }

                let bytes = parts[1].as_bytes();
                if bytes.len() != 8 {
                    eprintln!("size of key should be exactly 8 bytes");
                    continue;
                }
            },
            "put" => {
                if parts.len() != 3 {
                    eprintln!("put requires exactly two arguments")
                }

                let key_bytes = parts[1].as_bytes();
                let value_bytes = parts[2].as_bytes();
            },
            _ => {

            }
        }

    }
}

fn main() {
    env_logger::init();

    print!("server IP:PORT to connect: ");
    io::stdout().flush().unwrap();

    let mut ip_addr = String::new();
    io::stdin().read_line(&mut ip_addr).unwrap();
    match TcpStream::connect(ip_addr) {
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
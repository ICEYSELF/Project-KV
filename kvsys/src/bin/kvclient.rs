use std::net::TcpStream;
use std::io;

use env_logger;
use log::{info, warn, error};
use std::error::Error;
use std::io::Write;

fn mainloop(mut tcp_stream: TcpStream) -> Result<(), Box<dyn Error>> {
    loop {
        print!("kv-client> ");
        io::stdout().flush().unwrap();

        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        let parts = command.split_whitespace().collect::<Vec<_>>();
        if parts.len() == 0 {
            eprintln!("please, input at least one command");
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
        Ok(mut tcp_stream) => {
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
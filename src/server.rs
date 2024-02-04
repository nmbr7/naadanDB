use std::str::from_utf8;

use log::{debug, info, trace};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};
use String;

// TODO: setup a TCP server to accept DB connection and handle queries

pub struct ServerConfig {
    pub port: u16,
}

pub struct NaadanServer {
    pub config: ServerConfig,
}

impl NaadanServer {
    pub fn setup(server_config: ServerConfig) -> Self {
        NaadanServer {
            config: server_config,
        }
    }

    pub async fn start(self: Self) {
        trace!("Starting naadanDB server");

        let ip_port = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(ip_port).await.unwrap();

        loop {
            let (socket, _) = listener.accept().await.unwrap();

            tokio::spawn(async move {
                Self::process_request(socket).await;
            });
        }
    }

    async fn process_request(mut socket: TcpStream) {
        info!("Got new request: {:?}", socket);

        let buffer: &mut Vec<u8> = &mut Vec::new();

        socket.read_to_end(buffer).await.unwrap();
        debug!("Read data: {}", from_utf8(buffer).unwrap());
    }

    pub fn stop(self: Self) {
        trace!("Stopping naadanDB server")
    }
}

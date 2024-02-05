use std::{
    io::{BufRead, BufReader},
    str::from_utf8,
};

use log::{debug, info, trace};
use rand::Rng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use std::sync::Arc;

use crate::{
    query_engine::{self, NaadanQueryEngine},
    server,
    storage_engine::{self, OurStorageEngine, RowData, StorageEngine},
};

// TODO: setup a TCP server to accept DB connection and handle queries

#[derive(Debug)]
pub struct ServerConfig {
    pub port: u16,
}

#[derive(Debug)]
pub struct NaadanServer {
    pub config: ServerConfig,
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>,
}

impl NaadanServer {
    pub fn setup(server_config: ServerConfig) -> Self {
        NaadanServer {
            config: server_config,
            storage_engine: Arc::new(Mutex::new(Box::new(OurStorageEngine::init(1024)))),
        }
    }

    pub async fn start(self: Self) {
        trace!("Starting naadanDB server");

        let ip_port = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(ip_port).await.unwrap();

        let server_instance = Arc::new(Mutex::new(self));

        let mut rng = rand::thread_rng();

        let n = rng.gen_range(0..100);

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let n = rng.gen_range(0..100);
            let server_instance_clone = server_instance.clone();
            tokio::spawn(async move {
                Self::process_request(server_instance_clone, socket, n).await;
            });
        }
    }

    async fn process_request(
        server_instance: Arc<Mutex<NaadanServer>>,
        mut socket: TcpStream,
        n: usize,
    ) -> bool {
        info!("Got new request: {:?}", socket);
        let buffer: &mut Vec<u8> = &mut Vec::new();
        let mut count = 0;
        loop {
            let bytes = socket.read_buf(buffer).await.unwrap();
            if 0 == bytes {
                if buffer.is_empty() {
                    return true;
                } else {
                    return false;
                }
            }

            count += bytes;

            if (buffer.as_slice()[(count - bytes)..count] == b"EOF\n".to_vec()) {
                break;
            }
        }

        debug!("Read data: {}", from_utf8(&buffer[..(count - 4)]).unwrap());

        let sql_statement = from_utf8(&buffer[..(count - 4)]).unwrap();
        let sql_query = query_engine::NaadanQuery::init(sql_statement.to_string());

        let server_inst = server_instance.lock().await;
        let storage_engine_instance = server_inst.storage_engine.clone();
        let query_engine = NaadanQueryEngine::init(storage_engine_instance);

        let cal = query_engine.plan(&sql_query);

        let res = query_engine.execute();

        // let row_data = RowData {
        //     null_map: vec![1, 2],
        //     row_data: vec![1, 2],
        // };

        // println!("Random Number {}", n);
        // let mut row_id: usize = n;

        // s_engine.write_row(&row_id, &row_data);

        // println!("\n Reading 1 \n");
        // s_engine.read_row(row_id);

        let result = [0 as u8];
        socket.write(&result).await;

        true
    }

    pub fn stop(self: Self) {
        trace!("Stopping naadanDB server")
    }
}

use std::{result, str::from_utf8};

use log::{debug, error, info, trace};
use rand::Rng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use std::sync::Arc;

use crate::{
    query::{query_engine::NaadanQueryEngine, NaadanQuery},
    storage::{storage_engine::NaadanStorageEngine, NaadanError, StorageEngine},
    transaction::TransactionManager,
};

#[derive(Debug)]
pub struct ServerConfig {
    pub port: u16,
}

#[derive(Debug)]
pub struct NaadanServer {
    pub config: ServerConfig,
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine>>>,
    pub transaction_manager: Arc<Mutex<TransactionManager>>,
}

#[derive(Debug)]
pub struct SessionContext {
    pub current_transaction_id: u64,
}

impl SessionContext {
    pub fn new() -> Self {
        Self {
            current_transaction_id: 0,
        }
    }

    pub fn set_current_transaction_id(&mut self, current_transaction_id: u64) {
        self.current_transaction_id = current_transaction_id;
    }
}

impl NaadanServer {
    pub fn setup(server_config: ServerConfig) -> Self {
        NaadanServer {
            config: server_config,
            storage_engine: Arc::new(Mutex::new(Box::new(NaadanStorageEngine::init(1024)))),
            transaction_manager: Arc::new(Mutex::new(TransactionManager::init())),
        }
    }

    pub async fn start(self: Self) {
        trace!("Starting naadanDB server");

        let ip_port = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(ip_port).await.unwrap();

        let server_instance = Arc::new(self);

        let mut rng = rand::thread_rng();

        // let n = rng.gen_range(0..100);

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let n = rng.gen_range(0..100);

            let server_instance_clone = server_instance.clone();

            tokio::spawn(async move {
                match Self::process_request(server_instance_clone, socket, n).await {
                    Ok(_) => println!("Request processing done"),
                    Err(_) => error!("Request processing failed"),
                }
            });
        }
    }

    async fn process_request(
        server_instance: Arc<NaadanServer>,
        mut socket: TcpStream,
        _n: usize,
    ) -> Result<(), NaadanError> {
        info!("Got new request from IP: {:?}", socket.peer_addr());

        let buffer: &mut Vec<u8> = &mut Vec::new();
        let mut result: Vec<u8> = vec![];
        let sql_statement = match get_query_from_request(&mut socket, buffer).await {
            Ok(value) => value,
            Err(err) => {
                socket.write(err.to_string().as_bytes()).await.unwrap();
                return Ok(());
            }
        };

        {
            // Init and parse the query string to AST
            let sql_query = match NaadanQuery::init(sql_statement.to_string()) {
                Ok(res) => res,
                Err(err) => {
                    socket.write(err.to_string().as_bytes()).await.unwrap();
                    return Ok(());
                }
            };

            // Init a new query engine instance with reference to the global shared storage engine
            let query_engine = NaadanQueryEngine::init(
                server_instance.storage_engine.clone(),
                server_instance.transaction_manager.clone(),
            )
            .await;

            let mut session_context = SessionContext::new();

            // Process the sql query.
            let query_results = query_engine
                .process_query(&mut session_context, sql_query)
                .await;

            for query_result in query_results {
                match query_result {
                    Ok(val) => result.append(&mut val.to_string().as_bytes().to_vec()),
                    Err(err) => result.append(
                        &mut format!("Query execution failed: {}", err)
                            .as_bytes()
                            .to_vec(),
                    ),
                }
            }
        }

        socket.write(&result).await.unwrap();

        Ok(())
    }

    pub fn stop(self: Self) {
        trace!("Stopping naadanDB server")
    }
}

async fn get_query_from_request<'a>(
    socket: &'a mut TcpStream,
    buffer: &'a mut Vec<u8>,
) -> Result<&'a str, NaadanError> {
    let mut count = 0;
    loop {
        let bytes = socket.read_buf(buffer).await.unwrap();
        if 0 == bytes {
            println!("Read zero bytes from server");
            return Err(NaadanError::Unknown);
        }

        count += bytes;

        //println!("Bytes: {:?}", buffer);
        if buffer.as_slice()[(count - 4)..count] == b"EOF\n".to_vec() {
            if buffer.len() <= 5 {
                println!("Empty query received");
                return Err(NaadanError::Unknown);
            }
            break;
        }
    }

    Ok(from_utf8(&buffer[..(count - 4)]).unwrap())
}

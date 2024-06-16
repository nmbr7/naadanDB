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
    storage::{
        storage_engine::{self, NaadanStorageEngine},
        NaadanError, StorageEngine,
    },
    transaction::TransactionManager,
    utils,
};

#[derive(Debug)]
pub struct ServerConfig {
    pub port: u16,
}

#[derive(Debug)]
pub struct NaadanServer<E: StorageEngine + 'static> {
    pub config: ServerConfig,
    pub transaction_manager: Arc<Box<TransactionManager<E>>>,
}

#[derive(Debug)]
pub struct SessionContext {
    transaction_id: u64,
    transaction_type: TransactionType,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransactionType {
    Implicit,
    Explicit,
}

impl SessionContext {
    pub fn new() -> Self {
        Self {
            transaction_type: TransactionType::Implicit,
            transaction_id: 0,
        }
    }

    pub fn set_transaction_id(&mut self, transaction_id: u64) {
        self.transaction_id = transaction_id;
    }

    pub fn set_transaction_type(&mut self, transaction_type: TransactionType) {
        self.transaction_type = transaction_type;
    }

    pub fn transaction_type(&self) -> &TransactionType {
        &self.transaction_type
    }

    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
    }
}

impl<E: StorageEngine + Send + Sync> NaadanServer<E> {
    pub fn setup(server_config: ServerConfig, storage_engine: Arc<Box<E>>) -> Self {
        let transaction_manager = Arc::new(Box::new(TransactionManager::init(storage_engine)));

        NaadanServer {
            config: server_config,
            transaction_manager: transaction_manager,
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
                    Ok(_) => {
                        utils::log("Server".to_string(), "Request processing done".to_string())
                    }
                    Err(_) => utils::log(
                        "Server".to_string(),
                        "Request processing failed".to_string(),
                    ),
                }
            });
        }
    }

    async fn process_request(
        server_instance: Arc<NaadanServer<E>>,
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
            let query_engine =
                NaadanQueryEngine::init(server_instance.transaction_manager.clone()).await;

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
            utils::log("Server".to_string(), format!("Read zero bytes from server"));
            return Err(NaadanError::Unknown);
        }

        count += bytes;

        //utils::log(format!("Bytes: {:?}", buffer));
        if buffer.as_slice()[(count - 4)..count] == b"EOF\n".to_vec() {
            if buffer.len() <= 5 {
                utils::log("Server".to_string(), format!("Empty query received"));
                return Err(NaadanError::Unknown);
            }
            break;
        }
    }

    Ok(from_utf8(&buffer[..(count - 4)]).unwrap())
}

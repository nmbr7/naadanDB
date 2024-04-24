use std::sync::Arc;

use libnaadandb::{
    query::{query_engine::NaadanQueryEngine, NaadanQuery},
    server::SessionContext,
    storage::{storage_engine::NaadanStorageEngine, StorageEngine},
    transaction::TransactionManager,
};
use tokio::{process::Command, sync::Mutex};

type ArcStorageEngine = Arc<Mutex<Box<dyn StorageEngine>>>;

async fn clean_db_files() {
    Command::new("rm")
        .args(&["/tmp/DB_*"])
        .status()
        .await
        .unwrap();
}

fn create_storage_instance() -> ArcStorageEngine {
    let storage: ArcStorageEngine = Arc::new(Mutex::new(Box::new(NaadanStorageEngine::init(100))));

    storage
}

async fn reset_storage_and_process_queries(queries: &[&str]) {
    clean_db_files().await;
    let storage = create_storage_instance();
    load_db_data(storage.clone()).await;

    process_queries(queries, storage).await;
}

async fn process_queries(queries: &[&str], storage: ArcStorageEngine) {
    for query in queries {
        process_query(query.to_string(), storage.clone()).await;
    }
}

async fn process_query(query: String, storage: ArcStorageEngine) {
    // Parse the provided SQL query.
    let sql_query = NaadanQuery::init(query).unwrap();

    // Init a new query engine instance with reference to the global shared storage engine.
    let query_engine =
        NaadanQueryEngine::init(storage, Arc::new(Mutex::new(TransactionManager::init()))).await;

    let mut session_context = SessionContext::new();

    // Process the sql query Logical_Plan -> Physical_Plan -> Execute.
    let query_results = query_engine
        .process_query(&mut session_context, sql_query)
        .await;

    println!("********************************************");

    for query_result in query_results {
        match query_result {
            Ok(val) => println!("{}", val.to_string()),
            Err(err) => println!("Query execution failed: {}", err),
        }
    }
    println!("********************************************");
}

/// Load the base setup data in the DB
async fn load_db_data(storage: ArcStorageEngine) {
    let queries = [
        "Create table test1 (id int, name varchar(255))",
        "Insert into test1 values(1,'rom'),(2,'rob')",
    ];

    for query in queries {
        process_query(query.to_string(), storage.clone()).await;
    }
}

// ******************** Test Cases ******************** //

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_create_insert_select() {
    clean_db_files().await;

    let storage = create_storage_instance();

    let queries = [
        "Create table test1 (id int, name varchar(255))",
        "Insert into test1 values(1,'rom'),(2,'rob')",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), storage).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_none_predicate_update() {
    clean_db_files().await;
    let storage = create_storage_instance();
    load_db_data(storage.clone()).await;

    let queries = [
        "update test1 set name = 'Tommy'",
        "Select * from test1",
        "Select * from test1",
        "update test1 set id = 4",
        "Select * from test1",
    ];

    reset_storage_and_process_queries(queries.as_slice()).await;
}

// TODO: Update Test with predicate

// TODO: Select Test with predicate

// TODO: Select Test with 'join'

// TODO: Select Test with 'join with predicate'

// TODO: Select Test with 'limit'

// TODO: Select Test with 'order by'

// TODO: Select Test with 'group by'

// TODO: Select Test with all the common expressions
//       - join, Predicate, group by, order by, limit

// TODO: Transaction Test - Success case - parallel write
//       Insert 5 rows in a table
//       Select all and assert
//       Run 2 transactions and update 2 independent rows
//       The final select should show consistent result for the updated rows

// TODO: Transaction Test - Success case - parallel write
//       Insert 5 rows in a table
//       Select all and assert
//       Run 2 transactions and update 2 same rows
//       second transaction will fail and need to be re-run
//       The final select should show consistent result for the updated rows

// TODO: Transaction Test - Failure case - complete rollback
//       Insert 5 rows in a table
//       Select all and assert
//       Run 2 transactions and update 2 same rows
//       Rollback 1 transaction or kill the client
//       The final select should show consistent result for the updated rows

// TODO: Transaction Test - Success case - parallel read and write
//       Insert 5 rows in a table
//       Select all and assert
//       Run 4 transactions 2 of them updating 2 rows and other 2 reading the same updated rows
//       The 2 reads should return the row data as of the transaction start time
//       The final select should show consistent result for the updated rows

// TODO: Transaction Test - Success Case - Bank account balance case
//       Insert 100 bank accounts and balance
//       Run 100 transactions moving money from 1 account to another or randomly
//       Final sum of all account balance should be the same as that at the start.

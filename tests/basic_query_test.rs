use std::{fs::File, io::Write, sync::Arc};

use libnaadandb::{
    query::{query_engine::NaadanQueryEngine, NaadanQuery},
    server::SessionContext,
    storage::storage_engine::NaadanStorageEngine,
    transaction::TransactionManager,
};
use tokio::process::Command;

type ArcStorageEngine = Arc<Box<NaadanStorageEngine>>;

async fn clean_db_files() {
    Command::new("rm")
        .args(&["/tmp/DB_*"])
        .status()
        .await
        .unwrap();
}

fn create_storage_instance() -> ArcStorageEngine {
    let storage: ArcStorageEngine = Arc::new(Box::new(NaadanStorageEngine::init(100)));

    storage
}

async fn process_queries(
    queries: &[&str],
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut session_context = SessionContext::new();
    for query in queries {
        process_query(
            &mut session_context,
            query.to_string(),
            transaction_manager.clone(),
        )
        .await;
    }
}

async fn process_query(
    session_context: &mut SessionContext,
    query: String,
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut file = File::options()
        .create(true)
        .write(true)
        .append(true)
        .open("/tmp/Naadan_db_test.log")
        .unwrap();

    let mut file_log_string: String;

    // Parse the provided SQL query.
    let sql_query = NaadanQuery::init(query.clone()).unwrap();
    // Init a new query engine instance with reference to the global shared storage engine.
    let query_engine = NaadanQueryEngine::init(transaction_manager).await;

    file_log_string = format!("Started Query: [{}]", query,);

    file.write_all(format!("{}\n", file_log_string).as_bytes())
        .unwrap();
    file.flush().unwrap();

    // Process the sql query Logical_Plan -> Physical_Plan -> Execute.
    let query_results = query_engine.process_query(session_context, sql_query).await;

    for query_result in query_results {
        match query_result {
            Ok(val) => {
                //query.truncate(usize::pow(2, 8));
                file_log_string = format!(
                    "Query: [{}]..... execution succeeded with result: {}",
                    query,
                    val.to_string()
                );
            }
            Err(err) => {
                file_log_string = format!("Query: [{}] execution failed: {} ", query, err);
            }
        }

        file.write_all(format!("Finished {}\n", file_log_string).as_bytes())
            .unwrap();
        file.flush().unwrap();
    }
}

/// Load the base setup data in the DB
async fn load_db_data_batch(
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut queries: Vec<String> =
        vec!["Create table test1 (id int, ii int, name varchar, b int)".to_string()];

    let mut val: Vec<String> = vec![];
    for no in 1..=10_000 {
        val.push(format!(
            "({},{},'{}Test',1234)",
            no,
            no + 1,
            (no % 10).to_string()
        ));
    }

    queries.push(format!(
        "Insert into test1 (id, ii, name, b) values{}",
        val.join(",")
    ));
    let str_array: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

    process_queries(str_array.as_slice(), transaction_manager.clone()).await;
}

/// Load the base setup data in the DB
async fn load_db_data_batch_with_size(
    count: usize,
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut queries: Vec<String> =
        vec!["Create table test1 (id int, score int, name varchar, rate int)".to_string()];

    let mut val: Vec<String> = vec![];
    for no in 1..=count {
        val.push(format!(
            "({},{},'{}',1234)",
            no,
            no + 1,
            (no % 10).to_string()
        ));
    }

    queries.push(format!(
        "Insert into test1 (id,score,name,rate) values{}",
        val.join(",")
    ));
    let str_array: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

    process_queries(str_array.as_slice(), transaction_manager.clone()).await;
}

async fn load_db_data_seq(transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>) {
    let mut queries: Vec<String> = vec!["Create table test1 (id int, name varchar)".to_string()];

    for no in 1..200 {
        queries.push(format!("Insert into test1 (id,name) values({},'ro')", no));
    }

    let str_array: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

    process_queries(str_array.as_slice(), transaction_manager.clone()).await;
}

// ******************** Test Cases ******************** //

/// Basic test -- FixMe
#[tokio::test(flavor = "multi_thread")]
async fn basic_create_insert_select_test() {
    clean_db_files().await;

    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    let queries = vec![
        "Create table test1 (id int, name varchar)",
        "Insert into test1 values(1,'rom'),(2,'rob')",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager).await;
}

/// Basic txn test
#[tokio::test(flavor = "multi_thread")]
async fn txn_basic_test() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(10, transaction_manager.clone()).await;

    let queries = [
        "update test1 set name = 'To'",
        "BEGIN",
        "update test1 set id = 4",
        "update test1 set id = 6",
        "COMMIT",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager).await;
}

/// Basic test update query with and without predicate
#[tokio::test(flavor = "multi_thread")]
async fn update_query_basic_test() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(30, transaction_manager.clone()).await;

    let queries = [
        "BEGIN",
        "update test1 set rate = 7777",
        "update test1 set rate = 77696",
        "update test1 set name = 'LatestDate'",
        "update test1 set rate = 2147483647",
        "Select * from test1",
        "update test1 set name = 'La'",
        "COMMIT",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;

    load_db_data_batch_with_size(200, transaction_manager.clone()).await;

    let queries = [
        "BEGIN",
        "update test1 set rate = 9898",
        "update test1 set name = 'newestDate'",
        "update test1 set rate = 333347",
        "COMMIT",
        "Select * from test1",
        "update test1 set rate = 77 where id > 150",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;
}

/// Random updates and read in parallel txns
#[tokio::test(flavor = "multi_thread")]
async fn txn_parallel_update_random() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(215, transaction_manager.clone()).await;

    let transaction1 = transaction_manager.clone();
    let t1 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "update test1 set name = 'InitCh' where id > 200",
            "Select * from test1",
            "BEGIN",
            "update test1 set rate = 77696 where id > 150",
            "update test1 set score = 83647 where id > 180",
            "update test1 set name = 'Laaaaaaaaaaaaaaaaaaa' where id > 150",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction1.clone()).await;
    });

    let transaction2 = transaction_manager.clone();
    let t2 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set name = 'JK' where id < 140",
            "update test1 set score = 234568 where id < 140",
            "Select * from test1",
            "update test1 set name = 'Fin' where id < 140",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction2.clone()).await;
    });

    let _ = t1.await;
    let _ = t2.await;
}

/// Select query with basic predicate
#[tokio::test(flavor = "multi_thread")]
async fn select_query_predicate() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(10, transaction_manager.clone()).await;

    let queries = [
        "Select * from test1",
        "Select * from test1 where id > 5",
        "Select * from test1 where id != 7",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;
}

/// Transaction Test - Success case - parallel write
///      Run 2 transactions and update independent rows
///      The final select should show consistent result for the updated rows
#[tokio::test(flavor = "multi_thread")]
async fn txn_parallel_update_no_conflict() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(215, transaction_manager.clone()).await;

    let transaction1 = transaction_manager.clone();
    let t1 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set rate = 77696 where id > 150",
            "update test1 set score = 83647 where id > 180",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction1.clone()).await;
    });

    let transaction2 = transaction_manager.clone();
    let t2 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set score = 234568 where id < 140",
            "update test1 set name = 'Fin' where id < 140",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction2.clone()).await;
    });

    let _ = t1.await;
    let _ = t2.await;
}

/// Transaction Test - Success case - parallel write
///     Run 2 transactions and update same rows
///     Second transaction will fail and need to be re-run
///     The final select should show consistent result for the updated rows
#[tokio::test(flavor = "multi_thread")]
async fn txn_parallel_update_conflict() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(215, transaction_manager.clone()).await;

    let transaction1 = transaction_manager.clone();
    let t1 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set rate = 77696",
            "update test1 set name = 'LA'",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction1.clone()).await;
    });

    let transaction2 = transaction_manager.clone();
    let t2 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set name = 'JK'",
            "update test1 set score = 234568",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction2.clone()).await;
    });

    let _ = t1.await;
    let _ = t2.await;
}

// TODO: Select Test with 'join'

// TODO: Select Test with 'join with predicate'

// TODO: Select Test with 'limit'

// TODO: Select Test with 'order by'

// TODO: Select Test with 'group by'

// TODO: Select Test with all the common expressions
//       - join, Predicate, group by, order by, limit

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

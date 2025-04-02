use super::{POSTGRES_DBNAME, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER};
use std::time::{Duration, Instant};
use tokio_postgres::{connect, Client as PostgresClient, NoTls};

/* A utility struct for managing test interactions with a PostgreSQL table.
 # Fields
 - `client`: A `PostgresClient` instance used to execute queries against the database.
 - `table`: The name of the table being tested.

 This struct is typically used in test cases to encapsulate the database client
 and table name, simplifying the process of setting up and tearing down test data.
*/
pub struct TestTable {
    pub client: PostgresClient,
    pub table: String,
}

impl TestTable {
    pub async fn new(table: &str, create_sql: &str) -> Self {
        wait_for_postgres_ready().await;
        let client = create_postgres_client().await;
        let _ = drop_test_table(&client, table).await;
        create_test_table(&client, create_sql).await.unwrap();

        Self {
            client,
            table: table.to_string(),
        }
    }
}

impl Drop for TestTable {
    fn drop(&mut self) {
        let table = self.table.clone();

        tokio::task::spawn(async move {
            match create_postgres_client()
                .await
                .simple_query(&format!("DROP TABLE IF EXISTS {} CASCADE", table))
                .await
            {
                Ok(_) => {}
                Err(e) => eprintln!("Error dropping table {}: {}", table, e),
            }
        });
    }
}

fn postgres_connection_string() -> String {
    format!(
        "host={} port={} user={} password={} dbname={}",
        POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DBNAME
    )
}

async fn wait_for_postgres_ready() {
    let conn_str = postgres_connection_string();
    let start = Instant::now();

    loop {
        match connect(&conn_str, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                if client.query_one("SELECT 1", &[]).await.is_ok() {
                    return;
                }
            }
            Err(_) => {}
        }

        if start.elapsed() > Duration::from_secs(10) {
            panic!("Timed out waiting for Postgres");
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn create_postgres_client() -> PostgresClient {
    let conn_str = postgres_connection_string();
    let (client, connection) = connect(&conn_str, NoTls)
        .await
        .expect("Failed to connect to postgres");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

async fn create_test_table(client: &PostgresClient, create_sql: &str) -> Result<(), String> {
    client
        .simple_query(create_sql)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn drop_test_table(client: &PostgresClient, table_name: &str) -> Result<(), String> {
    let drop_sql = format!("DROP TABLE IF EXISTS {} CASCADE", table_name);
    client
        .simple_query(&drop_sql)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

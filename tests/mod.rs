mod clients;
mod common;

use common::postgres_utils::{postgres_connection_string, wait_for_postgres_ready};

#[tokio::test]
async fn test_basic_logical_replication() {
    wait_for_postgres_ready().await;

    let conn_str = postgres_connection_string();

    // Basic query to check connection
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let row = client.query_one("SELECT 1", &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
}

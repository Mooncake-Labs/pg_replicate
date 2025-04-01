use std::time::{Duration, Instant};
use tokio_postgres::{connect, NoTls};

pub fn postgres_connection_string() -> String {
    "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres".to_string()
}

pub async fn wait_for_postgres_ready() {
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

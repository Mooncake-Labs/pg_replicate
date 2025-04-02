use crate::common::{
    POSTGRES_DBNAME, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER,
};
use pg_replicate::clients::postgres::ReplicationClient;
use pg_replicate::table::LookupKey;
pub mod postgres;

pub async fn create_replication_client() -> ReplicationClient {
    ReplicationClient::connect_no_tls(
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DBNAME,
        POSTGRES_USER,
        Some(POSTGRES_PASSWORD.to_string()),
    )
    .await
    .expect("Failed to connect to postgres")
}

/// Helper function to assert that a LookupKey is a Key type with the expected columns
pub fn assert_is_key(lookup_key: &LookupKey, expected_columns: &[&str]) {
    match lookup_key {
        LookupKey::Key { name: _, columns } => {
            let expected: Vec<String> = expected_columns.iter().map(|&s| s.to_string()).collect();
            assert_eq!(
                &expected, columns,
                "Key columns don't match expected columns"
            );
        }
        LookupKey::FullRow => panic!("Expected Key type but got FullRow"),
    }
}

/// Helper function to assert that a LookupKey is FullRow
pub fn assert_is_full_row(lookup_key: &LookupKey) {
    match lookup_key {
        LookupKey::Key { name, columns } => {
            panic!("Expected FullRow but got Key({}, {:?})", name, columns)
        }
        LookupKey::FullRow => (),
    }
}

pub async fn test_lookup_key_with_definition(
    table_name: &str,
    create_sql: &str,
    expected_key: Option<Vec<&str>>,
    publication: Option<&str>,
) -> Result<(), anyhow::Error> {
    use crate::common::postgres_utils::TestTable;
    use pg_replicate::table::TableName;

    let test_table = TestTable::new(table_name, create_sql).await;

    if let Some(pub_name) = publication {
        let _ = test_table
            .client
            .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", pub_name))
            .await;
        let _ = test_table
            .client
            .simple_query(&format!(
                "CREATE PUBLICATION {} FOR TABLE {}",
                pub_name, table_name
            ))
            .await;
    }

    let replication_client = create_replication_client().await;

    let table_name = TableName {
        schema: "public".to_string(),
        name: table_name.to_string(),
    };

    let table_id = replication_client
        .get_table_id(&table_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("table ID not found!"))?;

    let column_schemas = replication_client
        .get_column_schemas(table_id, publication)
        .await?;
    let lookup_key = replication_client
        .get_lookup_key(table_id, &column_schemas)
        .await?;

    match expected_key {
        Some(columns) => assert_is_key(&lookup_key, &columns),
        None => assert_is_full_row(&lookup_key),
    }

    if let Some(pub_name) = publication {
        let _ = test_table
            .client
            .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", pub_name))
            .await;
    }

    Ok(())
}

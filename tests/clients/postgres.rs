use super::{
    assert_is_full_row, assert_is_key, create_replication_client, test_lookup_key_with_definition,
};

use crate::common::postgres_utils::TestTable;
use pg_replicate::table::TableName;

#[tokio::test]
async fn test_lookup_key_with_primary_key() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_pk_table (
            id INT PRIMARY KEY,
            data TEXT
        )
    ";

    test_lookup_key_with_definition("test_pk_table", create_sql, Some(vec!["id"]), None).await
}

#[tokio::test]
async fn test_lookup_key_with_composite_primary_key() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_composite_pk_table (
            id1 INT,
            id2 TEXT,
            data TEXT,
            PRIMARY KEY (id1, id2)
        )
    ";

    test_lookup_key_with_definition(
        "test_composite_pk_table",
        create_sql,
        Some(vec!["id1", "id2"]),
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_with_unique_constraint() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_unique_constraint_table (
            id INT,
            email TEXT NOT NULL UNIQUE,
            data TEXT
        )
    ";

    test_lookup_key_with_definition(
        "test_unique_constraint_table",
        create_sql,
        Some(vec!["email"]),
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_prefers_primary_key_over_unique() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_pk_and_unique_table (
            id INT PRIMARY KEY,
            email TEXT UNIQUE,
            data TEXT
        )
    ";

    test_lookup_key_with_definition(
        "test_pk_and_unique_table",
        create_sql,
        Some(vec!["id"]),
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_ignores_nullable_unique_columns() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_nullable_unique_table (
            id INT,
            email TEXT NULL UNIQUE,
            data TEXT
        )
    ";

    test_lookup_key_with_definition(
        "test_nullable_unique_table",
        create_sql,
        None, // Expect FullRow
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_ignores_partial_indexes() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_partial_index_table (
            id INT,
            email TEXT,
            active BOOLEAN,
            data TEXT
        );
        CREATE UNIQUE INDEX idx_partial_email ON test_partial_index_table (email) WHERE active = true;
    ";

    test_lookup_key_with_definition(
        "test_partial_index_table",
        create_sql,
        None, // Expect FullRow
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_with_no_unique_constraints() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_no_unique_table (
            id INT,
            data TEXT
        )
    ";

    test_lookup_key_with_definition(
        "test_no_unique_table",
        create_sql,
        None, // Expect FullRow
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_with_multicolumn_unique_index() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_multicolumn_unique_index (
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT,
            data TEXT,
            CONSTRAINT unique_name UNIQUE (first_name, last_name)
        )
    ";

    test_lookup_key_with_definition(
        "test_multicolumn_unique_index",
        create_sql,
        Some(vec!["first_name", "last_name"]),
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_respects_publication_columns() -> Result<(), anyhow::Error> {
    let pub_name = "test_pub";
    let table_name = "test_publication_columns";

    let create_sql = format!(
        "
        CREATE TABLE {} (
            id INT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            data TEXT
        )
        ",
        table_name
    );

    // Create a test table instance
    let test_table = TestTable::new(table_name, &create_sql).await;

    // Create publication with column list
    test_table
        .client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", pub_name))
        .await
        .unwrap();
    test_table
        .client
        .simple_query(&format!(
            "CREATE PUBLICATION {} FOR TABLE {} (email, data)",
            pub_name, table_name
        ))
        .await
        .unwrap();

    let replication_client = create_replication_client().await;

    let table_name = TableName {
        schema: "public".to_string(),
        name: table_name.to_string(),
    };

    let table_id = replication_client
        .get_table_id(&table_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("table ID not found!"))?;

    // Get columns with publication filtering
    let column_schemas = replication_client
        .get_column_schemas(table_id, Some(pub_name))
        .await?;
    let lookup_key = replication_client
        .get_lookup_key(table_id, &column_schemas)
        .await?;

    // Should use email as the key since id is not in the publication
    assert_is_key(&lookup_key, &["email"]);

    // Clean up
    test_table
        .client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", pub_name))
        .await
        .unwrap();

    Ok(())
}

#[tokio::test]
async fn test_lookup_key_with_multiple_unique_indexes() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_multiple_unique_indexes (
            id INT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            data TEXT
        )
    ";

    // Should choose the first unique index alphabetically after primary keys
    test_lookup_key_with_definition(
        "test_multiple_unique_indexes",
        create_sql,
        Some(vec!["email"]), // Postgres sorts indexes alphabetically
        None,
    )
    .await
}

#[tokio::test]
async fn test_lookup_key_with_mixed_nullable_non_nullable_unique() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_mixed_nullable (
            id INT,
            username TEXT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            data TEXT
        )
    ";

    // Should choose the non-nullable unique index
    test_lookup_key_with_definition("test_mixed_nullable", create_sql, Some(vec!["email"]), None)
        .await
}

#[tokio::test]
async fn test_lookup_key_with_no_columns_in_publication() -> Result<(), anyhow::Error> {
    let pub_name = "test_pub_only_data";
    let table_name = "test_publication_no_key_columns";

    let create_sql = format!(
        "
        CREATE TABLE {} (
            id INT PRIMARY KEY,
            username TEXT UNIQUE,
            data TEXT
        )
        ",
        table_name
    );

    // Create a test table instance
    let test_table = TestTable::new(table_name, &create_sql).await;

    // Create a publication that only includes data column
    test_table
        .client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", pub_name))
        .await
        .unwrap();
    test_table
        .client
        .simple_query(&format!(
            "CREATE PUBLICATION {} FOR TABLE {} (data)",
            pub_name, table_name
        ))
        .await
        .unwrap();

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
        .get_column_schemas(table_id, Some(pub_name))
        .await?;
    let lookup_key = replication_client
        .get_lookup_key(table_id, &column_schemas)
        .await?;

    // Should fallback to full row since no key columns are in the publication
    assert_is_full_row(&lookup_key);

    // Clean up
    test_table
        .client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", pub_name))
        .await
        .unwrap();

    Ok(())
}

#[tokio::test]
async fn test_lookup_key_ignores_deferrable_constraints() -> Result<(), anyhow::Error> {
    let create_sql = "
        CREATE TABLE test_deferrable_constraint_table (
            id INT,
            email TEXT NOT NULL,
            username TEXT NOT NULL,
            data TEXT,
            CONSTRAINT deferrable_unique_email UNIQUE (email) DEFERRABLE,
            CONSTRAINT non_deferrable_unique_username UNIQUE (username)
        )
    ";

    // Should choose the non-deferrable unique constraint (username)
    // and ignore the deferrable one (email)
    test_lookup_key_with_definition(
        "test_deferrable_constraint_table",
        create_sql,
        Some(vec!["username"]),
        None,
    )
    .await
}

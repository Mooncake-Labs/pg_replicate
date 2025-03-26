use std::fmt::Display;

use pg_escape::quote_identifier;
use tokio_postgres::types::Type;

#[derive(Debug, Clone)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn as_quoted_identifier(&self) -> String {
        let quoted_schema = quote_identifier(&self.schema);
        let quoted_name = quote_identifier(&self.name);
        format!("{quoted_schema}.{quoted_name}")
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

type TypeModifier = i32;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub typ: Type,
    pub modifier: TypeModifier,
    pub nullable: bool,
    pub primary: bool,
}

pub type TableId = u32;

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: TableName,
    pub table_id: TableId,
    pub column_schemas: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn has_primary_keys(&self) -> bool {
        self.column_schemas.iter().any(|cs| cs.primary)
    }

    /// For replica identity 'full', any table with columns is identifiable
    pub fn has_identifying_columns(&self) -> bool {
        self.has_primary_keys() || !self.column_schemas.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::Type;

    fn create_test_table_schema(columns: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            table_name: TableName {
                schema: "public".to_string(),
                name: "test_table".to_string(),
            },
            table_id: 1,
            column_schemas: columns,
        }
    }

    fn create_column(name: &str, nullable: bool, primary: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            typ: Type::VARCHAR,
            modifier: 0,
            nullable,
            primary,
        }
    }

    #[test]
    fn test_has_identifying_columns_with_primary_keys() {
        let schema = create_test_table_schema(vec![
            create_column("id", false, true),
            create_column("name", true, false),
        ]);

        assert!(schema.has_identifying_columns());
    }

    #[test]
    fn test_has_identifying_columns_without_primary_keys() {
        let schema = create_test_table_schema(vec![
            create_column("col1", false, false),
            create_column("col2", true, false),
        ]);

        assert!(schema.has_identifying_columns());
    }

    #[test]
    fn test_has_identifying_columns_empty_schema() {
        let schema = create_test_table_schema(vec![]);

        assert!(!schema.has_identifying_columns());
    }
}

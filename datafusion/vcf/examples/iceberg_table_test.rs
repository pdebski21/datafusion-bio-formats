use std::collections::HashMap;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

static REST_URI: &str = "http://localhost:8181";
static NAMESPACE: &str = "default";
static TABLE_NAME: &str = "t1";

///////////////////////////////////////////////////////////////////////////////////////////
// to setup tests running docker-compose.yaml with iceberg and rest services is required //
///////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let config = RestCatalogConfig::builder()
        .uri(REST_URI.to_string())
        .build();
    let catalog = RestCatalog::new(config);


    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_string());

    match catalog.namespace_exists(&namespace_ident).await {
        Ok(true) => {
            println!("Namespace already exists, dropping now.");
            if let Err(e) = catalog.drop_namespace(&namespace_ident).await {
                eprintln!("Failed to drop namespace: {}", e);
            }
        }
        Ok(false) => println!("Namespace does not exist."),
        Err(e) => eprintln!("Error checking namespace existence: {}", e),
    }

    let table_schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_ident.name.clone())
        .schema(table_schema.clone())
        .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
        .build();

    match catalog.table_exists(&table_ident).await {
        Ok(true) => {
            println!("Table already exists, dropping now.");
            if let Err(e) = catalog.drop_table(&table_ident).await {
                eprintln!("Failed to drop table: {}", e);
            }
        }
        Ok(false) => println!("Table does not exist."),
        Err(e) => eprintln!("Error checking table existence: {}", e),
    }

    let _ = catalog
        .drop_table(&table_ident)
        .await
        ;
    let _created_table = catalog
        .create_table(&table_ident.namespace, table_creation)
        .await
        .unwrap();

    assert!(catalog
        .list_tables(&namespace_ident)
        .await
        .unwrap()
        .contains(&table_ident));

    let loaded_table = catalog.load_table(&table_ident).await.unwrap();
    println!("Table {TABLE_NAME} loaded!\n\nTable: {:?}", loaded_table);
}
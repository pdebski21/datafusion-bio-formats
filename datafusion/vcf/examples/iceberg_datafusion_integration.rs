use std::collections::HashMap;
use std::sync::Arc;
use std::fs::File;

use datafusion::arrow::array::{BooleanArray, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
// use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
// use datafusion_vcf::table_provider::VcfTableProvider;
use datafusion_vcf::storage::VcfRemoteReader;

static REST_URI: &str = "http://localhost:8181";
static NAMESPACE: &str = "default";
static TABLE_NAME: &str = "t1";

///////////////////////////////////////////////////////////////////////////////////////////
// to setup tests running docker-compose.yaml with iceberg and rest services is required //
///////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    env_logger::init();

    let path = "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".to_string();
    let catalog = initialize_catalog().await?;

    let table_ident = create_table_schema(&catalog).await?;

    let _ = load_and_print_iceberg_table(&catalog, &table_ident).await?;

    let mut reader = VcfRemoteReader::new(path, 64, 1).await;
    let rb = reader.describe().await?;

    let ctx = SessionContext::new();
    let mem_table = MemTable::try_new(rb.schema().clone(), vec![vec![rb]])?;
    ctx.register_table("my_vcf_table", Arc::new(mem_table))?;

    let df = ctx.sql("SELECT * FROM my_vcf_table").await?;
    df.show().await?;

    Ok(())
}

async fn initialize_catalog() -> Result<RestCatalog, datafusion::error::DataFusionError> {
    let config = RestCatalogConfig::builder()
        .uri(REST_URI.to_string())
        .build();
    Ok(RestCatalog::new(config))
}

async fn create_table_schema(catalog: &RestCatalog) -> Result<TableIdent, datafusion::error::DataFusionError> {
    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_string());

    match catalog.namespace_exists(&namespace_ident).await {
        Ok(true) => {
            let _ = catalog.drop_namespace(&namespace_ident).await;
        }
        _ => {}
    }
    let _ = catalog.create_namespace(&namespace_ident, HashMap::new()).await;

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
        .schema(table_schema)
        .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
        .build();

    let _ = catalog.drop_table(&table_ident).await;

    if let Err(e) = catalog.create_table(&table_ident.namespace, table_creation).await {
        return Err(datafusion::error::DataFusionError::External(Box::new(e)));
    }

    Ok(table_ident)
}

async fn load_and_print_iceberg_table(catalog: &RestCatalog, table_ident: &TableIdent) -> Result<(), datafusion::error::DataFusionError> {
    let iceberg_table = catalog.load_table(table_ident).await.unwrap();
    println!("✅ Loaded Iceberg Table at: {}", iceberg_table.metadata().location());
    Ok(())
}

async fn create_table_schema(catalog: &RestCatalog) -> Result<TableIdent, datafusion::error::DataFusionError> {
    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_string());

    match catalog.namespace_exists(&namespace_ident).await {
        Ok(true) => {
            let _ = catalog.drop_namespace(&namespace_ident).await;
        }
        _ => {}
    }
    let _ = catalog.create_namespace(&namespace_ident, HashMap::new()).await;

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
        .schema(table_schema)
        .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
        .build();

    let _ = catalog.drop_table(&table_ident).await;

    if let Err(e) = catalog.create_table(&table_ident.namespace, table_creation).await {
        return Err(datafusion::error::DataFusionError::External(Box::new(e)));
    }

    Ok(table_ident)
}

async fn load_and_print_iceberg_table(catalog: &RestCatalog, table_ident: &TableIdent) -> Result<(), datafusion::error::DataFusionError> {
    let iceberg_table = catalog.load_table(table_ident).await.unwrap();
    println!("✅ Loaded Iceberg Table at: {}", iceberg_table.metadata().location());
    Ok(())
}
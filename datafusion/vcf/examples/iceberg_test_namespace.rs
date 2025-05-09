use std::collections::HashMap;

use iceberg::{Catalog, NamespaceIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

static REST_URI: &str = "http://localhost:8181";
use tokio;

///////////////////////////////////////////////////////////////////////////////////////////
// to setup tests running docker-compose.yaml with iceberg and rest services is required //
///////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() {
    let config = RestCatalogConfig::builder()
        .uri(REST_URI.to_string())
        .build();
    let catalog = RestCatalog::new(config);

    let existing_namespaces = catalog.list_namespaces(None).await.unwrap();
    println!(
        "Namespaces alreading in the existing catalog: {:?}",
        existing_namespaces
    );
  
    let namespace_ident =
        NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap();

    // Drop the namespace if it already exists.
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

    let _ = catalog.drop_namespace(&namespace_ident).await;
    
    let _created_namespace = catalog
        .create_namespace(
            &namespace_ident,
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        )
        .await
        .unwrap();
    println!("Namespace {:?} created!", namespace_ident);

    let loaded_namespace = catalog.get_namespace(&namespace_ident).await.unwrap();
    println!("Namespace loaded!\n\nNamespace: {:#?}", loaded_namespace,);
}
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_vcf::table_provider::VcfTableProvider;
use iceberg::catalog::hadoop::HadoopCatalog;
use iceberg::schema::{Schema as IcebergSchema, Type};
use iceberg::types::{PrimitiveType, StructType};
use iceberg::partition::PartitionSpec;
use iceberg_datafusion::IcebergTable;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    env_logger::init();
    
    // Define paths
    let vcf_path = "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".to_string();
    let warehouse_path = "/tmp/iceberg-warehouse/";
    
    // Create a DataFusion session
    let ctx = SessionContext::new();

    // 1. Register VCF table in DataFusion
    let vcf_provider = VcfTableProvider::new(vcf_path.clone(), None, None, Some(4), None, None)?;
    ctx.register_table("vcf_table", Arc::new(vcf_provider))?;

    // Query VCF data to show sample
    let df = ctx.sql("SELECT chrom, pos, id, ref, alt FROM vcf_table LIMIT 5").await?;
    println!("VCF data sample:");
    df.show().await?;

    // 2. Create an Iceberg catalog using HadoopCatalog (for local storage)
    let catalog = HadoopCatalog::new(warehouse_path.to_string());
    
    // 3. Define Iceberg schema for VCF data
    let iceberg_schema = IcebergSchema::new(vec![
        Type::primitive(PrimitiveType::String, true).with_name("chrom"),
        Type::primitive(PrimitiveType::Long, true).with_name("pos"),
        Type::primitive(PrimitiveType::String, true).with_name("id"),
        Type::primitive(PrimitiveType::String, true).with_name("ref"),
        Type::primitive(PrimitiveType::String, true).with_name("alt"),
    ]);

    // 4. Create the Iceberg table
    let partition_spec = PartitionSpec::unpartitioned();
    let iceberg_table = catalog.create_table(
        "default",
        "vcf_data",
        Arc::new(iceberg_schema),
        partition_spec,
        None, // No sort order
    ).await.expect("Failed to create Iceberg table");

    println!("Created Iceberg table at {:?}", iceberg_table.location());

    // 5. Register the Iceberg table with DataFusion
    let iceberg_provider = IcebergTable::try_new(iceberg_table)
        .expect("Failed to create Iceberg table provider");
    ctx.register_table("iceberg_vcf", Arc::new(iceberg_provider))?;

    // 6. Insert data from VCF into Iceberg table
    // Note: In a real implementation, you would use DataFusion's write capabilities
    // to write the data to the Iceberg table. This is a simplified example.
    ctx.sql("
        CREATE OR REPLACE TABLE iceberg_vcf 
        AS SELECT chrom, pos, id, ref, alt 
        FROM vcf_table 
        LIMIT 100
    ").await?;

    // 7. Query the Iceberg table to verify data
    let result = ctx.sql("SELECT * FROM iceberg_vcf LIMIT 5").await?;
    println!("Data from Iceberg table:");
    result.show().await?;

    Ok(())
}
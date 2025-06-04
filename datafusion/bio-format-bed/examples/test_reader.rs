use datafusion_bio_format_bed::storage::BedRemoteReader;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use futures::StreamExt;
use noodles::bed;
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let file_path = "gs://polars-bio-it/chr16_fragile_site.bed.bgz".to_string();
    let object_storage_options = ObjectStorageOptions::default();
    let mut reader = BedRemoteReader::<3>::new(file_path.clone(), object_storage_options).await;
    // read all records
    let mut records = reader.read_records_stream().await;
    while let Some(record_result) = records.next().await {
        match record_result {
            Ok(record) => println!("{:?}", record),
            Err(e) => eprintln!("Error reading record: {}", e),
        }
    }

    Ok(())
}

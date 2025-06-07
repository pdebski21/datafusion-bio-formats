use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_fastq::storage::{FastqLocalReader, FastqRemoteReader};
use futures_util::StreamExt;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let file_path = "gs://polars-bio-it/example.fastq.gz".to_string();
    // let file_path = "gs://polars-bio-it/example.fastq.bgz".to_string();

    // let file_path = "/Users/mwiewior/CLionProjects/datafusion-bio-formats/sandbox/example.fastq.gz".to_string();
    let file_path =
        "/Users/mwiewior/CLionProjects/datafusion-bio-formats/sandbox/example.fastq.bgz"
            .to_string();
    let object_storage_options = ObjectStorageOptions {
        allow_anonymous: false,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),        // 16 MB
        concurrent_fetches: Some(8), // Number of concurrent requests
        compression_type: None,
    };
    // let mut reader = FastqRemoteReader::new(file_path, object_storage_options).await?;
    let mut reader = FastqLocalReader::new(file_path, 1).await?;
    let mut records = reader.read_records().await;
    let mut cnt = 0;
    while let Some(record_result) = records.next().await {
        match record_result {
            Ok(record) => {
                // println!("Record: {:?}", record);
                cnt += 1;
            }
            Err(e) => eprintln!("Error reading record: {}", e),
        }
    }

    println!("Total records read: {}", cnt);
    Ok(())
}

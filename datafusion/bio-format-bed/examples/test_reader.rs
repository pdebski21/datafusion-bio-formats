use datafusion_bio_format_bed::storage::{BedLocalReader, BedRemoteReader};
use futures::StreamExt;

// use noodles::bed;
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // let file_path = "gs://polars-bio-it/chainMonDom5Link.bed.bgz".to_string();
    // let object_storage_options = ObjectStorageOptions{
    //     allow_anonymous: false,
    //     enable_request_payer: false,
    //     max_retries: Some(1),
    //     timeout: Some(300),
    //     chunk_size: Some(16), // 16 MB
    //     concurrent_fetches: Some(8), // Number of concurrent requests
    //     compression_type: None,
    // };
    // let mut reader = BedRemoteReader::new(file_path.clone(), object_storage_options).await;

    // let file_path = "/Users/mwiewior/research/data/AIListTestData/chainMonDom5Link.bed".to_string();
    let file_path =
        "/Users/mwiewior/CLionProjects/datafusion-bio-formats/sandbox/chr16_fragile_site.bed.bgz"
            .to_string();

    let mut reader = BedLocalReader::<6>::new(file_path, 1).await.unwrap();

    // read all records
    let mut cnt = 0;
    let mut records = reader.records();

    while let Some(record_result) = records.next().await {
        match record_result {
            Ok(record) => println!("Record: {:?}", record),
            Err(e) => eprintln!("Error reading record: {}", e),
        }
    }

    // let mut lines = reader.lines();
    // while let Some(record_result) = lines.next().await {
    //     match record_result {
    //         Ok(record) => cnt += 1,
    //         Err(e) => eprintln!("Error reading record: {}", e),
    //     }
    // }
    println!("Total records read: {}", cnt);
    Ok(())
}

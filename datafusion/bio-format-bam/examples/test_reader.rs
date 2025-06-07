use datafusion_bio_format_bam::storage::BamReader;
use futures_util::StreamExt;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/Users/mwiewior/research/data/WES/NA12878.proper.wes.md.bam".to_string();
    let reader = BamReader::new(
        file_path,
        Some(4), // Number of threads
        None,    // Object storage options
    );
    let mut reader = reader.await;

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

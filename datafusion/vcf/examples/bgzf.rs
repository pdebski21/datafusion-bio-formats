use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use std::time::Instant;
use log::debug;
use noodles::{bgzf, vcf};
use noodles::vcf::io::Reader;
use noodles_bgzf::MultithreadedReader;


#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {

    for file_path in vec!["/tmp/gnomad.exomes.v4.1.sites.chr21.vcf.bgz", "/tmp/gnomad.v4.1.sv.sites.vcf.gz"] {
        for thread_num in [1,2,4] {
            // let mut reader = File::open(file_path)
            //     .map(|f| noodles_bgzf::MultithreadedReader::with_worker_count(NonZero::new(thread_num).unwrap(), f))
            //     .map(vcf::io::Reader::new)?;
            let mut reader = get_local_vcf_reader(file_path.to_string(), thread_num)?;
            let start = Instant::now();
            let header = reader.read_header()?;
            println!("Records count: {}", reader.records().count());
            let duration = start.elapsed();
            println!("Time elapsed in reading the VCF file with {} threads: {:?}",  thread_num, duration);
        }
    }


    Ok(())
}
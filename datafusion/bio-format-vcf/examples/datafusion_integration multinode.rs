use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::time::Instant;
use std::fs::{OpenOptions};
use serde::Serialize;
use tokio::runtime::Runtime;
use sysinfo::{System, SystemExt, ProcessExt};
use datafusion::prelude::SessionContext;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use libc::{getrusage, rusage, RUSAGE_SELF};

#[derive(Serialize)]
struct BenchmarkResult {
    benchmark_name: String,
    query: String,
    elapsed_sec: f64,
    cpu_time_sec: f64,
    cpu_util_percent: f64,
    memory_diff_mb: f64,
    rows_processed: usize,
    throughput_rows_per_sec: f64,
}

fn get_cpu_time_sec() -> f64 {
    let mut usage = rusage {
        ru_utime: libc::timeval { tv_sec: 0, tv_usec: 0 },
        ru_stime: libc::timeval { tv_sec: 0, tv_usec: 0 },
        ..unsafe { std::mem::zeroed() }
    };
    unsafe {
        getrusage(RUSAGE_SELF, &mut usage);
    }
    let user_sec = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 * 1e-6;
    let sys_sec = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 * 1e-6;
    user_sec + sys_sec
}

fn benchmark_datafusion(_: &mut Criterion) {
    let path = "/Users/piotrdebski/Desktop/PW_OKNO/Magisterka/testdata/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".to_string();
    // let path = "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".to_string();
    let infos = None;

    let configurations = vec![
        (1024 * 1024, 4, 4),
        (2048 * 1024, 4, 4),
        (4096 * 1024, 16, 16),
    ];

    let queries = vec![
        "SELECT chrom FROM custom_table LIMIT 10000",
        // "SELECT chrom, start FROM custom_table WHERE start < 100000",
        // "SELECT * FROM custom_table WHERE FILTER = 'PASS'",
        // "SELECT COUNT(*) FROM custom_table",
    ];

    // let object_storage_options = |chunk_size: usize, concurrent_fetches: usize| ObjectStorageOptions {
    //     chunk_size: Some(chunk_size),
    //     concurrent_fetches: Some(concurrent_fetches),
    //     allow_anonymous: true,
    //     enable_request_payer: false,
    //     max_retries: Some(3),
    //     timeout: Some(30),
    //     compression_type: None,
    // };

    // let object_storage_options = None;

    let rt = Runtime::new().expect("Failed to create Tokio runtime");
    let mut results: Vec<BenchmarkResult> = Vec::new();

    for query in &queries {
        for (chunk_size, concurrent_fetches, num_threads) in &configurations {
            let benchmark_name = format!(
                "query_chunk_{}_fetches_{}_threads_{}",
                chunk_size, concurrent_fetches, num_threads
            );

            // let object_opts = object_storage_options(*chunk_size, *concurrent_fetches);
            // let object_opts = None;

            let result = rt.block_on(async {
                let pid = sysinfo::get_current_pid().expect("Could not get PID");
                let mut system = System::new();
                system.refresh_process(pid);
                let process = system.process(pid).expect("Process not found");
                let initial_memory_kb = process.memory();
                let cpu_time_start = get_cpu_time_sec();
                let start_time = Instant::now();

                let ctx = SessionContext::new();
                let provider = VcfTableProvider::new(
                    path.clone(),
                    infos.clone(),
                    None,
                    Some(*num_threads),
                    // Some(object_opts),
                    None,
                ).expect("Failed to create VcfTableProvider");

                ctx.register_table("custom_table", Arc::new(provider))
                    .expect("Failed to register table");

                let df = ctx.sql(query).await.expect("Failed to run SQL");
                let batches = df.collect().await.expect("Failed to collect results");

                let elapsed_sec = start_time.elapsed().as_secs_f64();
                let cpu_time_end = get_cpu_time_sec();
                system.refresh_process(pid);
                let process = system.process(pid).expect("Process not found after");

                let final_memory_kb = process.memory();
                let memory_diff_mb = (final_memory_kb as isize - initial_memory_kb as isize) as f64 / 1024.0;
                let cpu_time_diff = cpu_time_end - cpu_time_start;
                let cpu_util_percent = if elapsed_sec > 0.0 {
                    (cpu_time_diff / elapsed_sec) * 100.0
                } else {
                    0.0
                };

                let rows_processed: usize = batches.iter().map(|batch| batch.num_rows()).sum();
                let throughput = if elapsed_sec > 0.0 {
                    rows_processed as f64 / elapsed_sec
                } else {
                    0.0
                };

                println!(
                    "[{}] Query: \"{}\"\n\u{2192} Elapsed: {:.2}s, CPU Time \u{0394}: {:.2}s ({:.2}%), Memory \u{0394}: {:+.2} MB, Rows: {}, Throughput: {:.2} rows/s",
                    benchmark_name, query, elapsed_sec, cpu_time_diff, cpu_util_percent, memory_diff_mb, rows_processed, throughput
                );

                BenchmarkResult {
                    benchmark_name,
                    query: query.to_string(),
                    elapsed_sec,
                    cpu_time_sec: cpu_time_diff,
                    cpu_util_percent,
                    memory_diff_mb,
                    rows_processed,
                    throughput_rows_per_sec: throughput,
                }
            });

            results.push(result);
        }
    }

    let json = serde_json::to_string_pretty(&results).expect("Failed to serialize results to JSON");
    std::fs::write("benchmark_results.json", json).expect("Failed to write JSON file");

    let mut writer = csv::Writer::from_writer(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("benchmark_results.csv")
            .expect("Failed to open CSV file"),
    );
    for result in &results {
        writer.serialize(result).expect("Failed to write CSV row");
    }
    writer.flush().expect("Failed to flush CSV writer");

    println!("\n✅ Benchmark complete. Results saved to:");
    println!("\u{2022} benchmark_results.json");
    println!("\u{2022} benchmark_results.csv");
}

criterion_group!(benches, benchmark_datafusion);
criterion_main!(benches);
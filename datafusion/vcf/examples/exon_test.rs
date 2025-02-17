// use datafusion::error::Result;
// use datafusion::prelude::*;
// use exon::{ExonRuntimeEnvExt, ExonSession};
//
// #[tokio::main(flavor = "multi_thread")]
// async fn main() -> Result<()>{
//     let ctx = ExonSession::new_exon()?;
//     let path = "/tmp/gnomad.v4.1.sv.sites.vcf.gz";
//     ctx.session
//         .runtime_env()
//         .exon_register_object_store_uri(path)
//         .await?;
//
//     ctx.session.sql(
//         format!(
//             "CREATE EXTERNAL TABLE custom_table STORED AS VCF LOCATION '{}' OPTIONS (compression gzip);",
//            path
//         )
//             .as_str(),
//     )
//         .await?;
//     ctx.sql("SELECT count(*) FROM custom_table").await?.show().await?;
//     Ok(())
// }
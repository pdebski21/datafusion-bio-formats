use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_vcf::table_provider::VcfTableProvider;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    // Create a new context with the default configuration
    let ctx = SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true").await?;
    let table_provider = VcfTableProvider::new("/tmp/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".parse().unwrap(), vec!["SVTYPE".parse().unwrap()], vec![], Some(2))?;
    // let table_provider = VcfTableProvider::new("gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".parse().unwrap(), vec!["SVTYPE".parse().unwrap()], vec![])?;
    ctx.register_table("custom_table", Arc::new(table_provider)).expect("TODO: panic message");
    let df = ctx.sql("SELECT count(*) FROM custom_table").await?;
    // println!("{:?}", df.explain(false, false)?);
    df.show().await.expect("TODO: panic message");
    // println!("{:?}", );
    Ok(())
}
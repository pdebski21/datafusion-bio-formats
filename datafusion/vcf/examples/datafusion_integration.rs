use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_vcf::table_provider::VcfTableProvider;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    env_logger::init();
    let path = "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".to_string();
    let infos  = Some(Vec::from(["AC".to_string(), "AC_XX".to_string()]));
    // let infos  = None;
    // Create a new context with the default configuration
    let ctx = SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true").await?;
    // let table_provider = VcfTableProvider::new("/tmp/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".parse().unwrap(), vec!["SVTYPE".parse().unwrap()], vec![], Some(8))?;
    let table_provider = VcfTableProvider::new(path, infos, None, None)?;
    ctx.register_table("custom_table", Arc::new(table_provider)).expect("TODO: panic message");
    let df = ctx.sql("SELECT * FROM custom_table limit 5").await?;
    // println!("{:?}", df.explain(false, false)?);
    df.show().await.expect("TODO: panic message");
    // println!("{:?}", );
    Ok(())
}
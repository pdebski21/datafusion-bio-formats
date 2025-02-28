use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_vcf::table_provider::VcfTableProvider;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    env_logger::init();
    // let path = "gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".to_string();
    // let path = "gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz".to_string();
    let path = "gs://genomics-public-data/platinum-genomes/vcf/NA12878_S1.genome.vcf".to_string();
    // let path ="/tmp/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".to_string();
    // let path ="/tmp/gnomad.v4.1.sv.sites.vcf.gz".to_string();
    // let infos  = Some(Vec::from(["AC".to_string(), "AF".to_string(), "AN".to_string(), "FS".to_string(), "AN_raw".to_string(), "variant_type".to_string(), "AS_culprit".to_string(), "only_het".to_string()]));
    // let infos  = Some(Vec::from(["SVTYPE".to_string()]));
    // let infos  = Some(Vec::from(["AF".to_string()]));
    let infos  = None;
    // Create a new context with the default configuration
    let ctx = SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true").await?;
    // let table_provider = VcfTableProvider::new("/tmp/gnomad.exomes.v4.1.sites.chr21.vcf.bgz".parse().unwrap(), vec!["SVTYPE".parse().unwrap()], vec![], Some(8))?;
    let table_provider = VcfTableProvider::new(path, infos, None, Some(1))?;
    ctx.register_table("custom_table", Arc::new(table_provider)).expect("TODO: panic message");
    // let df = ctx.sql("SELECT count(*) FROM custom_table").await?;
    let df = ctx.sql("SELECT * FROM custom_table LIMIT 5").await?;
    // println!("{:?}", df.explain(false, false)?);
    df.show().await.expect("TODO: panic message");
    // println!("{:?}", );
    Ok(())
}
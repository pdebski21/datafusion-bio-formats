use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, Float64Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use futures::{stream, StreamExt, TryStreamExt};
// use rust_htslib::bcf::{Read, Reader, Records};
use std::{str};
use std::fs::File;
use std::iter::FromFn;
use std::num::NonZero;
use std::ops::Deref;
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use bytes::Bytes;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::Iter;
use futures_core::Stream;
use noodles::vcf;

use crate::storage::{get_compression_type, get_remote_stream, get_remote_stream_bgzf, get_storage_type, CompressionType, StorageType};
// use noodles::bgzf;
// use noodles_vcf as vcf;

fn build_record_batch(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    ids: &[String],
    refs: &[String],
    alts: &[String],
    quals: &[f64],
    filters: &[String],
) -> datafusion::error::Result<RecordBatch> {
    let chrom_array = Arc::new(StringArray::from(chroms.to_vec())) as Arc<dyn Array>;
    let pos_start_array = Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>;
    let pos_end_array = Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>;
    let id_array = Arc::new(StringArray::from(ids.to_vec())) as Arc<dyn Array>;
    let ref_array = Arc::new(StringArray::from(refs.to_vec())) as Arc<dyn Array>;
    let alt_array = Arc::new(StringArray::from(alts.to_vec())) as Arc<dyn Array>;
    let qual_array = Arc::new(Float64Array::from(quals.to_vec())) as Arc<dyn Array>;
    let filter_array = Arc::new(StringArray::from(filters.to_vec())) as Arc<dyn Array>;

    let arrays: Vec<Arc<dyn Array>> = vec![
        chrom_array, pos_start_array, pos_end_array, id_array, ref_array, alt_array, qual_array, filter_array,
    ];
    // println!("Creating record batch");
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
}



async fn get_local_vcf(file_path: String, schema_ref: SchemaRef, batch_size: usize, thread_num: usize) -> datafusion::error::Result<AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output=()> + Sized>> {
    let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
    let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
    let mut ids: Vec<String> = Vec::with_capacity(batch_size);
    let mut refs: Vec<String> = Vec::with_capacity(batch_size);
    let mut alts: Vec<String> = Vec::with_capacity(batch_size);
    let mut quals: Vec<f64> = Vec::with_capacity(batch_size);
    let mut filters: Vec<String> = Vec::with_capacity(batch_size);
    let mut count: usize = 0;
    let mut record_num = 0;
    let mut batch_num = 0;
    let schema = Arc::clone(&schema_ref);
    let file_path = file_path.clone();
    // let worker_count = thread::(NonZero::new(1).unwrap());
    let mut reader = File::open(file_path)
        .map(|f| noodles_bgzf::MultithreadedReader::with_worker_count(NonZero::new(thread_num).unwrap(), f))
        .map(vcf::io::Reader::new).unwrap();
    let header = reader.read_header().unwrap();


    let iter = std::iter::from_fn(move || {

        let mut records = reader.records();
        while count < batch_size {
            let record = records.next();
            record_num += 1;
            if record.is_none() {
                break;
            }
            let record = record.unwrap().unwrap();
            // For each record, fill the fixed columns.
            chroms.push(record.reference_sequence_name().to_string());
            poss.push(record.variant_start().unwrap().unwrap().get() as u32 + 1);
            ids.push(".".to_string());
            refs.push(record.reference_bases().to_string());
            alts.push(".".to_string());
            quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
            filters.push(".".to_string());
            count += 1;
        }
        if count == 0 {
            return None;
        }
        println!("Batch number: {}", batch_num);
        let batch = build_record_batch(Arc::clone(&schema), &chroms, &poss, &ids, &refs, &alts, &quals, &filters).unwrap();
        count = 0;
        chroms.clear();
        poss.clear();
        ids.clear();
        refs.clear();
        alts.clear();
        quals.clear();
        filters.clear();
        batch_num += 1;
        Some(Ok(batch))
    });
    stream::iter(iter)
}


async fn get_remote_vcf_stream(file_path: String, schema: SchemaRef, batch_size: usize) -> datafusion::error::Result<AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output=()> + Sized>> {
    let inner = get_remote_stream_bgzf(file_path.clone()).await.unwrap();
    let mut reader = vcf::r#async::io::Reader::new(inner);
    let header = reader.read_header().await?;

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
        let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
        let mut ids: Vec<String> = Vec::with_capacity(batch_size);
        let mut refs: Vec<String> = Vec::with_capacity(batch_size);
        let mut alts: Vec<String> = Vec::with_capacity(batch_size);
        let mut quals: Vec<f64> = Vec::with_capacity(batch_size);
        let mut filters: Vec<String> = Vec::with_capacity(batch_size);

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.
        let mut records = reader.records();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            poss.push(record.variant_start().unwrap().unwrap().get() as u32 + 1);
            ids.push(".".to_string());
            refs.push(record.reference_bases().to_string());
            alts.push(".".to_string());
            quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
            filters.push(".".to_string());
            record_num += 1;

            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                )?;
                batch_num += 1;
                // Optionally print batch number for debugging.
                println!("Yielding batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                ids.clear();
                refs.clear();
                alts.clear();
                quals.clear();
                filters.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !chroms.is_empty() {
            let batch = build_record_batch(
                Arc::clone(&schema.clone()),
                &chroms,
                &poss,
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}


async fn get_stream(file_path: String, schema_ref: SchemaRef, batch_size: usize) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.
    let schema = Arc::clone(&schema_ref);
    let file_path = file_path.clone();



    let store_type = get_storage_type(file_path.clone());
    let stream = match store_type {
        StorageType::GCS => get_remote_vcf_stream(file_path.clone(), schema.clone(), batch_size).await?,
        _ => panic!("Unsupported storage type")
    };


    // let stream = reader.records().into_stream().chunks(batch_size).map(move |chunk| {
    //         for r in chunk {
    //             let record = r.unwrap();
    //             chroms.push(record.reference_sequence_name().to_string());
    //             poss.push(record.variant_start().unwrap().unwrap().get() as u32 + 1);
    //             ids.push(".".to_string());
    //             refs.push(record.reference_bases().to_string());
    //             alts.push(".".to_string());
    //             quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
    //             filters.push(".".to_string());
    //             count += 1;
    //         };
    //         println!("Batch number: {}", batch_num);
    //         let batch = build_record_batch(Arc::clone(&schema), &chroms, &poss, &ids, &refs, &alts, &quals, &filters)?;
    //         count = 0;
    //         chroms.clear();
    //         poss.clear();
    //         ids.clear();
    //         refs.clear();
    //         alts.clear();
    //         quals.clear();
    //         filters.clear();
    //         batch_num += 1;
    //         Ok(batch)
    //     }
    // );

    // let stream = records.m

    // let iter = std::iter::from_fn(move  || {
    //     let mut records = reader.records()
    //     while count < batch_size {
    //         let record = records.next();
    //         record_num += 1;
    //         println!("Record number: {}", record_num);
    //         let record = block_on(record);
    //         if record.is_none() {
    //             break;
    //         }
    //         let record = record.unwrap().unwrap();
    //         // For each record, fill the fixed columns.
    //         chroms.push(record.reference_sequence_name().to_string());
    //         poss.push(record.variant_start().unwrap().unwrap().get() as u32 + 1);
    //         ids.push(".".to_string());
    //         refs.push(record.reference_bases().to_string());
    //         alts.push(".".to_string());
    //         quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
    //         filters.push(".".to_string());
    //         count += 1;
    //     }
    //     if count == 0 {
    //         return None;
    //     }
    //     println!("Batch number: {}", batch_num);
    //     let batch = build_record_batch(Arc::clone(&schema), &chroms, &poss, &ids, &refs, &alts, &quals, &filters).unwrap();
    //     count = 0;
    //     chroms.clear();
    //     poss.clear();
    //     ids.clear();
    //     refs.clear();
    //     alts.clear();
    //     quals.clear();
    //     filters.clear();
    //     batch_num += 1;
    //     Some(Ok(batch))
    // });
    // let stream = stream::iter(iter);
    let schema = schema_ref.clone();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))


}


// async fn get_stream(file_path: String, schema_ref: SchemaRef, batch_size: usize) -> datafusion::error::Result<SendableRecordBatchStream> {
//     // Open the BGZF-indexed VCF using IndexedReader.
//     let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
//     let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
//     let mut ids: Vec<String> = Vec::with_capacity(batch_size);
//     let mut refs: Vec<String> = Vec::with_capacity(batch_size);
//     let mut alts: Vec<String> = Vec::with_capacity(batch_size);
//     let mut quals: Vec<f64> = Vec::with_capacity(batch_size);
//     let mut filters: Vec<String> = Vec::with_capacity(batch_size);
//     let mut count: usize = 0;
//     let mut record_num = 0;
//     let mut batch_num = 0;
//     let schema = Arc::clone(&schema_ref);
//     let file_path = file_path.clone();
//     let mut reader = match file_path {
//         file_path if file_path.starts_with("gs://") | file_path.starts_with("s3://") => Reader::from_url(&Url::parse(&*file_path.clone()).unwrap()),
//         _ => Reader::from_path(file_path.clone())
//     }.unwrap();
//     reader.set_threads(1);
//     let mut reader = Arc::new(Mutex::new(reader));
//     let iter = std::iter::from_fn(move || {
//         let mut reader = reader.lock().unwrap();
//         let header = reader.header().clone();
//         let mut records = reader.records();
//         while count < batch_size {
//             let record = records.next();
//             record_num += 1;
//            if  record.is_none() {
//                break;
//            }
//             let record = record.unwrap().unwrap();
//             let contig_id = header.rid2name(record.rid().unwrap()).unwrap();
//             // For each record, fill the fixed columns.
//             chroms.push(String::from_utf8_lossy(contig_id).to_string());
//             poss.push(record.pos() as u32 + 1);
//             ids.push(".".to_string());
//             let alleles = record.alleles();
//             if let Some(r) = alleles.get(0) {
//                 refs.push(String::from_utf8_lossy(r).to_string());
//             } else {
//                 refs.push("".to_string());
//             }
//             if alleles.len() > 1 {
//                 let alt_vec: Vec<String> = alleles.iter().skip(1)
//                     .map(|a| String::from_utf8_lossy(a).to_string())
//                     .collect();
//                 alts.push(alt_vec.join(","));
//             } else {
//                 alts.push("".to_string());
//             }
//             quals.push(record.qual() as f64);
//             filters.push(if record.filters().count() == 0 { "PASS".to_string() } else { ".".to_string() });
//             count += 1;
//         }
//         if count == 0 {
//             return None;
//         }
//         println!("Batch number: {}", batch_num);
//         let batch = build_record_batch(Arc::clone(&schema), &chroms, &poss, &ids, &refs, &alts, &quals, &filters).unwrap();
//         count = 0;
//         chroms.clear();
//         poss.clear();
//         ids.clear();
//         refs.clear();
//         alts.clear();
//         quals.clear();
//         filters.clear();
//         batch_num += 1;
//         Some(Ok(batch))
//     });
//     let stream = stream::iter(iter);
//     Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
// }


pub struct VcfExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) info_fields: Vec<String>,
    pub(crate) format_fields: Vec<String>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
}


impl VcfExec {
    pub fn new(
        file_path: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        info_fields: Vec<String>,
        format_fields: Vec<String>,
        cache: PlanProperties,
        limit: Option<usize>,
    ) -> Self {
        Self {
            file_path: file_path.clone(),
            schema,
            projection,
            info_fields,
            format_fields,
            cache,
            limit,
        }
    }
}


impl Debug for VcfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}



impl DisplayAs for VcfExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }

}


impl ExecutionPlan for VcfExec {
    fn name(&self) -> &str {
        "VCFExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }



    fn execute(&self, _partition: usize, context: Arc<TaskContext>) -> datafusion::common::Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();
        let fut = get_stream(self.file_path.clone(), schema.clone(), batch_size);
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))

    }
    
}
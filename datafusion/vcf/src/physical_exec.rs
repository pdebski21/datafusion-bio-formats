use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int32Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{stream, StreamExt, TryStreamExt};
use std::{str};
use std::fs::File;
use std::hash::Hasher;
use std::io::Error;
use std::num::NonZero;
use std::ops::Deref;
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::ipc::FieldBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use env_logger::builder;
use log::{debug, info};
use noodles::vcf;
use noodles::vcf::Header;
use noodles::vcf::header::Infos;
use noodles::vcf::header::record::value::map::Info;
use noodles::vcf::header::record::value::map::info::{Number, Type};
use noodles::vcf::io::Reader;
use noodles::vcf::variant::Record;
use noodles::vcf::variant::record::info::field::{Value,value::Array as ValueArray};
use noodles_bgzf::MultithreadedReader;
use crate::storage::{get_local_vcf_reader, get_remote_stream_bgzf, get_remote_vcf_reader, get_storage_type, StorageType};
use crate::table_provider::{info_to_arrow_type, OptionalField};

fn build_record_batch(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    pose: &[u32],
    ids: &[String],
    refs: &[String],
    alts: &[String],
    quals: &[f64],
    filters: &[String],
    infos: Option<&Vec<Arc<dyn Array>>>,

) -> datafusion::error::Result<RecordBatch> {
    let chrom_array = Arc::new(StringArray::from(chroms.to_vec())) as Arc<dyn Array>;
    let pos_start_array = Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>;
    let pos_end_array = Arc::new(UInt32Array::from(pose.to_vec())) as Arc<dyn Array>;
    let id_array = Arc::new(StringArray::from(ids.to_vec())) as Arc<dyn Array>;
    let ref_array = Arc::new(StringArray::from(refs.to_vec())) as Arc<dyn Array>;
    let alt_array = Arc::new(StringArray::from(alts.to_vec())) as Arc<dyn Array>;
    let qual_array = Arc::new(Float64Array::from(quals.to_vec())) as Arc<dyn Array>;
    let filter_array = Arc::new(StringArray::from(filters.to_vec())) as Arc<dyn Array>;

    let mut arrays: Vec<Arc<dyn Array>> = vec![
        chrom_array, pos_start_array, pos_end_array, id_array, ref_array, alt_array, qual_array, filter_array,
    ];
    arrays.append(&mut infos.unwrap().clone());
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
}



fn load_infos (record: Box<dyn Record>, header: &Header, info_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>)) {
    for i in 0..info_builders.2.len() {
        let name = &info_builders.0[i];
        let data_type = &info_builders.1[i];
        let builder = &mut info_builders.2[i];
        let info = record.info();
        let value = info.get(&header, name);

        match value {
            Some(Ok(v)) => {
                match v {
                    Some(Value::Integer(v)) => {
                        builder.append_int(v);
                    },
                    Some(Value::Array(ValueArray::Integer(values))) => {
                        builder.append_array_int(values.iter().map(|v| v.unwrap().unwrap()).collect());
                    }
                    Some(Value::Array(ValueArray::Float(values))) => {
                        builder.append_array_float(values.iter().map(|v| v.unwrap().unwrap()).collect());
                    }

                    Some(Value::Float(v)) => {
                        builder.append_float(v);

                    }
                    Some(Value::String(v)) => {
                        builder.append_string(&v);
                    }
                    Some(Value::Array(ValueArray::String(values))) => {
                        builder.append_array_string(values.iter().map(|v| v.unwrap().unwrap().to_string()).collect());
                    }
                    Some(Value::Flag) => {
                        builder.append_boolean(true);
                    }
                    None => {
                        if data_type == &DataType::Boolean {
                            builder.append_boolean(false);
                        } else {
                            builder.append_null();
                        }
                    },
                    _ => panic!("Unsupported value type"),
                }
            }

            _ => {

                if data_type == &DataType::Boolean {
                    builder.append_boolean(false);
                } else {
                    builder.append_null();
                }
            }
        }


    }

}

fn builders_to_arrays(builders: &mut Vec<OptionalField>) -> Vec<Arc<dyn Array>> {
    builders.iter_mut().map(|f| f.finish()).collect::<Vec<Arc<dyn Array>>>()
}


async fn get_local_vcf(file_path: String, schema_ref: SchemaRef, batch_size: usize, thread_num: Option<usize>, info_fields: Option<Vec<String>>)  -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>> {
    let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
    let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
    let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
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
    let thread_num = thread_num.unwrap_or(1);
    let mut reader = get_local_vcf_reader(file_path, thread_num)?;
    let header = reader.read_header()?;
    let infos = header.infos();

    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) = (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields, &infos, &mut info_builders);

    let iter  = std::iter::from_fn(move || {

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
            poss.push(record.variant_start().unwrap().unwrap().get() as u32);
            pose.push(record.variant_end(&header).unwrap().get() as u32);
            ids.push(".".to_string());
            refs.push(record.reference_bases().to_string());
            alts.push(".".to_string());
            quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
            filters.push(".".to_string());
            load_infos(Box::new(record), &header, &mut info_builders);
            count += 1;
        }
        if count == 0 {
            return None;
        }
        debug!("Batch number: {}", batch_num);
        let batch = build_record_batch(Arc::clone(&schema), &chroms, &poss, &pose, &ids, &refs, &alts, &quals, &filters, Some(&builders_to_arrays(&mut info_builders.2))).unwrap();
        count = 0;
        chroms.clear();
        poss.clear();
        pose.clear();
        ids.clear();
        refs.clear();
        alts.clear();
        quals.clear();
        filters.clear();
        batch_num += 1;
        Some(Ok(batch))
    });
    Ok(stream::iter(iter))
}



async fn get_remote_vcf_stream(file_path: String, schema: SchemaRef, batch_size: usize, info_fields: Option<Vec<String>>) -> datafusion::error::Result<AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output=()> + Sized>> {
    let mut reader = get_remote_vcf_reader(file_path.clone()).await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) = (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields, &infos, &mut info_builders);

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
        let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
        let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
        let mut ids: Vec<String> = Vec::with_capacity(batch_size);
        let mut refs: Vec<String> = Vec::with_capacity(batch_size);
        let mut alts: Vec<String> = Vec::with_capacity(batch_size);
        let mut quals: Vec<f64> = Vec::with_capacity(batch_size);
        let mut filters: Vec<String> = Vec::with_capacity(batch_size);
        // add infos fields vector of vectors of different types

        debug!("Info fields: {:?}", info_builders);

        let mut record_num = 0;
        let mut batch_num = 0;


        // Process records one by one.
        let mut records = reader.records();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            poss.push(record.variant_start().unwrap()?.get() as u32);
            pose.push(record.variant_end(&header)?.get() as u32);
            ids.push(".".to_string());
            refs.push(record.reference_bases().to_string());
            alts.push(".".to_string());
            quals.push(record.quality_score().unwrap_or(Ok(0.0))? as f64);
            filters.push(".".to_string());
            load_infos(Box::new(record), &header, &mut info_builders);
            record_num += 1;



            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    Some(&builders_to_arrays(&mut info_builders.2)),
                    // if infos.is_empty() { None } else { Some(&infos) },

                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                pose.clear();
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
                &pose,
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
                Some(&builders_to_arrays(&mut info_builders.2))
                // if infos.is_empty() { None } else { Some(&infos) },
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

fn set_info_builders(batch_size: usize, info_fields: Option<Vec<String>>, infos: &Infos, info_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>)) {
    for f in info_fields.unwrap_or(Vec::new()) {
        let data_type = info_to_arrow_type(&infos, &f);
        let field = OptionalField::new(&data_type, batch_size);
        info_builders.0.push(f);
        info_builders.1.push(data_type);
        info_builders.2.push(field);
    }
}

async fn get_stream(file_path: String, schema_ref: SchemaRef, batch_size: usize, thread_num: Option<usize>, info_fields: Option<Vec<String>>) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_vcf(file_path.clone(), schema.clone(), batch_size, thread_num, info_fields).await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        },
        StorageType::GCS | StorageType::S3=> {
            let stream = get_remote_vcf_stream(file_path.clone(), schema.clone(), batch_size, info_fields).await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        },
        _ => panic!("Unsupported storage type")
    }

}



pub struct VcfExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) info_fields: Option<Vec<String>>,
    pub(crate) format_fields: Option<Vec<String>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) thread_num: Option<usize>,
}


impl VcfExec {
    pub fn new(
        file_path: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        cache: PlanProperties,
        limit: Option<usize>,
        thread_num: Option<usize>,
    ) -> Self {
        Self {
            file_path: file_path.clone(),
            schema,
            projection,
            info_fields,
            format_fields,
            cache,
            limit,
            thread_num,
        }
    }
}


impl Debug for VcfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}



impl DisplayAs for VcfExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        Ok(())
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
        let fut = get_stream(self.file_path.clone(), schema.clone(), batch_size, self.thread_num, self.info_fields.clone());
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))

    }
    
}
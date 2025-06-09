use datafusion::arrow;
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder,
    MapBuilder, StringBuilder, StructBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::error::ArrowError;
use std::sync::Arc;

#[derive(Debug)]
pub enum OptionalField {
    Int32Builder(Int32Builder),
    ArrayInt32Builder(ListBuilder<Int32Builder>),
    Float32Builder(Float32Builder),
    ArrayFloat32Builder(ListBuilder<Float32Builder>),
    BooleanBuilder(BooleanBuilder),
    ArrayBooleanBuilder(ListBuilder<BooleanBuilder>),
    Utf8Builder(StringBuilder),
    ArrayUtf8Builder(ListBuilder<StringBuilder>),
    StructBuilder(StructBuilder), //
}

impl OptionalField {
    pub fn new(data_type: &DataType, batch_size: usize) -> Result<OptionalField, ArrowError> {
        match data_type {
            DataType::Struct(..) => {
                let tag_builder = StringBuilder::with_capacity(batch_size, batch_size);
                let value_builder = StringBuilder::with_capacity(batch_size, batch_size);
                let fields = vec![
                    Field::new("tag", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ];
                Ok(OptionalField::StructBuilder(StructBuilder::new(
                    fields.clone(),
                    vec![
                        Box::new(tag_builder) as Box<dyn ArrayBuilder>,
                        Box::new(value_builder) as Box<dyn ArrayBuilder>,
                    ],
                )))
            }

            DataType::Int32 => Ok(OptionalField::Int32Builder(Int32Builder::with_capacity(
                batch_size,
            ))),
            DataType::Float32 => Ok(OptionalField::Float32Builder(
                Float32Builder::with_capacity(batch_size),
            )),
            DataType::Utf8 => Ok(OptionalField::Utf8Builder(StringBuilder::with_capacity(
                batch_size,
                batch_size * 10,
            ))),
            DataType::Boolean => Ok(OptionalField::BooleanBuilder(
                BooleanBuilder::with_capacity(batch_size),
            )),

            DataType::List(f) => match f.data_type() {
                DataType::Int32 => Ok(OptionalField::ArrayInt32Builder(
                    ListBuilder::with_capacity(Int32Builder::with_capacity(batch_size), batch_size),
                )),
                DataType::Float32 => Ok(OptionalField::ArrayFloat32Builder(
                    ListBuilder::with_capacity(
                        Float32Builder::with_capacity(batch_size),
                        batch_size,
                    ),
                )),
                DataType::Utf8 => Ok(OptionalField::ArrayUtf8Builder(ListBuilder::with_capacity(
                    StringBuilder::with_capacity(batch_size, batch_size * 10),
                    batch_size,
                ))),
                DataType::Boolean => Ok(OptionalField::ArrayBooleanBuilder(
                    ListBuilder::with_capacity(
                        BooleanBuilder::with_capacity(batch_size),
                        batch_size,
                    ),
                )),
                _ => Err(ArrowError::SchemaError(
                    "Unsupported list inner data type".into(),
                )),
            },

            _ => Err(ArrowError::SchemaError("Unsupported data type".into())),
        }
    }

    pub fn append_struct(&mut self, key: &str, value: &str) -> Result<(), ArrowError> {
        match self {
            OptionalField::StructBuilder(builder) => {
                builder
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(key);
                builder
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(value);
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected StructBuilder".into())),
        }
    }
    pub fn append_int(&mut self, value: i32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Invalid builder".into())),
        }
    }

    pub fn append_boolean(&mut self, value: bool) -> Result<(), ArrowError> {
        match self {
            OptionalField::BooleanBuilder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected BooleanBuilder".into())),
        }
    }

    pub fn append_array_int(&mut self, value: Vec<i32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                builder.values().append_slice(&value);
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayInt32Builder".into())),
        }
    }

    pub fn append_float(&mut self, value: f32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Float32Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected Float32Builder".into())),
        }
    }

    pub fn append_array_float(&mut self, value: Vec<f32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                builder.values().append_slice(&value);
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError(
                "Expected ArrayFloat32Builder".into(),
            )),
        }
    }

    pub fn append_string(&mut self, value: &str) -> Result<(), ArrowError> {
        match self {
            OptionalField::Utf8Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected Utf8Builder".into())),
        }
    }

    pub fn append_array_string(&mut self, value: Vec<String>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in value {
                    builder.values().append_value(&v);
                }
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayUtf8Builder".into())),
        }
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayInt32Builder(builder) => Ok(builder.append_null()),
            OptionalField::Utf8Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayUtf8Builder(builder) => Ok(builder.append_null()),
            OptionalField::Float32Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayFloat32Builder(builder) => Ok(builder.append_null()),
            OptionalField::BooleanBuilder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayBooleanBuilder(builder) => Ok(builder.append_null()),
            OptionalField::StructBuilder(builder) => {
                builder.append_null();
                Ok(())
            }
        }
    }

    pub fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayInt32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::Utf8Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayUtf8Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::Float32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayFloat32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::BooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayBooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::StructBuilder(builder) => {
                let struct_array = builder.finish();
                Ok(Arc::new(struct_array))
            }
        }
    }
}

pub fn builders_to_arrays(builders: &mut Vec<OptionalField>) -> Vec<Arc<dyn Array>> {
    builders
        .iter_mut()
        .map(|f| f.finish())
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

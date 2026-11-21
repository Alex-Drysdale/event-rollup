//! Parquet output through the parquet crate's low-level writer: no
//! compression, no dictionary, no statistics, one row group per file.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

pub enum Col {
    Str(Vec<ByteArray>),
    OptStr(Vec<ByteArray>, Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_offset_index_disabled(true)
            .set_max_row_group_row_count(None)
            .set_data_page_size_limit(usize::MAX / 4)
            .set_write_batch_size(1 << 20)
            .build(),
    )
}

/// Writes one file whose columns appear in `schema` in the same order as `cols`.
pub fn write(path: &Path, schema: &str, cols: Vec<Col>) {
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    let file = BufWriter::with_capacity(1 << 22, File::create(path).expect("create parquet"));
    let mut w = SerializedFileWriter::new(file, schema, props()).expect("writer");
    let mut rg = w.next_row_group().expect("row group");
    let mut cols = cols.into_iter();
    while let Some(mut cw) = rg.next_column().expect("column") {
        match cols.next().expect("column count") {
            Col::Str(v) => {
                cw.typed::<ByteArrayType>().write_batch(&v, None, None).expect("write");
            }
            Col::OptStr(v, def) => {
                cw.typed::<ByteArrayType>().write_batch(&v, Some(&def), None).expect("write");
            }
            Col::I32(v) => {
                cw.typed::<Int32Type>().write_batch(&v, None, None).expect("write");
            }
            Col::I64(v) => {
                cw.typed::<Int64Type>().write_batch(&v, None, None).expect("write");
            }
        }
        cw.close().expect("close column");
    }
    rg.close().expect("close row group");
    let inner = w.into_inner().expect("finish");
    inner.into_inner().ok().expect("flush");
}

pub const USER_SCHEMA: &str = "message user_daily {
  REQUIRED BYTE_ARRAY user_id (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

pub const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
  REQUIRED BYTE_ARRAY campaign_id (UTF8);
  REQUIRED BYTE_ARRAY channel (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}";

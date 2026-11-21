//! A minimal Parquet writer: one row group, one uncompressed PLAIN data page
//! (v1) per column, no dictionary, no statistics. The footer is Thrift
//! compact protocol, written by hand.
//!
//! Columns are REQUIRED except `OptStr`, whose definition levels are written
//! as RLE/bit-packed-hybrid bit-packed runs of width 1.

use std::fs::File;
use std::io::Write;
use std::path::Path;

pub enum Kind {
    Str,
    OptStr,
    Date,
    I32,
    I64,
}

/// One column's values, already PLAIN-encoded.
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    /// PLAIN values (non-null only for OptStr).
    pub data: Vec<u8>,
    /// For OptStr: one bit per row, 1 = present, LSB first.
    pub present: Vec<u8>,
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, rows: usize) -> Column {
        let per = match kind {
            Kind::Str | Kind::OptStr => 14,
            Kind::Date | Kind::I32 => 4,
            Kind::I64 => 8,
        };
        Column { name, kind, data: Vec::with_capacity(rows * per), present: Vec::new() }
    }
    #[inline(always)]
    pub fn str(&mut self, s: &[u8]) {
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.data.extend_from_slice(s);
    }
    /// For OptStr: row `i` (rows pushed in order) is `s` or null.
    #[inline(always)]
    pub fn opt_str(&mut self, i: usize, s: Option<&[u8]>) {
        if i % 8 == 0 {
            self.present.push(0);
        }
        if let Some(s) = s {
            *self.present.last_mut().unwrap() |= 1 << (i % 8);
            self.str(s);
        }
    }
    #[inline(always)]
    pub fn i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
    #[inline(always)]
    pub fn i64(&mut self, v: i64) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
}

// Thrift compact protocol.
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Thrift {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        *last = id;
        if (1..=15).contains(&delta) {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    /// Starts a struct as field `id` (or as a list element when `id` is None).
    fn begin(&mut self, id: Option<i16>) {
        if let Some(id) = id {
            self.field(id, T_STRUCT);
        }
        self.last.push(0);
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
}

// Parquet enums.
const TYPE_INT32: i32 = 1;
const TYPE_INT64: i32 = 2;
const TYPE_BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CONVERTED_UTF8: i32 = 0;
const CONVERTED_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

fn physical(k: &Kind) -> i32 {
    match k {
        Kind::Str | Kind::OptStr => TYPE_BYTE_ARRAY,
        Kind::Date | Kind::I32 => TYPE_INT32,
        Kind::I64 => TYPE_INT64,
    }
}

/// Writes a Parquet file with `rows` rows.
pub fn write(path: &Path, rows: usize, cols: &[Column]) {
    let mut file: Vec<u8> = Vec::with_capacity(cols.iter().map(|c| c.data.len() + c.present.len() + 64).sum::<usize>() + 4096);
    file.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (data_page_offset, total size)
    for c in cols {
        let mut levels = Vec::new();
        if let Kind::OptStr = c.kind {
            // Bit-packed run of ceil(rows / 8) groups of 8 values, width 1.
            let groups = rows.div_ceil(8);
            let mut run = Thrift::new();
            run.varint(((groups as u64) << 1) | 1);
            let mut body = run.buf;
            body.extend_from_slice(&c.present[..groups]);
            levels.extend_from_slice(&(body.len() as u32).to_le_bytes());
            levels.extend_from_slice(&body);
        }
        let page_size = levels.len() + c.data.len();
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page_size as i32);
        h.i32(3, page_size as i32);
        h.begin(Some(5));
        h.i32(1, rows as i32);
        h.i32(2, ENC_PLAIN);
        h.i32(3, ENC_RLE);
        h.i32(4, ENC_RLE);
        h.end();
        h.buf.push(0);
        let offset = file.len();
        file.extend_from_slice(&h.buf);
        file.extend_from_slice(&levels);
        file.extend_from_slice(&c.data);
        chunks.push((offset, file.len() - offset));
    }

    let mut m = Thrift::new();
    m.i32(1, 1);
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin(None);
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin(None);
        m.i32(1, physical(&c.kind));
        m.i32(3, if let Kind::OptStr = c.kind { OPTIONAL } else { REQUIRED });
        m.bin(4, c.name.as_bytes());
        match c.kind {
            Kind::Str | Kind::OptStr => {
                m.i32(6, CONVERTED_UTF8);
                m.begin(Some(10)); // LogicalType
                m.begin(Some(1)); // STRING
                m.end();
                m.end();
            }
            Kind::Date => {
                m.i32(6, CONVERTED_DATE);
                m.begin(Some(10));
                m.begin(Some(6)); // DATE
                m.end();
                m.end();
            }
            Kind::I32 | Kind::I64 => {}
        }
        m.end();
    }
    m.i64(3, rows as i64);
    m.list(4, T_STRUCT, 1);
    m.begin(None);
    m.list(1, T_STRUCT, cols.len());
    let total: usize = chunks.iter().map(|c| c.1).sum();
    for (c, &(offset, size)) in cols.iter().zip(&chunks) {
        m.begin(None);
        m.i64(2, offset as i64);
        m.begin(Some(3));
        m.i32(1, physical(&c.kind));
        m.list(2, T_I32, 2);
        m.zigzag(ENC_PLAIN as i64);
        m.zigzag(ENC_RLE as i64);
        m.list(3, T_BINARY, 1);
        m.varint(c.name.len() as u64);
        m.buf.extend_from_slice(c.name.as_bytes());
        m.i32(4, 0); // UNCOMPRESSED
        m.i64(5, rows as i64);
        m.i64(6, size as i64);
        m.i64(7, size as i64);
        m.i64(9, offset as i64);
        m.end();
        m.end();
    }
    m.i64(2, total as i64);
    m.i64(3, rows as i64);
    m.end();
    m.bin(6, b"event-rollup");
    m.buf.push(0);
    file.extend_from_slice(&m.buf);
    file.extend_from_slice(&(m.buf.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    let t = std::time::Instant::now();
    File::create(path).and_then(|mut f| f.write_all(&file)).expect("write parquet");
    if std::env::var_os("SS_DEBUG").is_some() {
        eprintln!("  write_all {} bytes: {:?}", file.len(), t.elapsed());
    }
}

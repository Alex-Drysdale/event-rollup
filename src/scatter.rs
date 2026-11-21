//! Partition buffers made of fixed-size chunks, and a radix scatter into them.
//!
//! Every partition is a list of chunks of `CHUNK_BYTES` taken from a `Pool`:
//! a bump allocator over large anonymous mappings (huge pages advised) with a
//! free list in front. Nothing is over-reserved and nothing is copied when a
//! partition grows, and the chunks of a partition that has been consumed can
//! be given to a pool and filled again by the next phase, so the same pages
//! are not faulted in twice. Chunks are never returned to the system.
//!
//! `Scatter` adds software write-combining: records for each partition are
//! staged in a small buffer (a few cache lines, all buffers together fit in
//! L2) and flushed as whole cache lines with non-temporal stores, so the
//! destination lines are never read before being written.

use std::marker::PhantomData;

pub const CHUNK_BYTES: usize = 16 << 10;
/// Bytes mapped at a time.
const BLOCK: usize = 32 << 20;
const HUGE: usize = 2 << 20;

pub struct Pool {
    free: Vec<*mut u8>,
    at: *mut u8,
    left: usize,
}

unsafe impl Send for Pool {}
// A shared pool is only ever looked at, never taken from.
unsafe impl Sync for Pool {}

impl Pool {
    pub fn new() -> Pool {
        Pool { free: Vec::new(), at: std::ptr::null_mut(), left: 0 }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let mut map = memmap2::MmapOptions::new().len(BLOCK + HUGE).map_anon().expect("map chunk memory");
        // Harmless where transparent huge pages are off.
        #[cfg(target_os = "linux")]
        let _ = map.advise(memmap2::Advice::HugePage);
        let p = map.as_mut_ptr();
        self.at = unsafe { p.add(p.align_offset(HUGE)) };
        self.left = BLOCK;
        std::mem::forget(map);
    }

    /// A chunk of `CHUNK_BYTES`, aligned to at least 4096; its contents are arbitrary.
    #[inline]
    pub fn take(&mut self) -> *mut u8 {
        if let Some(p) = self.free.pop() {
            return p;
        }
        if self.left < CHUNK_BYTES {
            self.grow();
        }
        let p = self.at;
        self.at = unsafe { p.add(CHUNK_BYTES) };
        self.left -= CHUNK_BYTES;
        p
    }

    /// Hands a chunk that nothing reads any more to this pool.
    #[inline]
    pub fn give(&mut self, p: *mut u8) {
        self.free.push(p);
    }
}

/// A list of chunks holding records of type `T`; all but the last are full.
pub struct Chunks<T> {
    list: Vec<*mut T>,
    /// The last chunk, and the number of records in it.
    cur: *mut T,
    n: usize,
    _own: PhantomData<T>,
}

unsafe impl<T: Send> Send for Chunks<T> {}
unsafe impl<T: Sync> Sync for Chunks<T> {}

impl<T: Copy> Chunks<T> {
    /// Records per chunk.
    pub const CAP: usize = CHUNK_BYTES / std::mem::size_of::<T>();

    pub fn new() -> Chunks<T> {
        assert!(std::mem::align_of::<T>() <= 4096 && Self::CAP > 0);
        Chunks { list: Vec::new(), cur: std::ptr::null_mut(), n: Self::CAP, _own: PhantomData }
    }

    pub fn len(&self) -> usize {
        if self.list.is_empty() { 0 } else { (self.list.len() - 1) * Self::CAP + self.n }
    }

    /// Room for at least one more record in the last chunk.
    #[inline(always)]
    fn room(&mut self, pool: &mut Pool) {
        if self.n == Self::CAP {
            self.cur = pool.take() as *mut T;
            self.list.push(self.cur);
            self.n = 0;
        }
    }

    #[inline(always)]
    pub fn push(&mut self, pool: &mut Pool, rec: T) {
        self.room(pool);
        unsafe { self.cur.add(self.n).write(rec) };
        self.n += 1;
    }

    /// The records, chunk by chunk.
    pub fn slices(&self) -> impl Iterator<Item = &[T]> + Clone {
        let last = self.list.len().wrapping_sub(1);
        self.list
            .iter()
            .enumerate()
            .map(move |(i, &p)| unsafe { std::slice::from_raw_parts(p, if i == last { self.n } else { Self::CAP }) })
    }

    /// The chunks themselves, to give to a pool once the records are dead.
    pub fn raw(&self) -> impl Iterator<Item = *mut u8> + '_ {
        self.list.iter().map(|&p| p as *mut u8)
    }
}

pub struct Scatter<T: Copy> {
    /// Records per staging buffer; `b * size_of::<T>()` is a multiple of 64
    /// and `b` divides the records per chunk.
    b: usize,
    stage: Vec<T>,
    fill: Vec<u32>,
    pub outs: Vec<Chunks<T>>,
    pool: Pool,
}

impl<T: Copy> Scatter<T> {
    pub fn new(parts: usize) -> Scatter<T> {
        let size = std::mem::size_of::<T>();
        let mut b = 1;
        while (b * size) % 64 != 0 || b * size < 128 {
            b += 1;
        }
        assert!(Chunks::<T>::CAP % b == 0, "record size does not fit the chunks");
        let zero: T = unsafe { std::mem::zeroed() };
        Scatter {
            b,
            stage: vec![zero; parts * b],
            fill: vec![0; parts],
            outs: (0..parts).map(|_| Chunks::new()).collect(),
            pool: Pool::new(),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, rec: T) {
        let f = self.fill[p] as usize;
        unsafe { *self.stage.get_unchecked_mut(p * self.b + f) = rec };
        if f + 1 == self.b {
            self.flush_full(p);
            self.fill[p] = 0;
        } else {
            self.fill[p] = (f + 1) as u32;
        }
    }

    #[inline(always)]
    fn flush_full(&mut self, p: usize) {
        let b = self.b;
        let out = &mut self.outs[p];
        // `b` divides the chunk, so there is room for all `b` or for none.
        out.room(&mut self.pool);
        let src = unsafe { self.stage.as_ptr().add(p * b) } as *const u8;
        let dst = unsafe { out.cur.add(out.n) } as *mut u8;
        let bytes = b * std::mem::size_of::<T>();
        debug_assert!(dst as usize % 64 == 0);
        #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
        unsafe {
            use std::arch::x86_64::*;
            let mut i = 0;
            while i < bytes {
                let v = _mm512_loadu_si512(src.add(i) as *const _);
                _mm512_stream_si512(dst.add(i) as *mut _, v);
                i += 64;
            }
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512f")))]
        unsafe {
            std::ptr::copy_nonoverlapping(src, dst, bytes);
        }
        out.n += b;
    }

    /// Moves what is left in the staging buffers out, and fences the
    /// non-temporal stores. Call before handing `outs` to other threads.
    pub fn finish(&mut self) {
        for p in 0..self.fill.len() {
            let f = self.fill[p] as usize;
            let start = p * self.b;
            for i in start..start + f {
                self.outs[p].push(&mut self.pool, self.stage[i]);
            }
            self.fill[p] = 0;
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_keep_order_and_reuse() {
        let mut pool = Pool::new();
        let mut a: Chunks<[u64; 3]> = Chunks::new();
        let n = Chunks::<[u64; 3]>::CAP * 3 + 5;
        for i in 0..n as u64 {
            a.push(&mut pool, [i, !i, 7]);
        }
        assert_eq!(a.len(), n);
        let all: Vec<[u64; 3]> = a.slices().flatten().copied().collect();
        assert_eq!(all.len(), n);
        assert!(all.iter().enumerate().all(|(i, r)| *r == [i as u64, !(i as u64), 7]));
        assert_eq!(Chunks::<u32>::new().slices().count(), 0);
        // Chunks given back are handed out again before fresh ones.
        let given: Vec<*mut u8> = a.raw().collect();
        for &p in &given {
            pool.give(p);
        }
        let mut b: Chunks<u64> = Chunks::new();
        for i in 0..(Chunks::<u64>::CAP * 4) as u64 {
            b.push(&mut pool, i);
        }
        let mut got: Vec<*mut u8> = b.raw().collect();
        let mut want = given.clone();
        got.sort();
        want.sort();
        assert_eq!(got, want);
        assert!(b.slices().flatten().copied().eq(0..(Chunks::<u64>::CAP * 4) as u64));
    }

    #[test]
    fn scatter_keeps_every_record() {
        let parts = 5;
        let mut s: Scatter<[u64; 4]> = Scatter::new(parts);
        let n = Chunks::<[u64; 4]>::CAP * 7 + 3;
        for i in 0..n as u64 {
            s.push((i % 3 * 2) as usize % parts, [i, i, i, i]);
        }
        s.finish();
        let mut seen = 0;
        for (p, out) in s.outs.iter().enumerate() {
            let mut last = None;
            for r in out.slices().flatten() {
                assert_eq!((r[0] % 3 * 2) as usize % parts, p);
                assert!(last < Some(r[0]) && r[1] == r[0] && r[3] == r[0]);
                last = Some(r[0]);
                seen += 1;
            }
            assert_eq!(out.slices().map(|c| c.len()).sum::<usize>(), out.len());
        }
        assert_eq!(seen, n);
    }
}

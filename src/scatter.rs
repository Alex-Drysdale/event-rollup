//! Radix scatter with software write-combining. Records for each partition
//! are staged in a small buffer (a few cache lines, all buffers together fit
//! in L2) and flushed as whole cache lines with non-temporal stores, so the
//! destination lines are never read before being written.

pub struct Scatter<T: Copy> {
    /// Records per staging buffer; `B * size_of::<T>()` is a multiple of 64.
    b: usize,
    stage: Vec<T>,
    fill: Vec<u32>,
    pub outs: Vec<Vec<T>>,
}

impl<T: Copy> Scatter<T> {
    pub fn new(parts: usize, reserve: usize) -> Scatter<T> {
        let size = std::mem::size_of::<T>();
        let mut b = 1;
        while (b * size) % 64 != 0 || b * size < 128 {
            b += 1;
        }
        let zero: T = unsafe { std::mem::zeroed() };
        Scatter {
            b,
            stage: vec![zero; parts * b],
            fill: vec![0; parts],
            outs: (0..parts).map(|_| Vec::with_capacity(reserve.max(b))).collect(),
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
        out.reserve(b);
        let src = unsafe { self.stage.as_ptr().add(p * b) } as *const u8;
        let len = out.len();
        let dst = unsafe { out.as_mut_ptr().add(len) } as *mut u8;
        let bytes = b * std::mem::size_of::<T>();
        #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
        if (dst as usize) % 64 == 0 {
            unsafe {
                use std::arch::x86_64::*;
                let mut i = 0;
                while i < bytes {
                    let v = _mm512_loadu_si512(src.add(i) as *const _);
                    _mm512_stream_si512(dst.add(i) as *mut _, v);
                    i += 64;
                }
                out.set_len(len + b);
            }
            return;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src, dst, bytes);
            out.set_len(len + b);
        }
    }

    /// Moves what is left in the staging buffers out, and fences the
    /// non-temporal stores. Call before handing `outs` to other threads.
    pub fn finish(&mut self) {
        for p in 0..self.fill.len() {
            let f = self.fill[p] as usize;
            let start = p * self.b;
            self.outs[p].extend_from_slice(&self.stage[start..start + f]);
            self.fill[p] = 0;
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }
}

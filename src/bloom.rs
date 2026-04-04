//! Minimal Bloom filter for fast probabilistic membership testing.
//!
//! Uses the Kirsch-Mitzenmacher double-hashing optimization to simulate k hash
//! functions from two base hashes, avoiding k independent hash computations.

// Items are pub(crate) and consumed by future modules; suppress dead-code lint
// until the integration point is wired up.
#![allow(dead_code)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A space-efficient probabilistic membership set.
///
/// `test` returning `false` means the item is **definitely absent**.
/// `test` returning `true` means the item is **probably present** (FPR applies).
pub(crate) struct BloomFilter {
    bits: Vec<u64>,
    /// Number of 64-bit words (bits.len())
    num_words: usize,
    /// Total bit capacity (m)
    num_bits: usize,
    /// Number of hash functions (k)
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a new, empty `BloomFilter` sized for `capacity` items at the
    /// target false-positive rate `fpr` (e.g. `0.01` for 1 %).
    pub(crate) fn with_capacity(capacity: usize, fpr: f64) -> Self {
        let (num_bits, num_hashes) = optimal_params(capacity, fpr);
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_words,
            num_bits,
            num_hashes,
        }
    }

    /// Build a `BloomFilter` pre-populated from an iterator of byte slices.
    pub(crate) fn from_iter<'a>(
        capacity: usize,
        fpr: f64,
        iter: impl Iterator<Item = &'a [u8]>,
    ) -> Self {
        let mut filter = Self::with_capacity(capacity, fpr);
        for item in iter {
            filter.insert(item);
        }
        filter
    }

    /// Insert `item` into the filter.
    pub(crate) fn insert(&mut self, item: &[u8]) {
        let (h1, h2) = base_hashes(item);
        for i in 0..self.num_hashes {
            let bit = kirsch_mitzenmacher(h1, h2, i, self.num_bits);
            self.bits[bit / 64] |= 1u64 << (bit % 64);
        }
    }

    /// Test membership. Returns `false` if the item is definitely absent,
    /// `true` if the item is probably present.
    pub(crate) fn test(&self, item: &[u8]) -> bool {
        let (h1, h2) = base_hashes(item);
        (0..self.num_hashes).all(|i| {
            let bit = kirsch_mitzenmacher(h1, h2, i, self.num_bits);
            self.bits[bit / 64] & (1u64 << (bit % 64)) != 0
        })
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Compute optimal (m, k) from n and target FPR p.
///
/// m = ceil(-n * ln(p) / ln(2)^2)
/// k = round((m / n) * ln(2))
fn optimal_params(n: usize, fpr: f64) -> (usize, usize) {
    let n = n.max(1) as f64;
    let p = fpr.clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON);
    let ln2 = std::f64::consts::LN_2;
    let m = (-n * p.ln() / (ln2 * ln2)).ceil() as usize;
    let m = m.max(64); // at least one word
    let k = ((m as f64 / n) * ln2).round() as usize;
    let k = k.max(1);
    (m, k)
}

/// Two independent hashes of `data` using `DefaultHasher` with distinct seeds.
fn base_hashes(data: &[u8]) -> (u64, u64) {
    let mut h = DefaultHasher::new();
    data.hash(&mut h);
    let h1 = h.finish();

    let mut h = DefaultHasher::new();
    // Mix with a seed to get an independent second hash.
    h1.hash(&mut h);
    data.hash(&mut h);
    let h2 = h.finish();

    (h1, h2)
}

/// Kirsch-Mitzenmacher: bit index for the i-th hash function.
#[inline]
fn kirsch_mitzenmacher(h1: u64, h2: u64, i: usize, m: usize) -> usize {
    h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize % m
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_insert_and_test() {
        let mut bf = BloomFilter::with_capacity(100, 0.01);
        bf.insert(b"hello");
        bf.insert(b"world");

        assert!(bf.test(b"hello"), "hello must be present");
        assert!(bf.test(b"world"), "world must be present");
        assert!(!bf.test(b"absent_key"), "absent_key must not be present");
    }

    #[test]
    fn bloom_from_iter() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let bf = BloomFilter::from_iter(10, 0.01, items.into_iter());

        assert!(bf.test(b"a"));
        assert!(bf.test(b"b"));
        assert!(bf.test(b"c"));
        // "d" was never inserted — must not be a false positive here
        // (with k≥1 and a well-sized filter it almost certainly won't be)
        let _ = bf.test(b"d"); // no assertion; probabilistic result accepted
    }

    #[test]
    fn bloom_fpr_within_bounds() {
        const N: usize = 10_000;
        const TARGET_FPR: f64 = 0.01;

        let mut bf = BloomFilter::with_capacity(N, TARGET_FPR);
        for i in 0..N {
            bf.insert(format!("item_{i}").as_bytes());
        }

        let mut false_positives = 0usize;
        for i in N..(2 * N) {
            if bf.test(format!("item_{i}").as_bytes()) {
                false_positives += 1;
            }
        }

        let measured = false_positives as f64 / N as f64;
        assert!(
            measured < TARGET_FPR * 3.0,
            "FPR {measured:.4} exceeded 3× target {TARGET_FPR}"
        );
    }

    #[test]
    fn bloom_zero_false_negatives() {
        const N: usize = 10_000;

        let mut bf = BloomFilter::with_capacity(N, 0.01);
        for i in 0..N {
            bf.insert(format!("key_{i}").as_bytes());
        }

        for i in 0..N {
            assert!(
                bf.test(format!("key_{i}").as_bytes()),
                "false negative for key_{i}"
            );
        }
    }
}

use std::alloc::{alloc, Layout};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    __m128i, _mm_cmpeq_epi8, _mm_cvtsi128_si32, _mm_load_si128, _mm_movemask_epi8, _mm_set1_epi8,
};
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, RandomState};
use std::mem;

fn next_power_of_2_min_256(value: usize) -> usize {
    let value = value.max(256);

    if value.is_power_of_two() {
        return value;
    }

    return 1 << (usize::BITS - value.leading_zeros());
}

// const SHARD_BITS: usize = 18;
// const SHARD_CAPACITY: usize = 1 << SHARD_BITS;
// const SHARD_MASK: u64 = (SHARD_CAPACITY - 1) as u64;
const SHARD_BLOCK_BITS: usize = 4;
const SHARD_BLOCK_SIZE: usize = 1 << SHARD_BLOCK_BITS;
//const SHARD_SLOT_MASK: u64 = SHARD_MASK & (!0u64 << SHARD_BLOCK_BITS);
//const BLOCKS_PER_SHARD: usize = SHARD_CAPACITY / SHARD_BLOCK_SIZE;

type FoundBitMask = u16;

#[inline(always)]
fn probe_block(ptr: *const u8, block_index: usize, hash8: u8) -> (FoundBitMask, i32) {
    debug_assert_eq!(ptr as usize & (16 - 1), 0);
    let match_vec = unsafe { _mm_set1_epi8(hash8 as i8) };
    let index_block = unsafe { _mm_load_si128(ptr.offset(block_index as isize) as *const __m128i) };
    let found_mask =
        unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(match_vec, index_block)) } as FoundBitMask;

    let metadata = unsafe { _mm_cvtsi128_si32(index_block) } & 0xFF;

    (found_mask, metadata)
}

#[inline(always)]
pub fn unrolled_search<Q, K>(keys: *const K, bi: usize, found_mask: FoundBitMask, key: &Q) -> usize
where
    K: Borrow<Q> + std::cmp::PartialEq<Q>,
    Q: Eq + Hash + ?Sized,
{
    let m = found_mask & !0x1;

    // This written in an unrolled way
    // to allow the compiler to optimise it correctly
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    let m = m & !(1 << found);
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    let m = m & !(1 << found);
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    let m = m & !(1 << found);
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    let m = m & !(1 << found);
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    let m = m & !(1 << found);
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    let m = m & !(1 << found);
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    let m = m & !(1 << found);
    let found = m.trailing_zeros();
    if found == FoundBitMask::BITS {
        return usize::MAX;
    }
    let i = bi + (found as usize);
    let k = unsafe { keys.offset(i as isize) };
    if unsafe { &*k == &*key.borrow() } {
        return i;
    }

    return usize::MAX;
}

#[inline(always)]
pub(crate) fn calc_index<Q, S>(hash_builder: &S, key: &Q, size: usize) -> (usize, u8)
where
    Q: Hash + ?Sized,
    S: BuildHasher,
{
    let slot_mask: u64 = ((size - 1) as u64) & (!0u64 << SHARD_BLOCK_BITS);
    let h = hash_builder.hash_one(key);
    //let mut hasher = hash_builder.build_hasher();
    //key.hash(&mut hasher);
    //let h = hasher.finish();
    ((h & slot_mask) as usize, 
    ((h & 0xFF).max(1)) as u8)
}

#[repr(align(32))]
pub struct SFixMap<
    K: Hash + Eq + Default,
    V: Default,
    S: BuildHasher + Default + Clone = RandomState,
> {
    size: usize,
    index: Vec<u8>,
    build_hasher: S,    
    keys: Vec<K>,
    values: Vec<V>,    
}

impl<K: Hash + Eq + Default, V: Default, S: BuildHasher + Default + Clone> SFixMap<K, V, S> {
    pub fn with_capacity_and_hasher(capacity: usize, build_hasher: S) -> Self {
        let size = next_power_of_2_min_256(3 * capacity);

        Self {
            index: vec![0; size],
            keys: (0..size).map(|_| K::default()).collect(),
            values: (0..size).map(|_| V::default()).collect(),
            size,
            build_hasher,
        }
    }

    /// Creates a new `SFixMap` with the specified capacity and a default build hasher.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, S::default())
    }

    // #[inline(always)]
    // fn calc_index<Q>(&self, key: &Q) -> (usize, u8)
    // where
    //     K: Borrow<Q>,
    //     Q: Hash + ?Sized,
    // {
    //     let mut hasher = self.build_hasher.build_hasher();
    //     key.hash(&mut hasher);
    //     let h = hasher.finish();
    //     (
    //         (h & SHARD_SLOT_MASK) as usize,
    //         ((h & 0xFF).max(1)) as u8,
    //     )
    // }

    /// Retrieves the value associated with the given key from the appropriate shard,
    /// applying the provided reader function to the value if found.
    #[inline(always)]
    pub fn get<'a, Q>(&'a self, key: &Q) -> Option<&'a V>
    where
        K: Borrow<Q> + std::cmp::PartialEq<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (slot, hash8) = calc_index(&self.build_hasher, &key, self.size);
        self.get_internal(slot, hash8, key)
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the key already exists, its value is replaced and the old value is returned.
    /// Otherwise, `None` is returned.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>, &'static str> {
        let (slot, hash8) = calc_index(&self.build_hasher, &key, self.size);
        self.insert_internal(slot, hash8, key, value)
    }

    #[inline(always)]
    pub fn get_internal<'a, Q>(&'a self, start: usize, hash8: u8, key: &Q) -> Option<&'a V>
    where
        K: Borrow<Q> + std::cmp::PartialEq<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut block_index = start;
        let blocks_per_map = self.size / 16;

        for _ in 0..blocks_per_map {
            let (found_mask, metadata) = probe_block(self.index.as_ptr(), block_index, hash8);
            let bi = block_index as usize;
            let i = unrolled_search(self.keys.as_ptr(), bi, found_mask, key);

            if i != usize::MAX {
                return Some(unsafe { self.values.get_unchecked(i) });
            }

            // not found if no overflow marker
            if metadata != 0xFF {
                break;
            }

            block_index = (block_index + SHARD_BLOCK_SIZE) & (self.size - 1) as usize;
        }

        None // No match found
    }

    #[inline]
    pub fn get_mut<'a, Q>(&'a mut self, start: usize, hash8: u8, key: &Q) -> Option<&'a mut V>
    where
        K: Borrow<Q> + std::cmp::PartialEq<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut block_index = start;
        let blocks_per_map = self.size / 16;

        for _ in 0..blocks_per_map {
            let (found_mask, metadata) = probe_block(self.index.as_ptr(), block_index, hash8);
            let bi = block_index as usize;
            let i = unrolled_search(self.keys.as_ptr(), bi, found_mask, key);

            if i != usize::MAX {
                return Some(unsafe { self.values.get_unchecked_mut(i) });
            }

            if metadata != 0xFF {
                break;
            }

            block_index = (block_index + SHARD_BLOCK_SIZE) & (self.size - 1) as usize;
        }

        None
    }

    pub fn insert_internal(
        &mut self,
        start: usize,
        hash8: u8,
        key: K,
        value: V,
    ) -> Result<Option<V>, &'static str> {
        let mut block_index = start;
        let blocks_per_map = self.size / 16;

        for _ in 0..blocks_per_map {
            // look for existing
            let (found_mask, _) = probe_block(self.index.as_ptr(), block_index, hash8);
            let bi = block_index as usize;
            let i = unrolled_search(self.keys.as_ptr(), bi, found_mask, &key);

            if i != usize::MAX {
                let v = unsafe { self.values.get_unchecked_mut(i) };
                // Key already exists, replace the value and return the old one
                return Ok(Some(mem::replace(v, value)));
            }

            // look for empty
            let (found_mask, _) = probe_block(self.index.as_ptr(), block_index, 0);
            let m = found_mask & !0x1;
            let found = m.trailing_zeros();
            if found != FoundBitMask::BITS {
                let i = bi + (found as usize);
                // Found an empty slot
                self.index[i] = hash8;
                self.keys[i] = key;
                self.values[i] = value;
                return Ok(None);
            }

            // no room - move to next
            self.index[block_index] = 0xFF; // set overflow marker
            block_index = (block_index + SHARD_BLOCK_SIZE) & (self.size - 1) as usize;
        }

        // Shard is full, return an error
        Err("Shard is full")
    }

    pub fn remove<Q>(&mut self, start: usize, hash8: u8, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q> + std::cmp::PartialEq<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut block_index = start;
        let blocks_per_map = self.size / 16;

        for _ in 0..blocks_per_map {
            let (found_mask, metadata) = probe_block(self.index.as_ptr(), block_index, hash8);
            let bi = block_index as usize;
            let i = unrolled_search(self.keys.as_ptr(), bi, found_mask, key);

            if i != usize::MAX {
                self.index[i] = 0;
                let k = unsafe { self.keys.get_unchecked_mut(i) };
                let v = unsafe { self.values.get_unchecked_mut(i) };
                let kk = mem::replace(k, K::default());
                let vv = mem::replace(v, V::default());
                return Some((kk, vv));
            }

            if metadata != 0xFF {
                break;
            }

            block_index = (block_index + SHARD_BLOCK_SIZE) & (self.size - 1) as usize;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::hash::RandomState;

    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut map: SFixMap<String, i32, RandomState> =
            SFixMap::<String, i32, RandomState>::with_capacity(10);

        // // Insert
        assert_eq!(map.insert("one".to_string(), 1).unwrap(), None);
        assert_eq!(map.insert("two".to_string(), 2).unwrap(), None);
        assert_eq!(map.insert("x".to_string(), 3).unwrap(), None);

        // Get
        assert_eq!(map.get("one"), Some(&1));
        assert_eq!(map.get("x"), Some(&3));
        assert_eq!(map.get(&"two".to_string()), Some(&2));
        assert_eq!(map.get("three"), None);
    }
}

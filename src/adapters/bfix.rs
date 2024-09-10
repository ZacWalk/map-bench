use crate::bench::{Collection, CollectionHandle};
use std::alloc;
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU16, AtomicUsize, Ordering};
use std::sync::Arc;

const BLOCK_SIZE: usize = 64;
const NUM_BLOCKS: usize = 8;

pub struct BFixVec<T> {
    size: AtomicUsize,
    blocks: [AtomicPtr<T>; NUM_BLOCKS],
}

impl<T: Default> BFixVec<T> {
    fn new() -> Self {
        Self {
            size: AtomicUsize::new(0),
            blocks: [const { AtomicPtr::new(null_mut::<T>()) }; NUM_BLOCKS], // All blocks initially None
        }
    }

    fn allocate(&self) -> Option<(usize, &mut T)> {
        let current_size = self.size.fetch_add(1, Ordering::Acquire);
        let block_index = current_size / BLOCK_SIZE;
        let index_in_block = current_size % BLOCK_SIZE;

        // Lazily allocate a block if needed
        if block_index < NUM_BLOCKS {
            let mut block_ptr = self.blocks[block_index].load(Ordering::Acquire);
            if block_ptr.is_null() {
                let new_block_ptr =
                    unsafe { alloc::alloc(alloc::Layout::array::<T>(BLOCK_SIZE).unwrap()) }
                        as *mut T;

                // Attempt to store the new block pointer atomically
                match self.blocks[block_index].compare_exchange(
                    block_ptr,
                    new_block_ptr,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        block_ptr = new_block_ptr;
                    } // Successfully stored the new block pointer
                    Err(_) => {
                        // Another thread already allocated the block
                        unsafe {
                            alloc::dealloc(
                                new_block_ptr as *mut u8,
                                alloc::Layout::array::<T>(BLOCK_SIZE).unwrap(),
                            );
                        }
                        block_ptr = self.blocks[block_index].load(Ordering::SeqCst);
                    }
                }
            }

            let result: &mut T = unsafe {
                let raw_ptr = block_ptr.add(index_in_block);
                std::ptr::write(raw_ptr, T::default());
                &mut *raw_ptr
            };

            return Some((current_size, result));
        }

        None
    }

    fn get(&self, index: usize) -> Option<&T> {
        let current_size = self.size.load(Ordering::Acquire);
        if index >= current_size {
            return None;
        }

        let block_index = index / BLOCK_SIZE;
        let index_in_block = index % BLOCK_SIZE;

        let ptr = self.blocks[block_index].load(Ordering::Acquire);

        if !ptr.is_null() {
            // Safety: We've checked the index is within bounds,
            // and blocks are only accessed after they're allocated
            let value_ref: &T = unsafe { &*ptr.add(index_in_block) };
            return Some(value_ref);
        }

        None
    }

    fn get_unchecked(&self, index: usize) -> &T {
        let block_index = index / BLOCK_SIZE;
        let index_in_block = index % BLOCK_SIZE;
        let ptr = self.blocks[block_index].load(Ordering::Acquire);

        // Safety: We've checked the index is within bounds,
        // and blocks are only accessed after they're allocated
        unsafe { &*ptr.add(index_in_block) }
    }

    fn get_mut_unchecked(&self, index: usize) -> &mut T {
        let block_index = index / BLOCK_SIZE;
        let index_in_block = index % BLOCK_SIZE;
        let ptr = self.blocks[block_index].load(Ordering::Acquire);

        // Safety: We've checked the index is within bounds,
        // and blocks are only accessed after they're allocated
        unsafe { &mut *ptr.add(index_in_block) }
    }
}

const SLOT_BITS: usize = 8;
const SLOT_COUNT: usize = 1 << SLOT_BITS;
const SLOT_MASK: u64 = (SLOT_COUNT - 1) as u64;

struct Entry<K: Default, V: Default> {
    key: K,
    value: V,
    next: AtomicU16,
}

impl<K: Default, V: Default> Default for Entry<K, V> {
    fn default() -> Self {
        Self {
            key: K::default(),
            value: V::default(),
            next: AtomicU16::new(0), // Or any other suitable default value for 'next'
        }
    }
}

struct Shard<K: Default, V: Default> {
    slots: [AtomicU16; SLOT_COUNT],
    entries: BFixVec<Entry<K, V>>,
}

impl<K: Default, V: Default> Shard<K, V> {
    fn new() -> Self {
        Self {
            slots: [const { AtomicU16::new(0) }; SLOT_COUNT],
            entries: BFixVec::new(),
        }
    }

    fn allocate_entry(&self) -> Option<(usize, &mut Entry<K, V>)> {
        self.entries.allocate()
    }

    fn free_entry(&self, i: usize) {}
}

pub struct BFixMap<K: Default, V: Clone + Default, S = RandomState> {
    shards: Vec<Shard<K, V>>,
    build_hasher: S,
    bucket_count: usize,
}

fn closest_power_of_2_min_1024(value: usize) -> usize {
    // Ensure the minimum value is 1024
    let value = value.max(1024);

    if value.is_power_of_two() {
        return value;
    }

    // Find the next power of 2
    let next_power_of_2 = 1 << (usize::BITS - value.leading_zeros());

    // Calculate the previous power of 2
    let prev_power_of_2 = next_power_of_2 >> 1;

    // Determine which power of 2 is closer
    if (value - prev_power_of_2) <= (next_power_of_2 - value) {
        prev_power_of_2
    } else {
        next_power_of_2
    }
}

#[inline]
fn find_index<Q, K: Default, V: Default>(
    bucket: &Shard<K, V>,
    slot: usize,
    key: &Q,
) -> (Option<usize>, Option<usize>, usize)
where
    K: Borrow<Q> + Eq,
    Q: Eq + Hash + ?Sized,
{
    let i = bucket.slots[slot].load(Ordering::Acquire) as usize;
    let mut prev: Option<usize> = None;

    if i != 0 {
        let mut ii = i - 1;

        loop {
            let entry = &bucket.entries.get_unchecked(ii);

            if entry.key.borrow() == key {
                return (Some(ii), prev, i);
            }

            prev = Some(ii);
            let next = entry.next.load(Ordering::Acquire);

            if next == 0 {
                return (None, prev, i); // not found
            }

            ii = (next - 1) as usize;
        }
    }
    (None, None, i)
}

/// A concurrent hash map with bucket-level fine-grained locking.
///
/// This map is optimized to provide safe concurrent access for multiple threads, allowing
/// simultaneous reads and writes without blocking the entire map.
///
/// This map has a naive implementation however it turns out to have very good performance
/// with large numbers of threads. The trade-off is that the number of buckets is set at
/// creation time based on the provided capacity. The collection can grow to contain larger
/// numbers of items than the specified capacity, but the number of buckets does not change.
/// This design avoids any complex mechanisms around splitting buckets, reducing lock contention.
///
/// # Type Parameters
///
/// * `K`: The type of keys stored in the map. Must implement `Hash` and `Eq`.
/// * `V`: The type of values stored in the map. Must implement `Clone`.
/// * `S`: The type of build hasher used for hashing keys. Defaults to `RandomState`.
impl<K: Hash + Eq + Default + Clone, V: Clone + Default, S: BuildHasher + Default>
    BFixMap<K, V, S>
{
    /// Creates a new `BFixMap` with the specified capacity and build hasher.
    pub fn with_capacity_and_hasher(capacity: usize, build_hasher: S) -> Self {
        let bucket_count = closest_power_of_2_min_1024(capacity / 222);
        let mut buckets = Vec::with_capacity(bucket_count);
        buckets.resize_with(bucket_count, || Shard::<K, V>::new());

        Self {
            shards: buckets,
            build_hasher,
            bucket_count,
        }
    }

    /// Creates a new `BFixMap` with the specified capacity and a default build hasher.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, S::default())
    }

    #[inline]
    fn calc_index<Q>(&self, key: &Q) -> (usize, usize)
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        let mut hasher = self.build_hasher.build_hasher();
        key.hash(&mut hasher);
        let h = hasher.finish();
        let shard_mask = (self.bucket_count - 1) as u64;
        (
            ((h >> SLOT_BITS) & shard_mask) as usize,
            (h & SLOT_MASK) as usize,
        )
    }

    /// Retrieves the value associated with the given key, if it exists.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (shard_index, slot) = self.calc_index(&key);
        let shard = &self.shards[shard_index];
        let i = shard.slots[slot as usize].load(Ordering::Relaxed) as usize;

        if i != 0 {
            let mut ii = i - 1;

            loop {
                let entry = &shard.entries.get_unchecked(ii);

                if entry.key.borrow() == key {
                    return Some(entry.value.clone());
                }

                let next = entry.next.load(Ordering::Relaxed);

                if next == 0 {
                    return None; // not found
                }

                ii = (next - 1) as usize;
            }
        }

        None
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the key already exists, its value is replaced and the old value is returned.
    /// Otherwise, `None` is returned.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let (shard_index, slot) = self.calc_index(&key);
        let shard = &self.shards[shard_index];
        let mut inserted_index: Option<usize> = None;

        loop {
            let (found_index, _, slot_value) = find_index(&shard, slot, &key);

            if let Some(index) = found_index {
                if let Some(i) = inserted_index {
                    shard.free_entry(i);
                }

                let entry = &mut shard.entries.get_mut_unchecked(index);
                return Some(std::mem::replace(&mut entry.value, value));
            }

            if inserted_index == None {
                let inserted = shard.allocate_entry();

                if let Some((i, entry)) = inserted {
                    entry.value = value.clone();
                    entry.key = key.clone();
                    inserted_index = Some(i);
                } else {
                    return None; //failed to allocate block!!
                }
            }

            shard
                .entries
                .get_mut_unchecked(inserted_index.unwrap())
                .next
                .store(slot_value as u16, Ordering::Release);

            match shard.slots[slot as usize].compare_exchange(
                slot_value as u16,
                (inserted_index.unwrap() + 1) as u16,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    break; // success
                }
                Err(_) => {
                    // Another thread already inserted on this slot.
                    // Need to try again.
                }
            }
        }

        None
    }

    /// Removes the key-value pair associated with the given key, if it exists.
    ///
    /// If the key exists, its value is removed and returned. Otherwise, `None` is returned.
    pub fn remove(&self, key: &K) -> Option<V> {
        let (shard_index, slot) = self.calc_index(&key);
        let shard = &self.shards[shard_index];

        loop {
            let (found_index, prev_index, slot_value) = find_index(&shard, slot, &key);

            if let Some(ifound) = found_index {
                let entry = &mut shard.entries.get_mut_unchecked(ifound);
                let next = entry.next.load(Ordering::Acquire); 
                // XXXXX Bug: `next` could change before the write to slots 
                // XXXXX Need an intermediate step to remove from list?

                // Remove from slot
                if slot_value == ifound + 1 {
                    match shard.slots[slot as usize].compare_exchange(
                        slot_value as u16,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // success
                        }
                        Err(_) => {
                            // Another thread already inserted on this slot.
                            // Need to try again.
                            continue;
                        }
                    }
                }
                else { 
                    // remove from prev in list
                    let prev_entry = &mut shard.entries.get_mut_unchecked(prev_index.unwrap());

                    match prev_entry.next.compare_exchange(
                        (ifound + 1) as u16,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // success
                        }
                        Err(_) => {
                            // Another thread already replaced next.
                            // Need to try again.
                            continue;
                        }
                    }
                }

                let result = Some(std::mem::replace(&mut entry.value, V::default()));
                shard.free_entry(ifound);
                return result;
            }

            return None // Didnt find
        }
    }

    /// Modifies the value associated with the given key using the provided function.
    ///
    /// If the key exists, the function `f` is called with a mutable reference to the value.
    /// Returns `true` if the value was modified, `false` otherwise.
    pub fn modify<F>(&self, key: &K, f: F) -> bool
    where
        F: FnOnce(&mut V),
    {
        let (shard_index, slot) = self.calc_index(&key);
        let shard = &self.shards[shard_index];
        let (found_index, _, _) = find_index(&shard, slot, &key);

        if let Some(index) = found_index {
            let entry = &mut shard.entries.get_mut_unchecked(index);
            f(&mut entry.value);
            return true;
        }

        false
    }
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct BFixCollection<K: Eq + Hash + Send + 'static + Default, V: Clone + Default, H: BuildHasher + 'static>(
    Arc<BFixMap<K, V, H>>,
);

pub struct BFixHandle<K: Eq + Hash + Send + 'static + Default, V: Clone + Default, H: BuildHasher + 'static>(
    Arc<BFixMap<K, V, H>>,
);

impl<K, V, H> BFixCollection<K, V, H>
where
    K: Send + Default + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(BFixMap::with_capacity_and_hasher(
            capacity,
            H::default(),
        )))
    }
}

impl<K, V, H> BFixHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + Eq + 'static + Default,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static + Default,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new(m: Arc<BFixMap<K, V, H>>) -> Self {
        Self(m)
    }
}

impl<K, V, H> Collection for BFixCollection<K, V, H>
where
    K: Send + Default + Sync + From<u64> + Copy + Hash + Ord + Eq + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = BFixHandle<K, V, H>;

    fn pin(&self) -> Self::Handle {
        Self::Handle::new(self.0.clone())
    }

    fn prefill_complete(&self) {}
}

impl<K, V, H> CollectionHandle for BFixHandle<K, V, H>
where
    K: Send + Default + Sync + From<u64> + Copy + Hash + Ord + Eq + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, key: &Self::Key) -> bool {
        self.0.get(key).is_some()
    }

    fn insert(&self, key: Self::Key) -> bool {
        self.0.insert(key, V::default()).is_none()
    }

    fn remove(&self, key: &Self::Key) -> bool {
        self.0.remove(key).is_some()
    }

    fn update(&self, key: &Self::Key) -> bool {
        self.0.modify(key, |count| *count += V::from(1))
    }
}

use crate::bench::{Collection, CollectionHandle};
use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;


type LOCK_ATOMIC = AtomicU64;
const LOCK_BITS: usize = size_of::<LOCK_ATOMIC>() * 8;

pub struct BitLock<T> {
    lock_state: LOCK_ATOMIC,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Sync for BitLock<T> {} // Required for shared access across threads

impl<T> BitLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            lock_state: LOCK_ATOMIC::new(0), // Initially, no locks are held
            data: UnsafeCell::new(data),
        }
    }

    #[inline]
    pub fn read(&self) -> BitLockReadGuard<T> {
        loop {
            let current_state = self.lock_state.load(Ordering::Acquire);

            // Check if the exclusive lock (bit 0) is held
            if current_state & 1 == 0 {
                // Find the first zero bit (available lock bit)
                let lock_bit = ((!current_state) | 1).leading_zeros() as usize;

                if lock_bit != LOCK_BITS {
                    let lock_bit = (LOCK_BITS - 1) - lock_bit;
                    let lock_bit_mask = 1 << lock_bit;

                    // Try to acquire the found lock bit
                    if self
                        .lock_state
                        .compare_exchange_weak(
                            current_state,
                            current_state | lock_bit_mask,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        return BitLockReadGuard {
                            lock: self,
                            lock_bit: lock_bit_mask,
                        };
                    }
                }
            }

            std::hint::spin_loop(); // Spin-wait efficiently
        }
    }

    #[inline]
    pub fn write(&self) -> BitLockWriteGuard<T> {
        // First, try to acquire the exclusive lock (bit 0)
        while self
            .lock_state
            .compare_exchange_weak(0, 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {}

        // Wait for all other locks to be released
        while self.lock_state.load(Ordering::Acquire) != 1 {
            std::hint::spin_loop(); // Spin-wait efficiently
        }

        BitLockWriteGuard { lock: self }
    }
}

pub struct BitLockReadGuard<'a, T> {
    lock: &'a BitLock<T>,
    lock_bit: u64,
}

impl<'a, T> std::ops::Deref for BitLockReadGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<'a, T> Drop for BitLockReadGuard<'a, T> {

    #[inline]
    fn drop(&mut self) {
        // Release the lock bit when the guard is dropped
        self.lock
            .lock_state
            .fetch_and(!self.lock_bit, Ordering::Release);
    }
}

pub struct BitLockWriteGuard<'a, T> {
    lock: &'a BitLock<T>,
}

impl<'a, T> std::ops::Deref for BitLockWriteGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<'a, T> std::ops::DerefMut for BitLockWriteGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<'a, T> Drop for BitLockWriteGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        // Release the exclusive lock (bit 0)
        self.lock.lock_state.fetch_and(!1, Ordering::Release);
    }
}

type BFixLock<T> = parking_lot::RwLock<T>;
//type BFixLock<T> = BitLock<T>;


const SLOT_BITS: usize = 8;
const SLOT_COUNT: usize = 1 << SLOT_BITS;
const SLOT_MASK: u64 = (SLOT_COUNT - 1) as u64;

struct Entry<K, V> {
    //slot: u8,
    key: K,
    value: V,
    next: u16,
}

struct Shard<K, V> {
    slots: [u16; SLOT_COUNT],
    entries: Vec<Entry<K, V>>,
}

impl<K, V> Shard<K, V> {
    fn new() -> Self {
        Self {
            slots: [0; SLOT_COUNT],
            entries: Vec::new(),
        }
    }
}

pub struct BFixMap<K, V: Clone, S = RandomState> {
    buckets: Vec<BFixLock<Shard<K, V>>>,
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
fn find_index<Q, K, V>(bucket: &Shard<K, V>, slot: usize, key: &Q) -> (Option<usize>, Option<usize>)
where
    K: Borrow<Q> + Eq,
    Q: Eq + Hash + ?Sized,
{
    let i = bucket.slots[slot] as usize;
    let mut prev: Option<usize> = None;

    if i != 0 {
        let mut ii = i - 1;

        loop {
            let entry = &bucket.entries[ii];

            if entry.key.borrow() == key {
                return (Some(ii), prev);
            }

            prev = Some(ii);

            if entry.next == 0 {
                return (None, prev); // not found
            }

            ii = (entry.next - 1) as usize;
        }
    }
    (None, None)
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
impl<K: Hash + Eq + Default, V: Clone + Default, S: BuildHasher + Default> BFixMap<K, V, S> {
    /// Creates a new `BFixMap` with the specified capacity and build hasher.
    pub fn with_capacity_and_hasher(capacity: usize, build_hasher: S) -> Self {
        let bucket_count = closest_power_of_2_min_1024(capacity / 222);
        let mut buckets = Vec::with_capacity(bucket_count);
        buckets.resize_with(bucket_count, || BFixLock::new(Shard::<K, V>::new()));

        Self {
            buckets,
            build_hasher,
            bucket_count,
        }
    }

    /// Creates a new `BFixMap` with the specified capacity and a default build hasher.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, S::default())
    }

    #[inline]
    fn hash_key<Q>(&self, key: &Q) -> (usize, usize)
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        let mut hasher = self.build_hasher.build_hasher();
        key.hash(&mut hasher);
        let h = hasher.finish();
        let shard_mask = (self.bucket_count - 1) as u64;
        (((h >> SLOT_BITS) & shard_mask) as usize, (h & SLOT_MASK) as usize)
    }

    /// Retrieves the value associated with the given key, if it exists.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash_key, slot) = self.hash_key(&key);
        let bucket = &self.buckets[hash_key].read();

        let i = bucket.slots[slot as usize] as usize;

        if i != 0 {
            let mut ii = i - 1;

            loop {
                let entry = &bucket.entries[ii];

                if entry.key.borrow() == key {
                    return Some(entry.value.clone());
                }

                if entry.next == 0 {
                    return None; // not found
                }

                ii = (entry.next - 1) as usize;
            }
        }

        None
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the key already exists, its value is replaced and the old value is returned.
    /// Otherwise, `None` is returned.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let (hash_key, slot) = self.hash_key(&key);
        let mut bucket = self.buckets[hash_key].write();

        let (found_index, prev_index) = find_index(&bucket, slot, &key);
        //let mut writer = BFixLockGuard::upgrade(bucket);

        if let Some(index) = found_index {
            let entry = &mut bucket.entries[index];
            return Some(std::mem::replace(&mut entry.value, value));
        }

        bucket.entries.push(Entry {
            next: 0,
            key,
            value: value,
        });

        let len = bucket.entries.len();

        if let Some(index) = prev_index {
            bucket.entries[index].next = len as u16;
        } else {
            bucket.slots[slot as usize] = len as u16; // std::cmp::min(len, 255) as u8;
        }

        None
    }

    /// Removes the key-value pair associated with the given key, if it exists.
    ///
    /// If the key exists, its value is removed and returned. Otherwise, `None` is returned.
    pub fn remove(&self, key: &K) -> Option<V> {
        let (hash_key, slot) = self.hash_key(&key);
        let mut bucket = self.buckets[hash_key].write();

        let (index, prev_index) = find_index(&bucket, slot, &key);

        if let Some(index) = index {
            let entry = &mut bucket.entries[index];
            let result = Some(std::mem::replace(&mut entry.value, V::default()));
            let slot_index = slot as usize;

            if let Some(index) = prev_index {
                bucket.entries[index].next = entry.next;
            }

            if bucket.slots[slot_index] == index as u16 {
                bucket.slots[slot_index] = 0u16;
            }

            return result;
        }

        None
    }

    /// Modifies the value associated with the given key using the provided function.
    ///
    /// If the key exists, the function `f` is called with a mutable reference to the value.
    /// Returns `true` if the value was modified, `false` otherwise.
    pub fn modify<F>(&self, key: &K, f: F) -> bool
    where
        F: FnOnce(&mut V),
    {
        let (hash_key, slot) = self.hash_key(&key);
        let mut bucket = self.buckets[hash_key].write();
        let (index, _) = find_index(&bucket, slot, &key);

        if let Some(index) = index {
            //let mut writer = BFixLockGuard::upgrade(bucket);
            let entry = &mut bucket.entries[index];
            f(&mut entry.value);
            return true;
        }

        false
    }
}

#[derive(Clone)]
pub struct BFixCollection<K: Eq + Hash + Send + 'static, V: Clone, H: BuildHasher + 'static>(
    Arc<BFixMap<K, V, H>>,
);

pub struct BFixHandle<K: Eq + Hash + Send + 'static, V: Clone, H: BuildHasher + 'static>(
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
    K: Send + Sync + Eq + Hash + Clone + Eq + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
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

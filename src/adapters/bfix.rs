use std::collections::hash_map::DefaultHasher;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::hint::spin_loop;
use std::ops::{Deref, DerefMut};
use std::cell::UnsafeCell;

use std::sync::Arc;

use crate::bench::{Collection, CollectionHandle};


//type RwLock<T> = parking_lot::RwLock<T>;
//type BFixLock<T> = parking_lot::RwLock<T>;
type BFixLock<T> = parking_lot::Mutex<T>;

struct Entry<K, V> {
    key: K,
    value: Option<V>,
}

pub struct BFixMap<K, V: Clone, S = DefaultHasher> {
    buckets: Vec<BFixLock<Vec<Entry<K, V>>>>,
    build_hasher: S,
    bucket_count: usize,
}

fn closest_power_of_2(value: usize) -> usize {
    // Ensure the minimum value is 1024
    let value = value.max(1024);

    // Handle special case where value is already a power of 2
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

impl<K: Hash + Eq, V: Clone, S: BuildHasher> BFixMap<K, V, S> {
    fn with_capacity_and_hasher(capacity: usize, build_hasher: S) -> Self {
        let bucket_count = closest_power_of_2(capacity / 256);
        let mut buckets = Vec::with_capacity(bucket_count);
        buckets.resize_with(bucket_count, || BFixLock::new(Vec::new()));

        Self {
            buckets,
            build_hasher,
            bucket_count,
        }
    }

    fn hash_key(&self, key: &K) -> usize {
        let mut hasher = self.build_hasher.build_hasher();
        key.hash(&mut hasher);
        let mask = self.bucket_count - 1;
        hasher.finish() as usize & mask
    }

    fn get(&self, key: &K) -> Option<V> {
        let hash_key = self.hash_key(&key);
        let buckets = &self.buckets;
        let bucket = &buckets[hash_key].lock();
        bucket
            .iter()
            .find(|entry| entry.key == *key)
            .map(|entry| entry.value.clone())?
    }

    fn insert(&self, key: K, value: V) -> Option<V> {
        let hash_key = self.hash_key(&key);
        let buckets = &self.buckets;
        let bucket = &mut buckets[hash_key].lock();
        for entry in bucket.iter_mut() {
            if entry.key == key {
                return mem::replace(&mut entry.value, Some(value));
            }
        }
        bucket.push(Entry {
            key,
            value: Some(value),
        });
        None
    }

    fn remove(&self, key: &K) -> Option<V> {
        let hash_key = self.hash_key(&key);
        let buckets = &self.buckets;
        let bucket = &mut buckets[hash_key].lock();
        for i in 0..bucket.len() {
            if bucket[i].key == *key {
                return mem::replace(&mut bucket[i].value, None);
            }
        }
        None
    }

    fn modify<F>(&self, key: &K, f: F) -> bool
    where
        F: FnOnce(&mut V),
    {
        let hash_key = self.hash_key(&key);
        let buckets = &self.buckets;
        let bucket = &mut buckets[hash_key].lock();
        let mut modified = false;

        bucket
            .iter_mut()
            .find(|entry| entry.key == *key)
            .map(|entry| {
                if let Some(ref mut value) = entry.value {
                    // Check if value exists
                    f(value);
                    modified = true;
                }
            });

        modified
    }
}

#[derive(Clone)]
pub struct BFixCollection<K: Eq + Hash + Send + 'static, V: Clone, H: BuildHasher + 'static>(
    Arc<BFixMap<K, V, H>>,
);

pub struct BFixHandle<K: Eq + Hash + Send + 'static, V: Clone, H: BuildHasher + 'static>(
    Arc<BFixMap<K, V, H>>,
);

impl<K, V, H> BFixHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new(m: Arc<BFixMap<K, V, H>>) -> Self {
        Self(m)
    }
}

impl<K, V, H> Collection for BFixCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = BFixHandle<K, V, H>;

    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(BFixMap::with_capacity_and_hasher(
            capacity,
            H::default(),
        )))
    }

    fn pin(&self) -> Self::Handle {
        Self::Handle::new(self.0.clone())
    }
}

impl<K, V, H> CollectionHandle for BFixHandle<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&mut self, key: &Self::Key) -> bool {
        self.0.get(key).is_some()
    }

    fn insert(&mut self, key: Self::Key) -> bool {
        self.0.insert(key, V::default()).is_none()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.0.remove(key).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        self.0.modify(key, |count| *count += V::from(1))
    }
}

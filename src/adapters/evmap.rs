use std::hash::{BuildHasher, Hash};
use std::sync::{Arc, Mutex};

use evmap::{ReadHandle, ShallowCopy, WriteHandle};

use crate::bench::{Collection, CollectionHandle};

#[derive(Clone)]
pub struct EvMeta;

unsafe impl Sync for EvMeta {}

impl EvMeta {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Clone)]
pub struct EvMapHandle<K: Eq + Hash + Clone, V: Eq + Hash + ShallowCopy, H: BuildHasher + Clone>(
    ReadHandle<K, V, EvMeta, H>,
    Arc<Mutex<WriteHandle<K, V, EvMeta, H>>>,
);

impl<K, V, H> EvMapHandle<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Eq
        + Hash
        + ShallowCopy
        + Send
        + Sync
        + Clone
        + Default
        + std::ops::Add<Output = V>
        + From<u64>
        + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    pub fn new(
        read_handle: ReadHandle<K, V, EvMeta, H>,
        write_handle: Arc<Mutex<WriteHandle<K, V, EvMeta, H>>>,
    ) -> Self {
        Self(read_handle, write_handle)
    }
}

pub struct EvMapCollection<K, V, H>(Arc<Mutex<EvMapHandle<K, V, H>>>)
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Eq
        + Hash
        + ShallowCopy
        + Send
        + Sync
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = V>
        + From<u64>
        + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static;

impl<K, V, H> EvMapCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Eq
        + Hash
        + ShallowCopy
        + Send
        + Sync
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = V>
        + From<u64>
        + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    pub fn with_capacity(_capacity: usize) -> Self {
        let (r, w) = evmap::with_hasher::<K, V, EvMeta, H>(EvMeta::new(), H::default()); //(capacity, H::default());
        let h = EvMapHandle::new(r, Arc::new(Mutex::new(w)));
        Self(Arc::new(Mutex::new(h)))
    }
}

unsafe impl<K, V, H> Sync for EvMapCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Eq
        + Hash
        + ShallowCopy
        + Send
        + Sync
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = V>
        + From<u64>
        + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
}

impl<K, V, H> Collection for EvMapCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Eq
        + Hash
        + ShallowCopy
        + Send
        + Sync
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = V>
        + From<u64>
        + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = EvMapHandle<K, V, H>;

    fn pin(&self) -> Self::Handle {
        let h = self.0.lock().unwrap();
        EvMapHandle::new(h.0.clone(), h.1.clone())
    }

    fn prefill_complete(&self)
    {
        let h = self.0.lock().unwrap();
        let mut w = h.1.lock().unwrap();
        w.refresh();
    }
}

impl<K, V, H> CollectionHandle for EvMapHandle<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Eq
        + Hash
        + ShallowCopy
        + Send
        + Sync
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = V>
        + From<u64>
        + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, key: &Self::Key) -> bool {
        self.0.get_one(&key).is_some()
    }

    fn insert(&self, key: Self::Key) -> bool {
        let mut w = self.1.lock().unwrap();
        w.insert(key, V::default());
        true
    }

    fn remove(&self, key: &Self::Key) -> bool {
        let mut w = self.1.lock().unwrap();
        w.clear(key.clone());
        true
    }

    fn update(&self, key: &Self::Key) -> bool {
        if let Some(value) = self.0.get_one(&key) {
            let v = *value;
            drop(value);
            let mut w = self.1.lock().unwrap();
            w.update(key.clone(), v + V::from(1));
            true
        } else {
            false
        }
    }
}

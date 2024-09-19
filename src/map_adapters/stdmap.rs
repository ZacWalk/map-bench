use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use crate::perf_map::{Collection, CollectionHandle, FromU64, ValueModifier};

type Lock<T> = parking_lot::RwLock<T>;
//type Lock<T> = parking_lot::Mutex<T>;

#[derive(Clone)]
pub struct StdHashMapCollection<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(
    Arc<Lock<HashMap<K, V, H>>>,
);

impl<K, V, H> StdHashMapCollection<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(Lock::new(HashMap::with_capacity_and_hasher(
            capacity,
            H::default(),
        ))))
    }
}

pub struct StdHashMapHandle<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(
    Arc<Lock<HashMap<K, V, H>>>,
);

impl<K, V, H> StdHashMapHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new(m: Arc<Lock<HashMap<K, V, H>>>) -> Self {
        Self(m)
    }
}

impl<K, V, H> Collection for StdHashMapCollection<K, V, H>
where
    K: Send + Sync + Hash + Ord + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = StdHashMapHandle<K, V, H>;

    fn pin(&self) -> Self::Handle {
        Self::Handle::new(self.0.clone())
    }

    fn prefill_complete(&self)
    {
    }
}

impl<K, V, H> CollectionHandle for StdHashMapHandle<K, V, H>
where
    K: Send + Sync + Hash + Ord + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, key: &Self::Key) -> bool {
        self.0.read().get(&key).is_some()
    }

    fn insert(&self, key: Self::Key) -> bool {
        self.0.write().insert(key, V::default()).is_none()
    }

    fn remove(&self, key: &Self::Key) -> bool {
        self.0.write().remove(&key).is_some()
    }

    fn update(&self, key: &Self::Key) -> bool {
        let mut w = self.0.write();

        if let Some(v) = w.get_mut(&key) {
            v.modify();
            true
        } else {
            false
        }
    }
}

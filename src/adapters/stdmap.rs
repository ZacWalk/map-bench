use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use crate::bench::{Collection, CollectionHandle};

//type RwLock<T> = parking_lot::RwLock<T>;
type Lock<T> = parking_lot::Mutex<T>;

#[derive(Clone)]
pub struct StdHashMapCollection<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(
    Arc<Lock<HashMap<K, V, H>>>,
);

pub struct StdHashMapHandle<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(
    Arc<Lock<HashMap<K, V, H>>>,
);

impl<K, V, H> StdHashMapHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new(m: Arc<Lock<HashMap<K, V, H>>>) -> Self {
        Self(m)
    }
}

impl<K, V, H> Collection for StdHashMapCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = StdHashMapHandle<K, V, H>;

    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(Lock::new(HashMap::with_capacity_and_hasher(
            capacity,
            H::default(),
        ))))
    }

    fn pin(&self) -> Self::Handle {
        Self::Handle::new(self.0.clone())
    }
}

impl<K, V, H> CollectionHandle for StdHashMapHandle<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&mut self, key: &Self::Key) -> bool {
        self.0.lock().get(&key).is_some()
    }

    fn insert(&mut self, key: Self::Key) -> bool {
        self.0.lock().insert(key, V::default()).is_none()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.0.lock().remove(&key).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        let mut w = self.0.lock();

        if let Some(v) = w.get_mut(&key) {
            *v += V::from(1);
            true
        } else {
            false
        }
    }
}

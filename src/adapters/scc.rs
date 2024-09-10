use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use crate::bench::{Collection, CollectionHandle};


#[derive(Clone)]
pub struct SccCollection<K, V, H: BuildHasher>(
    Arc<scc::HashMap<K, V, H>>,
);

impl<K, V, H> SccCollection<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn with_capacity(capacity: usize) -> Self {
        scc::ebr::Guard::new().accelerate();
        Self(Arc::new(scc::HashMap::with_capacity_and_hasher(
            capacity,
            H::default(),
        )))
    }
}

pub struct SccHandle<K, V, H: BuildHasher>(
    scc::HashMap<K, V, H>,
);

impl<K, V, H> SccHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new(m: scc::HashMap<K, V, H>) -> Self {
        Self(
            m,
        )
    }
}

impl<K, V, H> Collection for SccCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Hash + Ord + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = SccHandle<K, V, H>;

    fn pin(&self) -> Self::Handle {
        Self::Handle::new((*self.0).clone())
    }

    fn prefill_complete(&self)
    {
    }
}

impl<K, V, H> CollectionHandle for SccHandle<K, V, H>
where
    K: Send + Sync + From<u64> + Hash + Ord + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, key: &Self::Key) -> bool {
        self.0.read(&key, |_, _| ()).is_some()
    }

    fn insert(&self, key: Self::Key) -> bool {
        self.0.insert(key, V::default()).is_ok()
    }

    fn remove(&self, key: &Self::Key) -> bool {
        self.0.remove(&key).is_some()
    }

    fn update(&self, key: &Self::Key) -> bool {
        self.0.update(&key, |_, v| *v += V::from(1)).is_some()
    }
}


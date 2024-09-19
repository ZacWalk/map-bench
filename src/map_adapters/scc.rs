use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use crate::perf_map::{Collection, CollectionHandle, FromU64, ValueModifier};


#[derive(Clone)]
pub struct SccCollection<K, V, H: BuildHasher>(
    scc::HashMap<K, V, H>,

)where
K: Send + Sync + Eq + Hash + Clone + FromU64 + 'static,
V: Send + Sync + Clone + Default + ValueModifier + 'static,
H: Send + Sync + BuildHasher + Default + 'static + Clone;

impl<K, V, H> SccCollection<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn with_capacity(capacity: usize) -> Self {
        scc::ebr::Guard::new().accelerate();
        Self(scc::HashMap::with_capacity_and_hasher(
            capacity,
            H::default(),
        ))
    }
}

pub struct SccHandle<K, V, H: BuildHasher>(
    scc::HashMap<K, V, H>,
);

impl<K, V, H> SccHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
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
    K: Send + Sync + Hash + Ord + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = SccHandle<K, V, H>;

    fn pin(&self) -> Self::Handle {
        Self::Handle::new(self.0.clone())
    }

    fn prefill_complete(&self)
    {
    }
}

impl<K, V, H> CollectionHandle for SccHandle<K, V, H>
where
    K: Send + Sync + Hash + Ord + Clone + FromU64 +  'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, key: &Self::Key) -> bool {
        self.0.read(&key, |_, _| Some(1)).is_some()
    }

    fn insert(&self, key: Self::Key) -> bool {
        self.0.insert(key, V::default()).is_ok()
    }

    fn remove(&self, key: &Self::Key) -> bool {
        self.0.remove(&key).is_some()
    }

    fn update(&self, key: &Self::Key) -> bool {
        self.0.update(&key, |_, v| v.modify()).is_some()
    }
}


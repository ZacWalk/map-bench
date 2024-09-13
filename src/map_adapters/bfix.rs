use bfixmap::BFixMap;
use crate::perf_map::{Collection, CollectionHandle};
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;


///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct BFixCollection<K: Eq + Hash + Send + 'static + Default + Clone, V: Clone + Default, H: BuildHasher + 'static + Default>(
    Arc<BFixMap<K, V, H>>,
);

pub struct BFixHandle<K: Eq + Hash + Send + 'static + Default + Clone, V: Clone + Default, H: BuildHasher + 'static + Default>(
    Arc<BFixMap<K, V, H>>,
);

impl<K, V, H> BFixCollection<K, V, H>
where
    K: Send + Default + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone + Default,
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
    K: Send + Default + Sync + From<u64>  + Hash + Ord + Eq + Clone + 'static,
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
    K: Send + Default + Sync + From<u64> + Hash + Ord + Eq + Clone + 'static,
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
        //self.0.remove(key).is_some()
        true
    }

    fn update(&self, key: &Self::Key) -> bool {
        //self.0.modify(key, |count| *count += V::from(1))
        true
    }
}

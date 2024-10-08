use bfixmap::BFixMap;
use crate::perf_map::{Collection, CollectionHandle, FromU64, ValueModifier};
use std::hash::{BuildHasher, Hash};


#[derive(Clone)]
pub struct BFixCollection<K: Eq + Hash + Send + 'static + Default + Clone, V: Clone + Default, H: BuildHasher + 'static + Default + Clone>(
    BFixMap<K, V, H>,
);

pub struct BFixHandle<K: Eq + Hash + Send + 'static + Default + Clone, V: Clone + Default, H: BuildHasher + 'static + Default + Clone>(
    BFixMap<K, V, H>,
);

impl<K, V, H> BFixCollection<K, V, H>
where
    K: Send + Default + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone + Default,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self(BFixMap::with_capacity_and_hasher(
            capacity,
            H::default(),
        ))
    }
}

impl<K, V, H> BFixHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + Eq + 'static + Default,
    V: Send + Sync + Clone + Default + ValueModifier + 'static + Default,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new(m: BFixMap<K, V, H>) -> Self {
        Self(m)
    }
}

impl<K, V, H> Collection for BFixCollection<K, V, H>
where
    K: Send + Default + Sync  + Hash + Ord + Eq + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
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
    K: Send + Default + Sync + Hash + Ord + Eq + Clone + FromU64 + 'static,
    V: Send + Sync + Clone + Default + ValueModifier + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, key: &Self::Key) -> bool {
        self.0.get(&key, |_| Some(1)).is_some()
    }

    fn insert(&self, key: Self::Key) -> bool {
        self.0.insert(key, V::default()).unwrap().is_none()
    }

    fn remove(&self, key: &Self::Key) -> bool {
        self.0.remove(key).is_some()
    }

    fn update(&self, key: &Self::Key) -> bool {
        self.0.modify(key, |v| { v.modify(); true }).is_some()
    }
}

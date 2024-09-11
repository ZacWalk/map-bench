use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

use crate::perf_map::{Collection, CollectionHandle};

#[derive(Clone)]
pub struct NopCollection<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(PhantomData<K>, PhantomData<V>, PhantomData<H>);

impl<K, V, H> NopCollection<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn with_capacity(_capacity: usize) -> Self {
        Self(PhantomData, PhantomData, PhantomData)
    }
}

pub struct NopHandle<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(PhantomData<K>, PhantomData<V>, PhantomData<H>);

impl<K, V, H> NopHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData)
    }
}

impl<K, V, H> Collection for NopCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Hash + Ord + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = NopHandle<K, V, H>;

    fn pin(&self) -> Self::Handle {
        Self::Handle::new()
    }

    fn prefill_complete(&self)
    {
    }
}

impl<K, V, H> CollectionHandle for NopHandle<K, V, H>
where
    K: Send + Sync + From<u64> + Hash + Ord + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, _key: &Self::Key) -> bool {
        true
    }

    fn insert(&self, _key: Self::Key) -> bool {
        true
    }

    fn remove(&self, _key: &Self::Key) -> bool {
        true
    }

    fn update(&self, _key: &Self::Key) -> bool {
        true
    }
}

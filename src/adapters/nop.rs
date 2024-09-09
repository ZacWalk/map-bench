use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::bench::{Collection, CollectionHandle};

type Lock<T> = parking_lot::RwLock<T>;
//type Lock<T> = parking_lot::Mutex<T>;

#[derive(Clone)]
pub struct NopCollection<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(usize, PhantomData<K>, PhantomData<V>, PhantomData<H>);

impl<K, V, H> NopCollection<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self(capacity, PhantomData, PhantomData, PhantomData)
    }
}

pub struct NopHandle<K: Eq + Hash + Send + 'static, V, H: BuildHasher + 'static>(usize, PhantomData<K>, PhantomData<V>, PhantomData<H>);

impl<K, V, H> NopHandle<K, V, H>
where
    K: Send + Sync + Eq + Hash + Clone + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: Send + Sync + BuildHasher + Default + 'static + Clone,
{
    pub fn new(capacity: usize) -> Self {
        Self(capacity, PhantomData, PhantomData, PhantomData)
    }
}

impl<K, V, H> Collection for NopCollection<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Handle = NopHandle<K, V, H>;

    fn pin(&self) -> Self::Handle {
        Self::Handle::new(self.0.clone())
    }

    fn prefill_complete(&self)
    {
    }
}

impl<K, V, H> CollectionHandle for NopHandle<K, V, H>
where
    K: Send + Sync + From<u64> + Copy + Hash + Ord + 'static,
    V: Send + Sync + Clone + Default + std::ops::AddAssign + From<u64> + 'static,
    H: BuildHasher + Default + Send + Sync + Clone + 'static,
{
    type Key = K;

    fn get(&self, key: &Self::Key) -> bool {
        true
    }

    fn insert(&self, key: Self::Key) -> bool {
        true
    }

    fn remove(&self, key: &Self::Key) -> bool {
        true
    }

    fn update(&self, key: &Self::Key) -> bool {
        true
    }
}

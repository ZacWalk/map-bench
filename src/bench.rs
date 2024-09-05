use affinity::set_thread_affinity;
use core_affinity::get_core_ids;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::RngCore;
use rand::SeedableRng;
use std::sync::Barrier;
use std::{sync::Arc, time::Duration};

#[derive(Debug, Clone)]
pub struct Measurement {
    /// A total number of operations.
    pub total_ops: u64,
    /// Spent time.
    pub spent: Duration,
    /// A number of operations per second.
    pub throughput: f64,
    /// An average value of latency.
    pub latency: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct Mix {
    /// The percentage of operations in the mix that are reads.
    pub read: u8,
    /// The percentage of operations in the mix that are inserts.
    pub insert: u8,
    /// The percentage of operations in the mix that are removals.
    pub remove: u8,
    /// The percentage of operations in the mix that are updates.
    pub update: u8,
    /// The percentage of operations in the mix that are update-or-inserts.
    pub upsert: u8,
}

impl Mix {
    /// Constructs a very read-heavy workload (~95%), with limited concurrent modifications.
    pub fn read_heavy() -> Self {
        Self {
            read: 95,
            insert: 2,
            update: 1,
            remove: 1,
            upsert: 1,
        }
    }

    pub fn read_only() -> Self {
        Self {
            read: 100,
            insert: 0,
            update: 0,
            remove: 0,
            upsert: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Workload {
    /// The initial capacity of the table, specified as a power of 2.
    initial_cap_log2: u8,

    /// The fraction of the initial table capacity should we populate before running the benchmark.
    prefill_f: f64,

    /// Total number of operations as a multiple of the initial capacity.
    ops_f: f64,

    /// Number of threads to run the benchmark with.
    threads: usize,
}

impl Workload {
    /// Start building a new benchmark workload.
    pub fn new(threads: usize) -> Self {
        Self {
            initial_cap_log2: 25,
            prefill_f: 0.0,
            ops_f: 0.75,
            threads,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Read,
    Insert,
    Remove,
    Update,
    Upsert,
}

/// A collection that can be benchmarked by bustle.
///
/// Any thread that performs operations on the collection will first call `pin` and then perform
/// collection operations on the `Handle` that is returned. `pin` will not be called in the hot
/// loop of the benchmark.
pub trait Collection: Send + Sync + 'static {
    /// A thread-local handle to the concurrent collection under test.
    type Handle: CollectionHandle;

    /// Allocate a new instance of the benchmark target with the given capacity.
    fn with_capacity(capacity: usize) -> Self;

    /// Pin a thread-local handle to the concurrent collection under test.
    fn pin(&self) -> Self::Handle;
}

/// A handle to a key-value collection.
///
/// Note that for all these methods, the benchmarker does not dictate what the values are. Feel
/// free to use the same value for all operations, or use distinct ones and check that your
/// retrievals indeed return the right results.
pub trait CollectionHandle {
    /// The `u64` seeds used to construct `Key` (through `From<u64>`) are distinct.
    /// The returned keys must be as well.
    type Key: From<u64> + Clone;

    /// Perform a lookup for `key`.
    ///
    /// Should return `true` if the key is found.
    fn get(&mut self, key: &Self::Key) -> bool;

    /// Insert `key` into the collection.
    ///
    /// Should return `true` if no value previously existed for the key.
    fn insert(&mut self, key: Self::Key) -> bool;

    /// Remove `key` from the collection.
    ///
    /// Should return `true` if the key existed and was removed.
    fn remove(&mut self, key: &Self::Key) -> bool;

    /// Update the value for `key` in the collection, if it exists.
    ///
    /// Should return `true` if the key existed and was updated.
    ///
    /// Should **not** insert the key if it did not exist.
    fn update(&mut self, key: &Self::Key) -> bool;
}

fn run_thread<H: CollectionHandle>(
    tbl: &mut H,
    keys: &[H::Key],
    op_mix: &[Operation],
    ops: usize,
    prefilled: usize,
    barrier: Arc<Barrier>,
    run_checks: bool,
) where
    H::Key: std::fmt::Debug,
{
    // Invariant: erase_seq <= insert_seq
    // Invariant: insert_seq < numkeys
    let nkeys = keys.len();
    let mut erase_seq = 0;
    let mut insert_seq = prefilled;
    let mut find_seq = 0;

    // We're going to use a very simple LCG to pick random keys.
    // We want it to be _super_ fast so it doesn't add any overhead.
    assert!(nkeys.is_power_of_two());
    assert!(nkeys > 4);
    assert_eq!(op_mix.len(), 100);
    let a = nkeys / 2 + 1;
    let c = nkeys / 4 - 1;
    let find_seq_mask = nkeys - 1;

    // The elapsed time is measured by the lifetime of `workload_scope`.
    let workload_scope = scopeguard::guard(barrier, |barrier| {
        barrier.wait();
    });
    workload_scope.wait();

    for (i, op) in (0..((ops + op_mix.len() - 1) / op_mix.len()))
        .flat_map(|_| op_mix.iter())
        .enumerate()
    {
        if i == ops {
            break;
        }

        match op {
            Operation::Read => {
                let should_find = find_seq >= erase_seq && find_seq < insert_seq;
                let found = tbl.get(&keys[find_seq]);
                if find_seq >= erase_seq {
                    assert_eq!(
                        should_find, found,
                        "get({:?}) {} {} {}",
                        &keys[find_seq], find_seq, erase_seq, insert_seq
                    );
                } else {
                    // due to upserts, we may _or may not_ find the key
                }

                // Twist the LCG since we used find_seq
                find_seq = (a * find_seq + c) & find_seq_mask;
            }
            Operation::Insert => {
                let new_key = tbl.insert(keys[insert_seq].clone());

                if run_checks {
                    assert!(
                        new_key,
                        "insert({:?}) should insert a new value",
                        &keys[insert_seq]
                    );
                }
                insert_seq += 1;
            }
            Operation::Remove => {
                if erase_seq == insert_seq {
                    // If `erase_seq` == `insert_eq`, the table should be empty.
                    let removed = tbl.remove(&keys[find_seq]);

                    if run_checks {
                        assert!(
                            !removed,
                            "remove({:?}) succeeded on empty table",
                            &keys[find_seq]
                        );
                    }

                    // Twist the LCG since we used find_seq
                    find_seq = (a * find_seq + c) & find_seq_mask;
                } else {
                    let removed = tbl.remove(&keys[erase_seq]);
                    if run_checks {
                        assert!(removed, "remove({:?}) should succeed", &keys[erase_seq]);
                    }
                    erase_seq += 1;
                }
            }
            Operation::Update => {
                // Same as find, except we update to the same default value
                let should_exist = find_seq >= erase_seq && find_seq < insert_seq;
                let updated = tbl.update(&keys[find_seq]);
                if find_seq >= erase_seq {
                    if run_checks {
                        assert_eq!(should_exist, updated, "update({:?})", &keys[find_seq]);
                    }
                } else {
                    // due to upserts, we may or may not have updated an existing key
                }

                // Twist the LCG since we used find_seq
                find_seq = (a * find_seq + c) & find_seq_mask;
            }
            Operation::Upsert => {
                // Pick a number from the full distribution, but cap it to the insert_seq, so we
                // don't insert a number greater than insert_seq.
                let n = std::cmp::min(find_seq, insert_seq);

                // Twist the LCG since we used find_seq
                find_seq = (a * find_seq + c) & find_seq_mask;

                let _inserted = tbl.insert(keys[n].clone());
                if n == insert_seq {
                    insert_seq += 1;
                }
            }
        }
    }
}

pub fn run_workload<T: Collection>(
    name: &str,
    mix: Mix,
    workload: Workload,
    run_checks: bool,
    thread_affinity: bool,
) -> Measurement
where
    <T::Handle as CollectionHandle>::Key: Send + std::fmt::Debug,
{
    assert_eq!(
        mix.read + mix.insert + mix.remove + mix.update + mix.upsert,
        100,
        "mix fractions do not add up to 100%"
    );

    println!("============");
    println!("workload start {} {} threads", name, workload.threads);

    let initial_capacity = 1 << workload.initial_cap_log2;
    let total_ops = (initial_capacity as f64 * workload.ops_f) as usize;

    let mut rng = SmallRng::from_rng(thread_rng()).unwrap();

    println!("generating operation mix");
    let mut op_mix = Vec::with_capacity(100);
    op_mix.append(&mut vec![Operation::Read; usize::from(mix.read)]);
    op_mix.append(&mut vec![Operation::Insert; usize::from(mix.insert)]);
    op_mix.append(&mut vec![Operation::Remove; usize::from(mix.remove)]);
    op_mix.append(&mut vec![Operation::Update; usize::from(mix.update)]);
    op_mix.append(&mut vec![Operation::Upsert; usize::from(mix.upsert)]);
    op_mix.shuffle(&mut rng);

    let prefill = (initial_capacity as f64 * workload.prefill_f) as usize;

    // We won't be running through `op_mix` more than ceil(total_ops / 100), so calculate that
    // ceiling and multiply by the number of inserts and upserts to get an upper bound on how
    // many elements we'll be inserting.
    let max_insert_ops = (total_ops + 99) / 100 * usize::from(mix.insert + mix.upsert);
    let insert_keys = std::cmp::max(initial_capacity, max_insert_ops) + prefill;

    println!("generating key space {}", insert_keys);

    // Round this quantity up to a power of 2, so that we can use an LCG to cycle over the
    // array "randomly".
    let insert_keys_per_thread =
        ((insert_keys + workload.threads - 1) / workload.threads).next_power_of_two();
    let mut generators = Vec::new();
    for _ in 0..workload.threads {
        let mut thread_seed = [0u8; 32];
        rng.fill_bytes(&mut thread_seed[..]);
        generators.push(std::thread::spawn(move || {
            let mut rng: rand::rngs::SmallRng = rand::SeedableRng::from_seed(thread_seed);
            let mut keys: Vec<<T::Handle as CollectionHandle>::Key> =
                Vec::with_capacity(insert_keys_per_thread);
            keys.extend((0..insert_keys_per_thread).map(|_| rng.next_u64().into()));
            keys
        }));
    }
    let keys: Vec<_> = generators
        .into_iter()
        .map(|jh| jh.join().unwrap())
        .collect();

    println!("constructing initial table");
    let table = Arc::new(T::with_capacity(initial_capacity));

    // And fill it
    let prefill_per_thread = prefill / workload.threads;
    let mut prefillers = Vec::new();
    for keys in keys {
        let table = Arc::clone(&table);
        prefillers.push(std::thread::spawn(move || {
            let mut table = table.pin();
            for key in &keys[0..prefill_per_thread] {
                let inserted = table.insert(key.clone());
                assert!(inserted);
            }
            keys
        }));
    }
    let keys: Vec<_> = prefillers
        .into_iter()
        .map(|jh| jh.join().unwrap())
        .collect();

    println!("start threads");

    let core_ids = get_core_ids().expect("Failed to get core IDs");
    let ops_per_thread = total_ops / workload.threads;
    let op_mix = Arc::new(op_mix.into_boxed_slice());
    let barrier = Arc::new(Barrier::new(workload.threads + 1));
    let mut mix_threads = Vec::with_capacity(workload.threads);
    let mut n = 0;

    for keys in keys {
        let table = Arc::clone(&table);
        let op_mix = Arc::clone(&op_mix);
        let barrier = Arc::clone(&barrier);
        let core_id = core_ids[n % core_ids.len()];
        let core_id_usize: usize = core_id.id;

        mix_threads.push(std::thread::spawn(move || {
            if thread_affinity {
                set_thread_affinity(&[core_id_usize]).expect("Failed to set thread affinity");
            }

            let mut table = table.pin();
            run_thread(
                &mut table,
                &keys,
                &op_mix,
                ops_per_thread,
                prefill_per_thread,
                barrier,
                run_checks,
            )
        }));

        n += 1;
    }

    barrier.wait();
    let start = std::time::Instant::now();
    barrier.wait();
    let spent = start.elapsed();

    let _samples: Vec<_> = mix_threads
        .into_iter()
        .map(|jh| jh.join().unwrap())
        .collect();

    let avg = spent / total_ops as u32;
    println!(
        "workload complete in {:?} (ops: {}, avg: {:?})",
        spent, total_ops, avg
    );

    let total_ops = total_ops as u64;
    let threads = workload.threads as u32;

    Measurement {
        total_ops,
        spent,
        throughput: total_ops as f64 / spent.as_secs_f64(),
        latency: Duration::from_nanos((spent * threads).as_nanos() as u64 / total_ops),
    }
}

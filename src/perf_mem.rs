use rand::Rng;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::mem::transmute;
use std::os::raw::c_void;
use std::os::windows::ffi::OsStringExt;
use std::ptr::{self, null_mut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use winapi::shared::minwindef::DWORD;
use winapi::shared::winerror::ERROR_INSUFFICIENT_BUFFER;
use winapi::um::errhandlingapi::GetLastError;
use winapi::um::memoryapi::{VirtualAlloc, VirtualAllocExNuma, VirtualFree};
use winapi::um::processthreadsapi::{GetCurrentProcess, GetCurrentThread};
use winapi::um::processtopologyapi::SetThreadGroupAffinity;
use winapi::um::sysinfoapi::{GetSystemInfo, SYSTEM_INFO};
use winapi::um::winbase::{
    FormatMessageW, FORMAT_MESSAGE_FROM_SYSTEM, FORMAT_MESSAGE_IGNORE_INSERTS,
};
use winapi::um::winnt::{
    RelationNumaNode, RelationProcessorCore, GROUP_AFFINITY, HANDLE, MEM_COMMIT, MEM_RESERVE,
    PAGE_READWRITE, SYSTEM_LOGICAL_PROCESSOR_INFORMATION, SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
};
use winapi::um::heapapi::{HeapAlloc, HeapCreate, HeapDestroy, HeapFree};

use crate::perf::{self, calc_av_nanos};
use crate::perf_info::{get_last_error_message, GetLogicalProcessorInformationEx};

#[derive(Copy, Clone)]
struct CoreId {
    processor_group: u16,
    numa_mask: usize,
    core_mask: usize,
    numa_node_num: u32,
}

fn set_thread_affinity(core: &CoreId, numa_affinity: bool) -> Result<(), String> {
    let current_thread: HANDLE = unsafe { GetCurrentThread() };
    if current_thread.is_null() {
        return Err("Failed to get current thread handle".to_string());
    }

    let mask = if numa_affinity {
        core.numa_mask
    } else {
        core.core_mask
    };

    let mut group_mask: GROUP_AFFINITY = unsafe { std::mem::zeroed() };
    group_mask.Mask = mask;
    group_mask.Group = core.processor_group;

    let result = unsafe { SetThreadGroupAffinity(current_thread, &group_mask, ptr::null_mut()) };

    if result == 0 {
        return Err(format!(
            "Failed to set thread affinity for core {:X}:{:X}: {}",
            mask,
            core.processor_group,
            get_last_error_message(),
        ));
    }

    Ok(())
}

// distribute the ordering of CoreId evenly over the numa nodes
fn distribute_numa_cores(core_ids: Vec<CoreId>) -> Vec<CoreId> {
    let mut cores_by_numa = BTreeMap::<u32, Vec<CoreId>>::new();
    let mut rearranged_core_ids = Vec::new();

    for core_id in &core_ids {
        cores_by_numa
            .entry(core_id.numa_node_num)
            .or_insert(Vec::new())
            .push(core_id.clone());
    }

    while rearranged_core_ids.len() < core_ids.len() {
        for (_n, v) in cores_by_numa.iter_mut() {
            if let Some(c) = v.pop() {
                rearranged_core_ids.push(c);
            }
        }
    }

    rearranged_core_ids
}

pub fn get_num_cpus() -> usize {
    // Get system information
    let mut system_info: SYSTEM_INFO = unsafe { std::mem::zeroed() };
    unsafe { GetSystemInfo(&mut system_info) };
    return system_info.dwNumberOfProcessors as usize;
}

pub struct CoreInfo {
    pub ids: Vec<CoreId>,
    pub num_numa_nodes: usize,
}

pub fn get_core_info() -> Result<CoreInfo, String> {
    // Determine buffer size needed for GetLogicalProcessorInformationEx
    let mut core_infos = Vec::new();
    let mut numa_node_set = std::collections::HashSet::new();
    let relationship = RelationNumaNode;
    let mut buffer = vec![0u8; 1];
    let mut p_buffer_alloc = buffer.as_ptr();
    let p_buffer = p_buffer_alloc as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;
    let mut cb_buffer: DWORD = 1;

    let result =
        unsafe { GetLogicalProcessorInformationEx(relationship, p_buffer, &mut cb_buffer) };

    if result != 0 {
        // Unexpected success
        return Err(format!(
            "GetLogicalProcessorInformationEx returned nothing successfully."
        ));
    }

    let error = unsafe { GetLastError() };

    if error != ERROR_INSUFFICIENT_BUFFER {
        return Err(format!(
            "GetLogicalProcessorInformationEx returned error (1). GetLastError() = {}",
            error
        ));
    }

    // Allocate buffer
    let mut buffer = vec![0u8; cb_buffer as usize];
    let mut p_buffer_alloc = buffer.as_ptr();
    let mut p_buffer = p_buffer_alloc as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;

    let result =
        unsafe { GetLogicalProcessorInformationEx(relationship, p_buffer, &mut cb_buffer) };

    if result == 0 {
        return Err(format!(
            "GetLogicalProcessorInformationEx returned error (2). GetLastError() = {}",
            get_last_error_message()
        ));
    }

    let mut p_cur = p_buffer as *const u8;
    let p_end = unsafe { p_cur.add(cb_buffer as usize) };

    let mut idx = 0;
    while p_cur < p_end {
        let p_buffer_ref = unsafe { &*p_buffer };

        if p_buffer_ref.Relationship == relationship {
            let info = unsafe { &p_buffer_ref.u.NumaNode() };

            let processor_mask = info.GroupMask.Mask;
            let processor_group = info.GroupMask.Group;
            let numa_node_num = info.NodeNumber;

            for i in 0..64 {
                if (processor_mask & (1 << i)) != 0 {
                    core_infos.push(CoreId {
                        processor_group,
                        core_mask: 1 << i,
                        numa_mask: processor_mask,
                        numa_node_num,
                    });
                }
            }

            numa_node_set.insert(numa_node_num);
        }
        p_cur = unsafe { p_cur.add(p_buffer_ref.Size as usize) };
        p_buffer = p_cur as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;
        idx += 1;
    }

    Ok(CoreInfo {
        ids: distribute_numa_cores(core_infos),
        num_numa_nodes: numa_node_set.len(),
    })
}

#[derive(Clone)]
pub(crate) enum AffinityType {
    NoAffinity,
    NumaNodeAffinity,
    NumaMismatch,
}

pub(crate) fn run_independent_memory_access_test(
    name: &str,
    thread_count: usize,
    affinity: AffinityType,
    read_only: bool,
    block_size: usize,
) -> perf::Measurement {
    const TEST_LOOPS: u64 = 10_000_000;

    print!("Mem {name:8} (threads {thread_count:>3}) ... ");

    let core_info = get_core_info().expect("Failed to get core IDs");
    let results = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let barrier = Arc::new(Barrier::new(thread_count + 1));

    let mut handles = vec![];
    for n in 0..thread_count {
        let core_id = core_info.ids[n % core_info.ids.len()];
        let results_clone = Arc::clone(&results);
        let barrier = barrier.clone();
        let thread_affinity = affinity.clone();
        let num_numa_nodes = core_info.num_numa_nodes as u32;

        let handle = thread::spawn(move || {
            let memory_block_ptr = match thread_affinity {
                AffinityType::NoAffinity => unsafe {
                    VirtualAlloc(
                        null_mut(),
                        block_size,
                        MEM_COMMIT | MEM_RESERVE,
                        PAGE_READWRITE,
                    )
                },
                AffinityType::NumaNodeAffinity => {
                    set_thread_affinity(&core_id, true).expect("Failed to set thread affinity");
                    unsafe {
                        VirtualAllocExNuma(
                            GetCurrentProcess(),
                            ptr::null_mut(),
                            block_size,
                            winapi::um::winnt::MEM_COMMIT | winapi::um::winnt::MEM_RESERVE,
                            winapi::um::winnt::PAGE_READWRITE,
                            core_id.numa_node_num,
                        )
                    }
                }
                AffinityType::NumaMismatch => {
                    set_thread_affinity(&core_id, true).expect("Failed to set thread affinity");
                    unsafe {
                        let wrong_numa_node = (core_id.numa_node_num + 1) % num_numa_nodes;

                        VirtualAllocExNuma(
                            GetCurrentProcess(),
                            ptr::null_mut(),
                            block_size,
                            winapi::um::winnt::MEM_COMMIT | winapi::um::winnt::MEM_RESERVE,
                            winapi::um::winnt::PAGE_READWRITE,
                            wrong_numa_node,
                        )
                    }
                }
            };

            if memory_block_ptr.is_null() {
                panic!("Failed to allocate memory");
            }

            // place an array on that memory
            let mut u64_array = unsafe {
                std::slice::from_raw_parts_mut(
                    memory_block_ptr as *mut u64,
                    block_size / std::mem::size_of::<u64>(),
                )
            };

            // Fill the memory block with random u64 values
            let mut rng = rand::thread_rng();

            barrier.wait();

            let start_time = Instant::now();
            let mut reads_performed = 0;
            let mut writes_performed = 0;

            for _ in 0..TEST_LOOPS {
                let random_index = rng.gen_range(0..u64_array.len()) as usize;

                // Read
                let value = u64_array[random_index];
                std::hint::black_box(value);
                reads_performed += 1;

                if !read_only {
                    // Write
                    let random_index2 = rng.gen_range(0..u64_array.len()) as usize;
                    u64_array[random_index2] = writes_performed;
                    writes_performed += 1;
                }
            }

            let elapsed = start_time.elapsed();

            // Free the allocated memory for this thread
            unsafe {
                VirtualFree(memory_block_ptr, 0, winapi::um::winnt::MEM_RELEASE);
            }

            let mut results = results_clone.lock().unwrap();
            results.push(elapsed);
        });

        handles.push(handle);
    }

    barrier.wait();

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Calculate averages
    let op_count = thread_count as u64 * TEST_LOOPS * if read_only { 1 } else { 2 };
    let average_duration = calc_av_nanos(results, op_count);

    println!("avg: {:8.2} ns", average_duration);

    perf::Measurement {
        name,
        latency: average_duration,
        thread_count: thread_count as u64,
    }
}

pub(crate) fn run_fetch_add_test(
    name: &str,
    thread_count: usize,
    counter_count: usize,
) -> perf::Measurement {
    const INCREMENT_COUNT: u64 = 10_000_000;

    print!("fetch_add global (threads {thread_count:>3}) ... ");

    let atomic_counters: Vec<Arc<AtomicU64>> = (0..counter_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let barrier = Arc::new(Barrier::new(thread_count + 1));
    let results = Arc::new(Mutex::new(Vec::<Duration>::new()));

    let mut handles = vec![];
    for i in 0..thread_count {
        let atomic_counter_clone = Arc::clone(&atomic_counters[i % atomic_counters.len()]);
        let barrier = barrier.clone();
        let results_clone = Arc::clone(&results);

        let handle = thread::spawn(move || {
            barrier.wait();

            let start_time = Instant::now();

            loop {
                let current_value = atomic_counter_clone.fetch_add(1, Ordering::SeqCst);
                if current_value >= INCREMENT_COUNT - 1 {
                    let duration = start_time.elapsed();
                    let mut results = results_clone.lock().unwrap();
                    results.push(duration);
                    break;
                }
            }
        });

        handles.push(handle);
    }

    barrier.wait();

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Calculate statistics from the results
    let op_count: u64 = atomic_counters
        .iter()
        .map(|counter| unsafe { (**counter).load(Ordering::SeqCst) })
        .sum();

    let average_duration = calc_av_nanos(results, op_count);

    println!("avg: {:8.2} ns", average_duration);

    perf::Measurement {
        name,
        latency: average_duration,
        thread_count: thread_count as u64,
    }
}

pub(crate) fn run_mutex_test(
    name: &str,
    thread_count: usize,
    counter_count: usize,
) -> perf::Measurement {
    const INCREMENT_COUNT: u64 = 10_000_000;

    print!("mutex counter (threads {thread_count:>3}) ... ");

    // Use a Vec of mutexes instead of atomics
    let mutex_counters: Vec<Arc<Mutex<u64>>> = (0..counter_count)
        .map(|_| Arc::new(Mutex::new(0)))
        .collect();

    let barrier = Arc::new(Barrier::new(thread_count + 1));
    let results = Arc::new(Mutex::new(Vec::<Duration>::new()));

    let mut handles = vec![];
    for i in 0..thread_count {
        let mutex_counter_clone = Arc::clone(&mutex_counters[i % mutex_counters.len()]);
        let barrier = barrier.clone();
        let results_clone = Arc::clone(&results);

        let handle = thread::spawn(move || {
            barrier.wait();

            let start_time = Instant::now();

            loop {
                // Acquire the mutex lock to modify the counter
                let mut counter = mutex_counter_clone.lock().unwrap();
                *counter += 1; // Increment the counter
                let current_value = *counter;

                if current_value >= INCREMENT_COUNT {
                    let duration = start_time.elapsed();
                    let mut results = results_clone.lock().unwrap();
                    results.push(duration);
                    break;
                }
                // The lock is automatically released when `counter` goes out of scope
            }
        });

        handles.push(handle);
    }

    barrier.wait();

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Calculate statistics from the results
    let counter_total: u64 = mutex_counters
        .iter()
        .map(|counter| *counter.lock().unwrap())
        .sum();
    let average_duration = calc_av_nanos(results, counter_total);

    println!("avg: {:8.2} ns", average_duration);

    perf::Measurement {
        name,
        latency: average_duration,
        thread_count: thread_count as u64,
    }
}

fn allocate_atomic_u64_on_numa_nodes(num_numa_nodes: usize) -> Vec<*mut AtomicU64> {
    (0..num_numa_nodes)
        .map(|numa_node| {
            let ptr = unsafe {
                VirtualAllocExNuma(
                    GetCurrentProcess(),
                    ptr::null_mut(),
                    std::mem::size_of::<AtomicU64>(),
                    MEM_COMMIT | MEM_RESERVE,
                    PAGE_READWRITE,
                    numa_node as u32,
                )
            };

            if ptr.is_null() {
                panic!("Failed to allocate memory on NUMA node {}", numa_node);
            }

            let atomic_u64_ptr = ptr as *mut AtomicU64;
            unsafe {
                (*atomic_u64_ptr).store(0, Ordering::Relaxed);
            } // Initialize to 0

            atomic_u64_ptr
        })
        .collect()
}

pub(crate) fn run_numa_fetch_add_test(
    name: &str,
    thread_count: usize,
    numa_affinity: bool,
) -> perf::Measurement {
    const INCREMENT_COUNT: u64 = 10_000_000;

    print!("fetch_add numa (threads {thread_count:>3}) ... ");

    let core_info = get_core_info().expect("Failed to get core IDs");

    // Allocate an atomic counter per NUMA node
    let atomic_counters = allocate_atomic_u64_on_numa_nodes(core_info.num_numa_nodes);

    let barrier = Arc::new(Barrier::new(thread_count + 1));
    let results = Arc::new(Mutex::new(Vec::<Duration>::new()));

    let mut handles = vec![];
    for thread_index in 0..thread_count {
        let core_id = core_info.ids[thread_index % core_info.ids.len()];
        let numa_node = core_id.numa_node_num as usize;
        let counter_as_usize =
            unsafe { transmute::<*mut AtomicU64, usize>(atomic_counters[numa_node]) };
        let barrier = barrier.clone();
        let results_clone = Arc::clone(&results);

        let handle = thread::spawn(move || {
            set_thread_affinity(&core_id, numa_affinity).expect("Failed to set thread affinity");
            let counter_ptr = unsafe { transmute::<usize, *mut AtomicU64>(counter_as_usize) };

            barrier.wait();

            let start_time = Instant::now();

            loop {
                let current_value = (unsafe { &*counter_ptr }).fetch_add(1, Ordering::SeqCst);
                if current_value >= INCREMENT_COUNT - 1 {
                    let duration = start_time.elapsed();
                    let mut results = results_clone.lock().unwrap();
                    results.push(duration);
                    break;
                }
            }
        });

        handles.push(handle);
    }

    barrier.wait();

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    let counter_total: u64 = atomic_counters
        .iter()
        .map(|counter| unsafe { (**counter).load(Ordering::SeqCst) })
        .sum();

    let average_duration = calc_av_nanos(results, counter_total);

    println!("avg: {:8.2} ns", average_duration);

    perf::Measurement {
        name,
        latency: average_duration,
        thread_count: thread_count as u64,
    }
}


pub(crate) fn run_heapalloc_test(
    name: &str,
    thread_count: usize,
    num_heaps: usize
) -> perf::Measurement {
    const TEST_LOOPS: u64 = 10_000;
    const DELAY_FREE: usize = 100;

    print!("HeapAlloc {name:8} (threads {thread_count:>3}, heaps {num_heaps:>2}) ... ");

    let results = Arc::new(Mutex::new(Vec::<Duration>::new()));
    let barrier = Arc::new(Barrier::new(thread_count + 1));

    // Create the specified number of heaps
    let heaps: Vec<_> = (0..num_heaps)
        .map(|_| unsafe { HeapCreate(0, 0, 0) })
        .collect();

    let mut handles = vec![];
    for thread_id in 0..thread_count {
        let results_clone = Arc::clone(&results);
        let barrier = barrier.clone();
        let heap_index = thread_id % num_heaps;
        let heap_handle = heaps[heap_index];
        let heap_id: usize = heap_handle as usize; // Cast to usize

        let handle = thread::spawn(move || {
            // Assign a specific heap to this thread

            let heap = heap_id as *mut c_void;
            let start_time = Instant::now();
            let mut writes_performed = 0;
            let mut rng = rand::thread_rng();

            // Circular buffer for allocated blocks
            let mut allocated_blocks: [*mut c_void; DELAY_FREE] = [std::ptr::null_mut(); DELAY_FREE];
            let mut head: usize = 0;
            let mut tail: usize = 0;

            barrier.wait();

            for _ in 0..TEST_LOOPS {
                // Allocate memory from the assigned heap
                let block_size = rng.gen_range(16..=1024);
                let memory_block_ptr = unsafe { HeapAlloc(heap, 0, block_size) };

                if memory_block_ptr.is_null() {
                    panic!("Failed to allocate memory");
                }

                // Write a few values to the block (adjust as needed)
                let u32_array = unsafe {
                    std::slice::from_raw_parts_mut(
                        memory_block_ptr as *mut u32,
                        block_size / std::mem::size_of::<u32>(),
                    )
                };
                u32_array[0] = writes_performed as u32;
                writes_performed += 1;

                // Add the allocated block to the circular buffer
                allocated_blocks[tail] = memory_block_ptr;
                tail = (tail + 1) % DELAY_FREE;

                // If the buffer is full, free the oldest block
                if head == tail {
                    let block_to_free = allocated_blocks[head];
                    unsafe { HeapFree(heap, 0, block_to_free) };
                    head = (head + 1) % DELAY_FREE;
                }
            }

            while head != tail {
                let block_to_free = allocated_blocks[head];
                unsafe { HeapFree(heap, 0, block_to_free) };
                head = (head + 1) % DELAY_FREE;
            }

            let elapsed = start_time.elapsed();

            let mut results = results_clone.lock().unwrap();
            results.push(elapsed);
        });

        handles.push(handle);
    }

    barrier.wait();

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Destroy the heaps
    for heap in heaps {
        unsafe { HeapDestroy(heap) };
    }

    // Calculate averages
    let op_count = thread_count as u64 * TEST_LOOPS;
    let average_duration = calc_av_nanos(results, op_count);

    println!("avg: {:8.2} ns", average_duration);

    perf::Measurement {
        name,
        latency: average_duration,
        thread_count: thread_count as u64,
    }
}
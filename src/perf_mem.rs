use rand::Rng;
use std::ffi::OsString;
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

use crate::perf;
use crate::perf_info::{get_last_error_message, GetLogicalProcessorInformationEx};

#[derive(Copy, Clone)]
struct CoreId {
    processor_group: u16,
    processor_mask: usize,
    numa_node_num: u32,
}

fn set_thread_affinity(core: &CoreId) -> Result<(), String> {
    let current_thread: HANDLE = unsafe { GetCurrentThread() };
    if current_thread.is_null() {
        return Err("Failed to get current thread handle".to_string());
    }

    let mut group_mask: GROUP_AFFINITY = unsafe { std::mem::zeroed() };
    group_mask.Mask = core.processor_mask;
    group_mask.Group = core.processor_group;

    let result = unsafe { SetThreadGroupAffinity(current_thread, &group_mask, ptr::null_mut()) };

    if result == 0 {
        return Err(format!(
            "Failed to set thread affinity for core {:X}:{:X}: {}",
            core.processor_mask,
            core.processor_group,
            get_last_error_message(),
        ));
    }

    Ok(())
}

pub fn get_num_cpus() -> usize {
    // Get system information
    let mut system_info: SYSTEM_INFO = unsafe { std::mem::zeroed() };
    unsafe { GetSystemInfo(&mut system_info) };
    return system_info.dwNumberOfProcessors as usize;
}

struct CoreInfo {
    ids: Vec<CoreId>,
    num_numa_nodes: usize,
}

fn get_core_info() -> Result<CoreInfo, String> {
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
                        processor_mask: 1 << i,
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
        ids: core_infos,
        num_numa_nodes: numa_node_set.len(),
    })
}

#[derive(Clone)] 
pub(crate) enum AffinityType {
    NoAffinity,
    NumaNodeAffinity,
    NumaMismatch,
}

pub(crate) fn run_memory_access_test(
    name: &str,
    thread_count: usize,
    affinity: AffinityType,
) -> perf::Measurement {
    const TEST_SECONDS: u64 = 2;
    const BLOCK_SIZE: usize = 8 * 1024 * 1024;
    const TEST_DURATION: Duration = Duration::from_secs(TEST_SECONDS);

    print!("Mem {name:8} (threads {thread_count:>3}) ... ");

    let core_info = get_core_info().expect("Failed to get core IDs");
    let results = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));
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
                        BLOCK_SIZE,
                        MEM_COMMIT | MEM_RESERVE,
                        PAGE_READWRITE,
                    )
                },
                AffinityType::NumaNodeAffinity => {
                    set_thread_affinity(&core_id).expect("Failed to set thread affinity");
                    unsafe {
                        VirtualAllocExNuma(
                            GetCurrentProcess(),
                            ptr::null_mut(),
                            BLOCK_SIZE,
                            winapi::um::winnt::MEM_COMMIT | winapi::um::winnt::MEM_RESERVE,
                            winapi::um::winnt::PAGE_READWRITE,
                            core_id.numa_node_num,
                        )
                    }
                }
                AffinityType::NumaMismatch => {
                    set_thread_affinity(&core_id).expect("Failed to set thread affinity");
                    unsafe {
                        let wrong_numa_node = (core_id.numa_node_num + 1) % num_numa_nodes;

                        VirtualAllocExNuma(
                            GetCurrentProcess(),
                            ptr::null_mut(),
                            BLOCK_SIZE,
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
                std::slice::from_raw_parts_mut(memory_block_ptr as *mut u64, BLOCK_SIZE / std::mem::size_of::<u64>())
            };

            // Fill the memory block with random u64 values
            let mut rng = rand::thread_rng();

            barrier.wait();

            let start_time = Instant::now();
            let mut reads_performed = 0;
            let mut writes_performed = 0;

            while start_time.elapsed() < TEST_DURATION {
                // 50% chance of read, 50% chance of write
                if rng.gen_bool(0.5) {
                    // Read
                    let random_index = rng.gen_range(0..u64_array.len()) as usize;
                    let value = u64_array[random_index];
                    std::hint::black_box(value);
                    reads_performed += 1;
                } else {
                    // Write
                    let random_index = rng.gen_range(0..u64_array.len()) as usize;
                    let new_value: u64 = rng.gen();
                    u64_array[random_index] = new_value;
                    writes_performed += 1;
                }
            }

            // Free the allocated memory for this thread
            unsafe {
                VirtualFree(memory_block_ptr, 0, winapi::um::winnt::MEM_RELEASE);
            }

            let mut results = results_clone.lock().unwrap();
            results.push((reads_performed, writes_performed));
        });

        handles.push(handle);
    }

    barrier.wait();

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Calculate averages
    const NANOS_IN_1_SEC: u64 = 1_000_000_000u64;

    let results = results.lock().unwrap();
    let total_reads: u64 = results.iter().map(|m| m.0).sum();
    let total_writes: u64 = results.iter().map(|m| m.1).sum();
    let latency =
        (NANOS_IN_1_SEC * TEST_SECONDS * thread_count as u64) / (total_reads + total_writes);

    println!("avg: {} ns", latency);

    perf::Measurement {
        name,
        latency,
        thread_count: thread_count as u64,
    }
}


pub(crate) fn run_fetch_add_test(
    name: &str,
    thread_count: usize,
) -> perf::Measurement {
    const INCREMENT_COUNT: u64 = 100_000_000;

    print!("CAS (threads {thread_count:>3}) ... ");

    let atomic_counter = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(thread_count + 1));
    let results = Arc::new(Mutex::new(Vec::<u128>::new())); 

    let mut handles = vec![];
    for _ in 0..thread_count {
        let atomic_counter_clone = Arc::clone(&atomic_counter);
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
                    results.push(duration.as_nanos());
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
    let current_value = atomic_counter.load(Ordering::Acquire);
    let results = results.lock().unwrap();
    let total_duration: u128 = results.iter().sum();
    let average_duration = total_duration / current_value as u128;

    println!("avg: {} ns", average_duration);

    perf::Measurement {
        name,
        latency: average_duration as u64, 
        thread_count: thread_count as u64,
    }
}
use affinity::set_thread_affinity;
use core_affinity::get_core_ids;
use rand::Rng;
use std::ptr::null_mut;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use winapi::um::memoryapi::{VirtualAlloc, VirtualFree};
use winapi::um::winnt::{MEM_COMMIT, MEM_RESERVE, PAGE_READWRITE};

use crate::perf;


pub(crate) fn run_memory_access_test(name : &'static str, thread_count: usize, affinity : bool) -> perf::Measurement {
    const BLOCK_SIZE: usize = 8 * 1024 * 1024; // 8 MB
    const TEST_DURATION: Duration = Duration::from_secs(1);

    let core_ids = get_core_ids().expect("Failed to get core IDs");
    let results = Arc::new(Mutex::new(Vec::<(u64,u64)>::new()));
    let barrier = Arc::new(Barrier::new(thread_count + 1));

    // Create and run threads
    let mut handles = vec![];
    for n in 0..thread_count {

        let core_id = core_ids[n % core_ids.len()];
        let core_id_usize = core_id.id as usize;
        let results_clone = Arc::clone(&results);
        let barrier = barrier.clone();

        let handle = thread::spawn(move || {

            if (affinity)
            {
                set_thread_affinity(&[core_id_usize]).expect("Failed to set thread affinity");
            }

            // Allocate memory for this thread using VirtualAlloc
            let memory_block_ptr = unsafe {
                VirtualAlloc(
                    null_mut(),
                    BLOCK_SIZE,
                    MEM_COMMIT | MEM_RESERVE,
                    PAGE_READWRITE,
                )
            };

            if memory_block_ptr.is_null() {
                panic!("Failed to allocate memory");
            }

            // Create a slice from the raw pointer (unsafe)
            let memory_block: &mut [u8] =
                unsafe { std::slice::from_raw_parts_mut(memory_block_ptr as *mut u8, BLOCK_SIZE) };

            // Fill the memory block with random u64 values
            let mut rng = rand::thread_rng();
            for chunk in memory_block.chunks_exact_mut(8) {
                let random_u64: u64 = rng.gen();
                chunk.copy_from_slice(&random_u64.to_le_bytes());
            }

            barrier.wait();

            let start_time = Instant::now();
            let mut reads_performed = 0;
            let mut writes_performed = 0;

            while start_time.elapsed() < TEST_DURATION {
                // 50% chance of read, 50% chance of write
                if rng.gen_bool(0.5) {
                    // Read
                    let random_index = (rng.gen_range(0..(BLOCK_SIZE / 8)) * 8) as usize;
                    let value_bytes = &memory_block[random_index..random_index + 8];
                    let value = u64::from_le_bytes(value_bytes.try_into().unwrap());
                    std::hint::black_box(value);
                    reads_performed += 1;
                } else {
                    // Write
                    let random_index = (rng.gen_range(0..(BLOCK_SIZE / 8)) * 8) as usize;
                    let new_value: u64 = rng.gen();
                    memory_block[random_index..random_index + 8]
                        .copy_from_slice(&new_value.to_le_bytes());
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
    let results = results.lock().unwrap();
    let total_reads: u64 = results.iter().map(|m| m.0).sum();
    let total_writes: u64 = results.iter().map(|m| m.1).sum();

    perf::Measurement {
        name,
        total: (total_reads + total_writes) / (thread_count as u64),
        thread_count: thread_count as u64,
    }
}

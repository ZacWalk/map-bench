use std::alloc::{GlobalAlloc, Layout};
use std::os::raw::c_void;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};
use winapi::um::heapapi::{HeapAlloc, HeapCreate, HeapDestroy, HeapFree, HeapReAlloc};
use winapi::um::processthreadsapi::GetCurrentThread;
use winapi::um::processtopologyapi::GetThreadGroupAffinity;
use winapi::um::winnt::{GROUP_AFFINITY, HANDLE, HEAP_ZERO_MEMORY};

const MAX_NUMA_NODES: usize = 8;
const COOKIE_SIZE: usize = std::mem::size_of::<HeapCookie>();

struct HeapCookie {
    heap_handle: winapi::um::winnt::HANDLE,
    allocated_ptr: *mut u8,
}

// Global allocator implementation
struct NumaAwareAllocator {
    heaps: [AtomicPtr<c_void>; MAX_NUMA_NODES],
}

unsafe impl GlobalAlloc for NumaAwareAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Get the NUMA node of the current thread
        let numa_node = get_current_thread_numa_node();
        let heap = self.get_heap(numa_node);
        raw_allocate(layout, heap, false)
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        if ptr != null_mut() {
            // Get the cookie from the allocation
            let cookie_ptr = ptr.sub(COOKIE_SIZE) as *mut HeapCookie;
            let heap_handle = (*cookie_ptr).heap_handle;
            let allocated_ptr = (*cookie_ptr).allocated_ptr;

            // Free the memory using the correct heap
            HeapFree(heap_handle, 0, allocated_ptr.cast::<c_void>());
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let numa_node = get_current_thread_numa_node();
        let heap = self.get_heap(numa_node);
        raw_allocate(layout, heap, true)
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let cookie_ptr = ptr.sub(COOKIE_SIZE) as *mut HeapCookie;
        let heap_handle = (*cookie_ptr).heap_handle;
        let old_allocated_ptr = (*cookie_ptr).allocated_ptr;

        let offset = ptr.offset_from(old_allocated_ptr) as usize;

        let alignment = layout.align();
        let aligned_new_size = new_size + alignment + COOKIE_SIZE;
        let allocated_ptr = unsafe { HeapReAlloc(heap_handle, 0, old_allocated_ptr.cast::<c_void>(), aligned_new_size).cast::<u8>() };
        let new_ptr = allocated_ptr.add(offset);

        // Store the cookie at the beginning
        let cookie_ptr = new_ptr.sub(COOKIE_SIZE).cast::<HeapCookie>();
        (*cookie_ptr).heap_handle = heap_handle;
        (*cookie_ptr).allocated_ptr = allocated_ptr;

        new_ptr
    }
}

#[inline]
unsafe fn raw_allocate(
    layout: Layout,
    heap: *mut std::ffi::c_void,
    zeroed: bool,
) -> *mut u8 {
    let flags = if zeroed { HEAP_ZERO_MEMORY } else { 0 };

    // Calculate the total size needed, including space for alignment padding if necessary
    let total_size = layout.size() + COOKIE_SIZE;
    let alignment = layout.align();
    let alignment_mask = if alignment == 0 { 0 } else { alignment - 1 };
    let alignment_padding = alignment;
    // Maximum possible padding needed
    let aligned_total_size = total_size + alignment_padding;

    // Allocate memory
    let allocated_ptr = HeapAlloc(heap, flags, aligned_total_size).cast::<u8>();
    if allocated_ptr.is_null() {
        return std::ptr::null_mut();
    }

    // Calculate the aligned pointer for the payload, skipping over the cookie
    let raw_addr = allocated_ptr.add(COOKIE_SIZE) as usize;
    let aligned_addr = (raw_addr + alignment_mask) & !alignment_mask;
    let aligned_ptr = aligned_addr as *mut u8;

    // Store the cookie at the beginning
    let cookie_ptr = aligned_ptr.sub(COOKIE_SIZE).cast::<HeapCookie>();
    (*cookie_ptr).heap_handle = heap;
    (*cookie_ptr).allocated_ptr = allocated_ptr;

    aligned_ptr
}

impl NumaAwareAllocator {

    #[inline]
    fn get_heap(&self, numa_node: usize) -> *mut std::ffi::c_void {
        // Attempt to initialize the heap pointer if it's null
        let mut current_heap_ptr = self.heaps[numa_node].load(Ordering::Relaxed);
        while current_heap_ptr.is_null() {
            let new_heap_ptr = create_heap();
            let result = self.heaps[numa_node].compare_exchange_weak(
                std::ptr::null_mut(),
                new_heap_ptr,
                Ordering::SeqCst,
                Ordering::Relaxed,
            );

            match result {
                Ok(_) => {
                    // Successfully initialized the heap pointer
                    current_heap_ptr = new_heap_ptr;
                    break;
                }
                Err(actual) => {
                    // Another thread already initialized it, use that value
                    delete_heap(new_heap_ptr);
                    current_heap_ptr = actual;
                }
            }
        }
        current_heap_ptr
    }
}

const NULL_PTR: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());

// Initialize the global allocator
#[global_allocator]
static ALLOCATOR: NumaAwareAllocator = NumaAwareAllocator {
    heaps: [NULL_PTR; MAX_NUMA_NODES],
};

#[inline]
fn get_current_thread_numa_node() -> usize {
    unsafe {
        let mut group_affinity: GROUP_AFFINITY = std::mem::zeroed();
        let result = GetThreadGroupAffinity(GetCurrentThread(), &mut group_affinity);

        if result == 0 {
            // Handle the error appropriately, perhaps by returning a default value or panicking
            panic!("GetThreadGroupAffinity failed");
        }

        // Extract the NUMA node from the group affinity mask
        // Long term need to check if each group maps to a NUMA node
        let numa_node_mask = group_affinity.Mask;
        numa_node_mask.trailing_zeros() as usize % MAX_NUMA_NODES
    }
}

#[inline]
fn create_heap() -> HANDLE {
    unsafe {
        let heap_handle = HeapCreate(0, 0, 0);

        if heap_handle.is_null() {
            panic!("HeapCreate failed for NUMA node");
        }

        heap_handle
    }
}

#[inline]
fn delete_heap(heap_ptr: HANDLE) {
    unsafe {
        if HeapDestroy(heap_ptr) == 0 {
            // Error handling: HeapDestroy failed
            // You might want to panic, log the error, or return a Result
            panic!("HeapDestroy failed!");
        }
    }
}

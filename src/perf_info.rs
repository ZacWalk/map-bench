use std::ffi::{c_void, OsString};
use std::mem::{size_of, transmute};
use std::os::windows::ffi::OsStringExt;
use std::ptr::{self, null_mut, NonNull};

use winapi::shared::basetsd::ULONG_PTR;
use winapi::shared::minwindef::DWORD;
use winapi::shared::winerror::ERROR_INSUFFICIENT_BUFFER;
use winapi::um::errhandlingapi::GetLastError;
use winapi::um::winbase::{FormatMessageW, FORMAT_MESSAGE_FROM_SYSTEM, FORMAT_MESSAGE_IGNORE_INSERTS};
use winapi::um::winnt::{
    RelationAll, RelationCache, RelationNumaNode, RelationProcessorCore, RelationProcessorPackage, GROUP_AFFINITY, SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX
};

use winapi::um::winnt::{
    CACHE_RELATIONSHIP, GROUP_RELATIONSHIP, NUMA_NODE_RELATIONSHIP, PROCESSOR_GROUP_INFO,
    PROCESSOR_RELATIONSHIP,
};

pub fn get_last_error_message() -> String {
    unsafe {
        let error_code = GetLastError();
        let mut buffer: Vec<u16> = Vec::with_capacity(512); // Adjust capacity as needed

        let len = FormatMessageW(
            FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
            ptr::null(),
            error_code,
            0,
            buffer.as_mut_ptr(),
            buffer.capacity() as u32,
            ptr::null_mut(),
        );

        if len > 0 {
            buffer.set_len(len as usize);
            OsString::from_wide(&buffer[..len as usize - 1]) // Remove trailing null
                .into_string()
                .unwrap_or_else(|_| format!("Error code: {}", error_code))
        } else {
            format!("Error code: {} (Failed to retrieve message)", error_code)
        }
    }
}

fn yesno(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "no"
    }
}

fn print_bitmap(mask: ULONG_PTR) {
    for i in (0..size_of::<ULONG_PTR>() * 8).rev() {
        print!("{}", (mask >> i) & 1);
    }
}

fn print_group_affinity(affinity: &GROUP_AFFINITY) {
    print!(" Group #{} = ", affinity.Group);
    print_bitmap(affinity.Mask);
    println!();
}

#[link(name = "kernel32")]
extern "system" {
    pub fn GetLogicalProcessorInformationEx(
        RelationshipType: winapi::um::winnt::LOGICAL_PROCESSOR_RELATIONSHIP,
        Buffer: winapi::um::winnt::PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
        ReturnedLength: *mut winapi::shared::ntdef::ULONG,
    ) -> u32; // Returns BOOL (which is an alias for i32)
}

pub fn print_cpu_info() {
    let mut buffer = vec![0u8; 1];
    let mut p_buffer_alloc = buffer.as_ptr();
    let p_buffer = p_buffer_alloc as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;
    let mut cb_buffer: DWORD = 1;

    let result = unsafe {
        GetLogicalProcessorInformationEx(RelationAll, p_buffer, &mut cb_buffer)
    };

    if result != 0 {
        // Unexpected success
        println!("GetLogicalProcessorInformationEx returned nothing successfully.");
        return;
    }

    println!(
        "GetLogicalProcessorInformationEx needs {} byte data.",
        cb_buffer
    );

    let error = unsafe { GetLastError() };

    if error != ERROR_INSUFFICIENT_BUFFER {
        println!(
            "GetLogicalProcessorInformationEx returned error (1). GetLastError() = {}",
            error
        );
        return;
    }

    // Allocate buffer
    let mut buffer = vec![0u8; cb_buffer as usize];
    let mut p_buffer_alloc = buffer.as_ptr();
    let mut p_buffer = p_buffer_alloc as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;

    let result = unsafe {
        GetLogicalProcessorInformationEx(RelationAll, p_buffer, &mut cb_buffer)
    };

    if result == 0 {
        println!(
            "GetLogicalProcessorInformationEx returned error (2). GetLastError() = {}",
            get_last_error_message()
        );
        return;
    }

    println!(
        "GetLogicalProcessorInformationEx returned {} byte data.",
        cb_buffer
    );

    let mut p_cur = p_buffer as *const u8;
    let p_end = unsafe { p_cur.add(cb_buffer as usize) };

    let mut idx = 0;
    while p_cur < p_end {
        let p_buffer_ref = unsafe { &*p_buffer };
        println!();
        println!("Info #{}:", idx);

        let relationship_list = [
            "ProcessorCore",
            "NumaNode",
            "Cache",
            "ProcessorPackage",
            "Group",
        ];
        let relationship_str = if p_buffer_ref.Relationship < relationship_list.len() as u32 {
            relationship_list[p_buffer_ref.Relationship as usize]
        } else {
            "(reserved)"
        };

        println!(
            " Relationship = {} ({})",
            relationship_str, p_buffer_ref.Relationship
        );

        match p_buffer_ref.Relationship {
            RelationProcessorCore | RelationProcessorPackage => {
                let info = unsafe { &p_buffer_ref.u.Processor() };
                if p_buffer_ref.Relationship == RelationProcessorCore {
                    println!(
                        " SMT Support = {}",
                        yesno(info.Flags == winapi::um::winnt::LTP_PC_SMT)
                    );
                    println!(" Efficiency Class = {}", info.EfficiencyClass);
                }
                println!(" GroupCount = {}", info.GroupCount);
                for i in 0..info.GroupCount {
                    print_group_affinity(unsafe { &info.GroupMask[i as usize] });
                }
            }
            RelationNumaNode => {
                let info = unsafe { &p_buffer_ref.u.NumaNode() };
                println!(" Numa Node Number = {}", info.NodeNumber);
                print_group_affinity(&info.GroupMask);
            }
            RelationCache => {
                let info = unsafe { &p_buffer_ref.u.Cache() };
                let cachetype_list = ["Unified", "Instruction", "Data", "Trace"];
                let cachetype_str = if info.Type < cachetype_list.len() as u32 {
                    cachetype_list[info.Type as usize]
                } else {
                    "(reserved)"
                };
                println!(" Type = L{} {}", info.Level, cachetype_str);
                print!(" Assoc = ");
                if info.Associativity == 0xff {
                    println!("full");
                } else {
                    println!("{}", info.Associativity);
                }
                println!(" Line Size = {}B", info.LineSize);
                println!(" Cache Size = {}KB", info.CacheSize / 1024);
                print_group_affinity(&info.GroupMask);
            }
            RelationGroup => {
                let info = unsafe { &p_buffer_ref.u.Group() };
                println!(" Max Group Count = {}", info.MaximumGroupCount);
                println!(" Active Group Count = {}", info.ActiveGroupCount);
                for i in 0..info.ActiveGroupCount {
                    let ginfo = unsafe { &info.GroupInfo[i as usize] };
                    println!(" Group #{}:", i);
                    println!("  Max Processor Count = {}", ginfo.MaximumProcessorCount);
                    println!("  Active Processor Count = {}", ginfo.ActiveProcessorCount);
                    print!("  Active Processor Mask = ");
                    print_bitmap(ginfo.ActiveProcessorMask);
                    println!();
                }
            }
            _ => {} // Handle other or reserved relationships if needed
        }

        p_cur = unsafe { p_cur.add(p_buffer_ref.Size as usize) };
        p_buffer = p_cur as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;
        idx += 1;
    }
}

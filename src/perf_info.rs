use std::ffi::{c_void, OsString};
use std::fs::File;
use std::io::Write;
use std::mem::{size_of, transmute};
use std::os::windows::ffi::OsStringExt;
use std::ptr::{self, null_mut, NonNull};

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

fn format_bitmap(mask: usize) -> String {  // Assuming ULONG_PTR is usize
    let mut result = String::new();
    for i in (0..size_of::<usize>() * 8).rev() {
        result.push_str(&format!("{}", (mask >> i) & 1));
    }
    result
}

#[link(name = "kernel32")]
extern "system" {
    pub fn GetLogicalProcessorInformationEx(
        RelationshipType: winapi::um::winnt::LOGICAL_PROCESSOR_RELATIONSHIP,
        Buffer: winapi::um::winnt::PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
        ReturnedLength: *mut winapi::shared::ntdef::ULONG,
    ) -> u32; // Returns BOOL (which is an alias for i32)
}

pub fn write_cpu_info() {

    let mut file = File::create("processor.info.txt").expect("Failed to create file");


    let mut buffer = vec![0u8; 1];
    let mut p_buffer_alloc = buffer.as_ptr();
    let p_buffer = p_buffer_alloc as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;
    let mut cb_buffer: DWORD = 1;

    let result = unsafe {
        GetLogicalProcessorInformationEx(RelationAll, p_buffer, &mut cb_buffer)
    };

    if result != 0 {
        // Unexpected success
        writeln!(file, "GetLogicalProcessorInformationEx returned nothing successfully.");
        return;
    }

    writeln!(file, 
        "GetLogicalProcessorInformationEx needs {} byte data.",
        cb_buffer
    );

    let error = unsafe { GetLastError() };

    if error != ERROR_INSUFFICIENT_BUFFER {
        writeln!(file, 
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
        writeln!(file, 
            "GetLogicalProcessorInformationEx returned error (2). GetLastError() = {}",
            get_last_error_message()
        );
        return;
    }

    writeln!(file, 
        "GetLogicalProcessorInformationEx returned {} byte data.",
        cb_buffer
    );

    let mut p_cur = p_buffer as *const u8;
    let p_end = unsafe { p_cur.add(cb_buffer as usize) };

    let mut idx = 0;
    while p_cur < p_end {
        let p_buffer_ref = unsafe { &*p_buffer };

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

        writeln!(file, 
            "{} ({}) ",
            relationship_str, p_buffer_ref.Relationship
        );

        match p_buffer_ref.Relationship {
            RelationProcessorCore | RelationProcessorPackage => {
                let info = unsafe { &p_buffer_ref.u.Processor() };
                if p_buffer_ref.Relationship == RelationProcessorCore {
                    writeln!(file, 
                        " SMT Support: {}",
                        yesno(info.Flags == winapi::um::winnt::LTP_PC_SMT)
                    );
                    writeln!(file, " Efficiency Class: {}", info.EfficiencyClass);
                }
                writeln!(file, " GroupCount {}", info.GroupCount);
                for i in 0..info.GroupCount {
                    let g = unsafe { &info.GroupMask[i as usize] };
                    writeln!(file, " Group #{} = {}", g.Group, format_bitmap(g.Mask));
                }
            }
            RelationNumaNode => {
                let info = unsafe { &p_buffer_ref.u.NumaNode() };
                writeln!(file, " Numa Node: {}", info.NodeNumber);
                writeln!(file, " Group #{} = {}", info.GroupMask.Group, format_bitmap(info.GroupMask.Mask));
            }
            RelationCache => {
                let info = unsafe { &p_buffer_ref.u.Cache() };
                let cachetype_list = ["Unified", "Instruction", "Data", "Trace"];
                let cachetype_str = if info.Type < cachetype_list.len() as u32 {
                    cachetype_list[info.Type as usize]
                } else {
                    "(reserved)"
                };
                writeln!(file, " L{} {}", info.Level, cachetype_str);
                writeln!(file, " Assoc: ");
                if info.Associativity == 0xff {
                    writeln!(file, "full");
                } else {
                    writeln!(file, "{}", info.Associativity);
                }
                writeln!(file, " Line Size = {}B", info.LineSize);
                writeln!(file, " Cache Size = {}KB", info.CacheSize / 1024);
                writeln!(file, " Group #{} = {}", info.GroupMask.Group, format_bitmap(info.GroupMask.Mask));
            }
            RelationGroup => {
                let info = unsafe { &p_buffer_ref.u.Group() };
                writeln!(file, " Max Group Count = {}", info.MaximumGroupCount);
                writeln!(file, " Active Group Count = {}", info.ActiveGroupCount);
                for i in 0..info.ActiveGroupCount {
                    let ginfo = unsafe { &info.GroupInfo[i as usize] };
                    writeln!(file, " Group #{}:", i);
                    writeln!(file, "  Max Processor Count = {}", ginfo.MaximumProcessorCount);
                    writeln!(file, "  Active Processor Count = {}", ginfo.ActiveProcessorCount);
                    writeln!(file, "  Active Processor Mask = ");
                    writeln!(file, "  Mask = {}", format_bitmap(ginfo.ActiveProcessorMask));
                }
            }
            _ => {} // Handle other or reserved relationships if needed
        }

        writeln!(file, );

        p_cur = unsafe { p_cur.add(p_buffer_ref.Size as usize) };
        p_buffer = p_cur as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX;
        idx += 1;
    }
}

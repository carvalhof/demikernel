//======================================================================================================================
// Imports
//======================================================================================================================

use crate::{
    QDesc,
    runtime::{
        fail::Fail,
        libdpdk::{
            rte_hash,
            rte_lcore_id,
            rte_hash_crc,
            rte_hash_create,
            rte_hash_parameters,
            rte_socket_id,
            rte_hash_add_key_with_hash_data,
            rte_hash_lookup_with_hash_data,
        },
        network::NetworkRuntime,
    },
    inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock,
};

use std::{
    hash::{
        Hash, 
        Hasher,
        DefaultHasher,
    },
    os::raw::c_void,
};

//==============================================================================
// Constants
//==============================================================================

const MAX_ENTRIES: u32 = 128;

//======================================================================================================================
// Structures
//======================================================================================================================
pub struct DPDKHashMap {
    hash: *mut rte_hash,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================
impl DPDKHashMap {
    pub fn new() -> Self {
        let hash: *mut rte_hash = unsafe {
            fn cast_c(f: unsafe fn(*const c_void, u32, u32) -> u32) -> unsafe extern "C" fn(*const c_void, u32, u32) -> u32 {
                unsafe { std::mem::transmute(f) }
            }

            let id = rte_lcore_id();
            let name = format!("DPDK_hashmap_{:?}", id);

            let params = rte_hash_parameters {
                name: name.as_ptr() as *const i8,
                entries: MAX_ENTRIES,
                reserved: 0,
                key_len: std::mem::size_of::<QDesc>() as u32,
                hash_func: Some(cast_c(rte_hash_crc)),
                hash_func_init_val: 0,
                socket_id: rte_socket_id() as i32,
                extra_flag: 0,
            };

            let ptr = rte_hash_create(&params);

            if ptr.is_null() {
                panic!("Could not allocate");
            }

            ptr
        };

        Self { hash }
    }

    pub fn insert<T: NetworkRuntime>(&self, key: QDesc, value: *mut SharedControlBlock<T>) -> Result<(), Fail> {
        let hash_value: u32 = {
            let mut state: DefaultHasher = DefaultHasher::default();
            key.hash(&mut state);
            state.finish() as u32
        };

        let ret: i32 = unsafe { rte_hash_add_key_with_hash_data(self.hash, &key as *const _ as *mut c_void, hash_value, value as *mut c_void) };

        if ret == 0 {
            Ok(())
        } else {
            panic!("Could not allocate")
        }
    }

    pub fn get_mut<T: NetworkRuntime>(&self, key: QDesc) -> Option<*mut SharedControlBlock<T>> {
        let hash_value: u32 = {
            let mut state: DefaultHasher = DefaultHasher::default();
            key.hash(&mut state);
            state.finish() as u32
        };

        let value: *mut c_void = std::ptr::null_mut();
        let value_ptr: *mut *mut c_void = &value as *const _ as *mut *mut c_void;
        let ret: i32 = unsafe { rte_hash_lookup_with_hash_data(self.hash, &key as *const _ as *mut c_void, hash_value, value_ptr) };

        if ret < 0 {
            None 
        } else {
            Some(unsafe { *value_ptr as *mut SharedControlBlock<T> })
        }
    }
}

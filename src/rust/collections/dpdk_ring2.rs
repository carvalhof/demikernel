//======================================================================================================================
// Imports
//======================================================================================================================

use crate::runtime::{
    fail::Fail,
    libdpdk::{
        rte_socket_id,
        rte_ring,
        rte_ring_create,
        rte_ring_enqueue,
        rte_ring_dequeue,
    },
};

use ::std::{
    ptr::null_mut,
    os::raw::c_void,
};

//======================================================================================================================
// Structures
//======================================================================================================================
pub struct DPDKRing2 {
    ring: *mut rte_ring,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================
impl DPDKRing2 {
    pub fn new(name: String, count: u32) -> Self {
        let ring: *mut rte_ring = unsafe {
            let ptr = rte_ring_create(name.as_bytes().as_ptr() as *const i8, count, rte_socket_id().try_into().unwrap(), 0);

            if ptr.is_null() {
                panic!("Could not allocate the DPDKRing2");
            }

            ptr
        };

        Self { ring }
    }

    pub fn enqueue<T>(&self, obj: T) -> Result<(), Fail> {
        let obj_ptr: *mut c_void = Box::into_raw(Box::new(obj)) as *mut c_void;

        if unsafe { rte_ring_enqueue(self.ring, obj_ptr ) } == 0 {
            Ok(())
        } else {
            Err(Fail::new(libc::EINVAL, "ring is full"))
        }
    }

    pub fn dequeue<T>(&self) -> Option<T> {
        let mut obj: *mut T = null_mut();
        let obj_ptr: *mut *mut T = &mut obj;

        if unsafe { rte_ring_dequeue(self.ring, obj_ptr as *mut *mut c_void) } == 0 {
            let item = unsafe { Box::from_raw(*obj_ptr) };
            Some(*item)
        } else {
            None
        }
    }
}

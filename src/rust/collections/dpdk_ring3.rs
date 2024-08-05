//======================================================================================================================
// Imports
//======================================================================================================================

use crate::runtime::{
    fail::Fail,
    libdpdk::{
        rte_zmalloc,
        rte_ring,
        rte_spinlock_t,
        rte_spinlock_lock,
        rte_spinlock_unlock,
    },
};

use ::std::{
    ptr::null_mut,
    os::raw::c_void,
};
use crate::pal::arch;

//======================================================================================================================
// Structures
//======================================================================================================================
#[repr(C)]
pub struct DPDKRing3 {
    array: *mut *mut crate::inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock<crate::catnip::runtime::SharedDPDKRuntime>,
    len: usize,
    size: usize,
    lock: *mut rte_spinlock_t,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================
impl DPDKRing3 {
    pub fn new(name: String, size: usize) -> Self {
        let array = unsafe {
            let ptr = rte_zmalloc(name.as_bytes().as_ptr() as *const i8, size as usize * std::mem::size_of::<*mut crate::inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock<crate::catnip::runtime::SharedDPDKRuntime>>(), arch::CPU_DATA_CACHE_LINE_SIZE.try_into().unwrap()) as *mut *mut crate::inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock<crate::catnip::runtime::SharedDPDKRuntime>;

            if ptr.is_null() {
                panic!("Could not allocate the ring");
            }

            ptr
        };

        let lock = unsafe {
            let ptr = rte_zmalloc(std::ptr::null(), std::mem::size_of::<rte_spinlock_t>(), arch::CPU_DATA_CACHE_LINE_SIZE.try_into().unwrap()) as *mut rte_spinlock_t;

            if ptr.is_null() {
                panic!("Could not allocate the lock");
            }

            ptr
        };

        Self { 
            array,
            len: 0,
            size,
            lock,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn enqueue(&mut self, obj: *mut crate::inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock<crate::catnip::runtime::SharedDPDKRuntime>) -> Result<(), Fail> {
        unsafe {
            if self.array.is_null() {
                return Err(Fail::new(libc::EINVAL, "array is null"));
            }

            if self.len < self.size {
                rte_spinlock_lock(self.lock);
                let result = (|| {
                    *self.array.offset(self.len as isize) = obj;
                    self.len += 1;
                    Ok(())
                })();

                rte_spinlock_unlock(self.lock);

                result
            } else {
                Err(Fail::new(libc::EINVAL, "ring3 is full"))
            }
        }
    }

    pub fn peek(&self, counter: usize) -> Option<*mut crate::inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock<crate::catnip::runtime::SharedDPDKRuntime>> {
        unsafe {
            if self.array.is_null() || self.len == 0 {
                return None
            }
    
            let i: isize = (counter % self.len) as isize;
            Some(*self.array.offset(i))
        }
    }
}

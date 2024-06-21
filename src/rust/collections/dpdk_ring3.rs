//======================================================================================================================
// Imports
//======================================================================================================================

use crate::runtime::{
    fail::Fail,
    libdpdk::{
        rte_socket_id,
        rte_zmalloc,
        rte_ring,
        rte_spinlock_t,
        rte_ring_create,
        rte_ring_enqueue,
        rte_ring_dequeue,
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
            // Verifique se o array não é nulo antes de acessar
            if self.array.is_null() {
                return Err(Fail::new(libc::EINVAL, "array is null"));
            }
        
            if self.len < self.size {
                // Garantir que o lock seja sempre liberado
                rte_spinlock_lock(self.lock);
                let result = (|| {
                    // // Cria um Box e obtém um ponteiro cru para ele
                    // let obj_ptr: *mut crate::inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock<crate::catnip::runtime::SharedDPDKRuntime> = Box::into_raw(Box::new(obj)) as *mut crate::inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock<crate::catnip::runtime::SharedDPDKRuntime>;
        
                    // Insere o ponteiro no array na posição correta
                    *self.array.offset(self.len as isize) = obj;
        
                    // Incrementa o comprimento do array
                    self.len += 1;
        
                    // Retorna Ok se tudo deu certo
                    Ok(())
                })();
        
                // Libera o lock
                rte_spinlock_unlock(self.lock);
        
                // Retorna o resultado da operação
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

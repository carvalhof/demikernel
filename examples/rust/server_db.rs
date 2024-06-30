// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

#![feature(test)]
extern crate test;

use ::anyhow::Result;
use ::demikernel::{
    QDesc,
    QToken,
    LibOS,
    LibOSName,
    demi_sgarray_t,
    collections::{
        dpdk_ring2::DPDKRing2,
        dpdk_spinlock::DPDKSpinLock,
    },
    runtime::libdpdk::{
        rte_lcore_count,
        rte_eal_mp_wait_lcore,
        rte_eal_remote_launch,
        rte_tcp_hdr,
        rte_flow_attr,
        rte_flow_error,
        rte_flow_item,
        rte_flow_action,
        rte_flow_validate,
        rte_flow_create,
        rte_flow_action_queue,
        rte_flow_item_type_RTE_FLOW_ITEM_TYPE_ETH,
        rte_flow_item_type_RTE_FLOW_ITEM_TYPE_IPV4,
        rte_flow_item_type_RTE_FLOW_ITEM_TYPE_TCP,
        rte_flow_item_type_RTE_FLOW_ITEM_TYPE_END,
        rte_flow_action_type_RTE_FLOW_ACTION_TYPE_END,
        rte_flow_action_type_RTE_FLOW_ACTION_TYPE_QUEUE,
    },
    catnip::runtime::memory::MemoryManager,
};
use ::std::{
    env,
    sync::Arc,
    str::FromStr,
    net::SocketAddr,
};
use rocksdb::{DB, Options};

#[cfg(target_os = "windows")]
pub const AF_INET: i32 = windows::Win32::Networking::WinSock::AF_INET.0 as i32;

#[cfg(target_os = "windows")]
pub const SOCK_STREAM: i32 = windows::Win32::Networking::WinSock::SOCK_STREAM as i32;

#[cfg(target_os = "linux")]
pub const AF_INET: i32 = libc::AF_INET;

#[cfg(target_os = "linux")]
pub const SOCK_STREAM: i32 = libc::SOCK_STREAM;

#[cfg(feature = "profiler")]
use ::demikernel::perftools::profiler;

//==============================================================================
// Constants
//==============================================================================

const REQUEST_SIZE: usize = 64;
const RING_SIZE: u32 = 128*1024;

//======================================================================================================================
// Flow affinity (DPDK)
//======================================================================================================================

fn flow_affinity(nr_queues: usize) {
    use ::std::{
        mem::zeroed,
        os::raw::c_void,
    };

    unsafe {
        let n: u16 = 128;
        for i in 0..n {
            let mut err: rte_flow_error = zeroed();

            let mut attr: rte_flow_attr = zeroed();
            attr.set_egress(0);
            attr.set_ingress(1);

            let mut pattern: Vec<rte_flow_item> = vec![zeroed(); 4];
            pattern[0].type_ = rte_flow_item_type_RTE_FLOW_ITEM_TYPE_ETH;
            pattern[1].type_ = rte_flow_item_type_RTE_FLOW_ITEM_TYPE_IPV4;
            pattern[2].type_ = rte_flow_item_type_RTE_FLOW_ITEM_TYPE_TCP;
            let mut flow_tcp: rte_tcp_hdr = zeroed();
            let mut flow_tcp_mask: rte_tcp_hdr = zeroed();
            flow_tcp.dst_port = u16::to_be(12345 + i);
            flow_tcp_mask.dst_port = u16::MAX;
            pattern[2].spec = &mut flow_tcp as *mut _ as *mut c_void;
            pattern[2].mask = &mut flow_tcp_mask as *mut _ as *mut c_void;
            pattern[3].type_ = rte_flow_item_type_RTE_FLOW_ITEM_TYPE_END;

            let mut action: Vec<rte_flow_action> = vec![zeroed(); 2];
            action[0].type_ = rte_flow_action_type_RTE_FLOW_ACTION_TYPE_QUEUE;
            let mut queue_action: rte_flow_action_queue = zeroed();
            queue_action.index = i % (nr_queues as u16);
            action[0].conf = &mut queue_action as *mut _ as *mut c_void;
            action[1].type_ = rte_flow_action_type_RTE_FLOW_ACTION_TYPE_END;

            rte_flow_validate(0, &attr, pattern.as_ptr(), action.as_ptr(), &mut err);

            rte_flow_create(0, &attr, pattern.as_ptr(), action.as_ptr(), &mut err);
        }
    }
}

//======================================================================================================================
// Structures
//======================================================================================================================

struct WorkerArg {
    worker_id: usize,
    dispatcher_id: usize,
    db: Arc<DB>,
    mm: Arc<MemoryManager>,
    spinlock: *mut DPDKSpinLock,
    from_worker: *mut DPDKRing2,
    to_worker: *mut DPDKRing2,
}

struct DispatcherArg {
    addr: SocketAddr,
    dispatcher_id: usize,
    spinlock: *mut DPDKSpinLock,
    from_workers: *mut DPDKRing2,
    to_workers: *mut DPDKRing2,
}

//======================================================================================================================
// Worker
//======================================================================================================================

extern "C" fn worker_wrapper(data: *mut std::os::raw::c_void) -> i32 {
    let args: &mut WorkerArg = unsafe { &mut *(data as *mut WorkerArg) };

    worker_fn(args);

    #[allow(unreachable_code)]
    0
}

fn worker_fn(args: &mut WorkerArg) -> ! {
    let _dispatcher_id: usize = args.dispatcher_id;
    let _worker_id: usize = args.worker_id;
    let to_worker: *mut DPDKRing2 = args.to_worker;
    let from_worker: *mut DPDKRing2 = args.from_worker;
    let mm = &args.mm;

    // Get the Dabatase.
    let db = &args.db;

    // Releasing the lock.
    unsafe { (*args.spinlock).unlock() };

    loop {
        if let Some((qd, sga)) = unsafe { (*to_worker).dequeue::<(QDesc, demi_sgarray_t)>() } {
            // Process the request.

            let ptr: *mut u8 = sga.sga_segs[0].sgaseg_buf as *mut u8;
            let bytes_read = sga.sga_segs[0].sgaseg_len as usize;
            let buffer: &[u8] = unsafe { std::slice::from_raw_parts(ptr, bytes_read) };
            let command = String::from_utf8_lossy(&buffer[16..bytes_read]);
            let mut parts = command.split_whitespace();
            if let Some(operation) = parts.next() {
                let response: Vec<u8> = match operation {
                    "FLUSH" => {
                        let mut keys_to_delete: Vec<Box<[u8]>> = vec![];
                        for item in db.iterator(rocksdb::IteratorMode::Start) {
                            match item {
                                Ok((key, _)) => keys_to_delete.push(key),
                                Err(_) => break,
                            }
                        }

                        for key in keys_to_delete {
                            db.delete(key).unwrap();
                        }

                        b"OK\n".to_vec()
                    }
                    "SET" => {
                        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                            match db.put_opt(key.as_bytes(), value.as_bytes(), &Default::default()) {
                                Ok(_) => b"OK\n".to_vec(),
                                Err(_) => b"NOT OK\n".to_vec()
                            }
                        } else {
                            b"ERROR\n".to_vec()
                        }
                    }
                    "GET" => {
                        if let Some(key) = parts.next() {
                            match db.get(key.as_bytes()) {
                                Ok(value) => {
                                    match value {
                                        Some(mut reply) => {
                                            reply.push(b'\n');
                                            reply.clone()
                                        },
                                        None => b"NOT_FOUND\n".to_vec(),
                                    }
                                }
                                Err(_) => b"NOT_FOUND\n".to_vec(),
                            }
                        } else{
                            b"ERROR\n".to_vec()
                        }
                    }
                    "SCAN" => {
                        let mut response: Vec<u8> = match (parts.next(), parts.next()) {
                            // (Some(start_key), Some(end_key)) => {
                            //     let mut response: Vec<u8> = vec![];
                            //     let mut iter = db.iterator(IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward));
                            //     while let Some(Ok((key, value))) = iter.next() {
                            //         if key > end_key.as_bytes().into() {
                            //             break;
                            //         }
                            //         response.extend_from_slice(&key);
                            //         response.extend_from_slice(b" : ");
                            //         response.extend_from_slice(&value);
                            //         response.extend_from_slice(b"\n");
                            //     }

                            //     response
                            // }
                            _ => {
                                let mut response: Vec<u8> = vec![];
                                let mut iter = db.iterator(rocksdb::IteratorMode::Start);
                                while let Some(Ok((key, value))) = iter.next() {
                                    response.extend_from_slice(&key);
                                    response.extend_from_slice(b" : ");
                                    response.extend_from_slice(&value);
                                    response.extend_from_slice(b"\n");
                                }

                                response
                            }
                        };

                        if response.is_empty() {
                            response.append(&mut b"EMPTY\n".to_vec());
                        }

                        // let size_in_bytes: [u8; 8] = (response.len() as u64).to_le_bytes();
                        // let sga2: demi_sgarray_t = mm.alloc_sgarray(8).unwrap();

                        // // Fill in scatter-gather array.
                        // let ptr2: *mut u8 = sga2.sga_segs[0].sgaseg_buf as *mut u8;
                        // let len2: usize = sga2.sga_segs[0].sgaseg_len as usize;
                        // let slice2: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(ptr2, len2) };

                        // // Copy the size.
                        // slice2.copy_from_slice(&size_in_bytes);

                        // // Send the size.
                        // if let Err(e) = unsafe { (*from_worker).enqueue::<(QDesc, demi_sgarray_t)>((qd, sga2)) } {
                        //     panic!("Error: {:}", e);
                        // }

                        response
                    }
                    _ => {
                        b"UNKNOWN_COMMAND\n".to_vec()
                    }
                };

                let chunk_size = 1400;
                for chunk in response.chunks(chunk_size) {
                    let sga2: demi_sgarray_t = mm.alloc_sgarray(16+chunk.len()).unwrap();
                    
                    // Fill in scatter-gather array.
                    let ptr2: *mut u8 = sga2.sga_segs[0].sgaseg_buf as *mut u8;
                    let len2: usize = sga2.sga_segs[0].sgaseg_len as usize;
                    let slice2: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(ptr2, len2) };

                    // Copy the Timestamp.
                    slice2[0..16].copy_from_slice(&buffer[0..16]);

                    // Copy the reply.
                    slice2[16..].copy_from_slice(chunk);

                    // Send the reply.
                    if let Err(e) = unsafe { (*from_worker).enqueue::<(QDesc, demi_sgarray_t)>((qd, sga2)) } {
                        panic!("Error: {:}", e);
                    }
                    break;
                }
            }
        }
    }
}

//======================================================================================================================
// Dispatcher
//======================================================================================================================

extern "C" fn dispatcher_wrapper(data: *mut std::os::raw::c_void) -> i32 {
    let args: &mut DispatcherArg = unsafe { &mut *(data as *mut DispatcherArg) };

    dispatcher_fn(args);

    #[allow(unreachable_code)]
    0
}

fn dispatcher_fn(args: &mut DispatcherArg) -> ! {
    let mut addr: SocketAddr = args.addr;
    let dispatcher_id: usize = args.dispatcher_id;
    let to_workers: *mut DPDKRing2 = args.to_workers;
    let from_workers: *mut DPDKRing2 = args.from_workers;

    // Create the LibOS
    let mut libos: LibOS = match LibOS::new(LibOSName::Catnip, None) {
        Ok(libos) => libos,
        Err(e) => panic!("failed to initialize libos: {:?}", e),
    };

    // Setup peer.
    let sockqd: QDesc = match libos.socket(AF_INET, SOCK_STREAM, 0) {
        Ok(qd) => qd,
        Err(e) => panic!("failed to create socket: {:?}", e.cause),
    };

    // Bind the socket
    addr.set_port(addr.port() + (dispatcher_id as u16));
    match libos.bind(sockqd, addr) {
        Ok(()) => (),
        Err(e) => panic!("bind failed: {:?}", e.cause),
    };

    // Mark the socket as a passive one.
    match libos.listen(sockqd, 256) {
        Ok(()) => (),
        Err(e) => panic!("listen failed: {:?}", e.cause),
    };

    // Releasing the lock.
    unsafe { (*args.spinlock).unlock(); }

    let mut qts: Vec<QToken> = Vec::with_capacity(1024);

    // Accept incoming connection.
    if let Ok(qt) = libos.accept(sockqd) {
        qts.push(qt);
    }

    loop {
        // Try to get an application reply
        while let Some((qd, sga)) = unsafe { (*from_workers).dequeue::<(QDesc, demi_sgarray_t)>() } {
            // Push the reply.
            libos.push(qd, &sga).unwrap();
        }

        // Wait for some event.
        if let Some((idx, qr)) = libos.try_wait_any(&qts) {
            // Remove the qtoken.
            qts.remove(idx);

            // Parse the result.
            match qr.qr_opcode {
                demikernel::runtime::types::demi_opcode_t::DEMI_OPC_ACCEPT => {
                    // Pop the first request.   
                    let qd: QDesc = unsafe { qr.qr_value.ares.qd.into() };
                    if let Ok(qt) = libos.pop(qd, Some(REQUEST_SIZE)) {
                        qts.push(qt);
                    }

                    // Accept a new incoming connection.
                    if let Ok(qt) = libos.accept(sockqd) {
                        qts.push(qt);
                    }
                }
                demikernel::runtime::types::demi_opcode_t::DEMI_OPC_POP => {
                    // Process the request.
                    let qd: QDesc = qr.qr_qd.into();
                    let sga: demi_sgarray_t = unsafe { qr.qr_value.sga };

                    if let Err(e) = unsafe { (*to_workers).enqueue::<(QDesc, demi_sgarray_t)>((qd, sga)) } {
                        panic!("Error: {:?}", e);
                    }

                    // Pop the next request.
                    if let Ok(qt) = libos.pop(qd, Some(REQUEST_SIZE)) {
                        qts.push(qt);
                    }
                }
                _ => panic!("Not should be here"),
            }
        }
    }
}

//======================================================================================================================
// usage()
//======================================================================================================================

/// Prints program usage and exits.
fn usage(program_name: &String) {
    println!("Usage:");
    println!("{} MODE address CORES nr_dispatcher nr_workers FAKEWORK\n", program_name);
    println!("Modes:");
    println!("  --client    Run program in client mode.");
    println!("  --server    Run program in server mode.\n");
    println!("Fakework:\n");
    println!("  null");
    println!("  sqrt");
    println!("  randmem:1024");
    println!("  stridedmem:1024:7");
    println!("  streamingmem:1024");
    println!("  pointerchase:1024:7\n");
}

//======================================================================================================================
// main()
//======================================================================================================================

pub fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() >= 5 {
        if args[1] == "--server" {
            let addr: SocketAddr = SocketAddr::from_str(&args[2])?;
            let list_of_cores: Vec<&str> = args[3].split(":").collect();
            let nr_dispatchers: usize = usize::from_str(&args[4])?;
            let nr_workers: usize = usize::from_str(&args[5])?;

            // Checking for Catnip LibOS.
            match LibOSName::from_env() {
                Ok(libos_name) => {
                    match libos_name {
                        LibOSName::Catnip => (),
                        _ => panic!("LibOS should be Catnip")
                    }
                }
                Err(e) => anyhow::bail!("{:?}", e),
            }

            // Initialize DPDK EAL.
            let mm = {
                let rx_queues: u16 = nr_dispatchers as u16;
                let tx_queues: u16 = nr_dispatchers as u16;
                match LibOS::init(rx_queues, tx_queues) {
                    Ok(mm) => mm,
                    Err(e) => anyhow::bail!("{:?}", e),
                }
            };
            
            // Ensure the number of lcores.
            unsafe {
                if rte_lcore_count() < ((nr_workers + nr_dispatchers + 1) as u32) || list_of_cores.len() < (nr_workers + nr_dispatchers + 1) as usize {
                    panic!("The number of DPDK lcores should be at least {:?}", nr_workers + nr_dispatchers + 1);
                }
            }

            // Install flow rules to steer the incoming packets.
            flow_affinity(nr_dispatchers);

            // Create the Database
            let db_path: String = format!("database.db");
            let mut options = Options::default();
            options.create_if_missing(true);
            let db = Arc::new(DB::open(&options, db_path)?);

            {
                let mut keys_to_delete = vec![];
                for item in db.iterator(rocksdb::IteratorMode::Start) {
                    match item {
                        Ok((key, _)) => keys_to_delete.push(key),
                        Err(_) => break,
                    }
                }

                for key in keys_to_delete {
                    db.delete(key).unwrap();
                }
            }

            let mut lcore_idx: usize = 1;
            let spinlock: *mut DPDKSpinLock = Box::into_raw(Box::new(DPDKSpinLock::new()));

            for dispatcher_id in 0..nr_dispatchers {
                let name_to_workers: String = format!("to_workers_d{:?}", dispatcher_id);
                let name_from_workers: String = format!("from_workers_d{:?}", dispatcher_id);

                let to_workers: *mut DPDKRing2 = Box::into_raw(Box::new(DPDKRing2::new(name_to_workers, RING_SIZE)));
                let from_workers: *mut DPDKRing2 = Box::into_raw(Box::new(DPDKRing2::new(name_from_workers, RING_SIZE)));

                // We ensure that we have integer division here
                for worker_id in 0..(nr_workers/nr_dispatchers) {
                    let mut arg: WorkerArg = WorkerArg {
                        worker_id,
                        dispatcher_id,                       
                        db: db.clone(),
                        mm: mm.clone(),
                        spinlock,
                        from_worker: from_workers,
                        to_worker: to_workers,
                    };

                    let lcore: u32 = u32::from_str(list_of_cores[lcore_idx])?;
                    lcore_idx += 1;
                    unsafe { (*spinlock).set() };
                    let arg_ptr: *mut std::os::raw::c_void = &mut arg as *mut _ as *mut std::os::raw::c_void;
                    unsafe { rte_eal_remote_launch(Some(worker_wrapper), arg_ptr, lcore) };

                    unsafe { (*spinlock).lock() };
                }

                let mut arg: DispatcherArg = DispatcherArg {
                    addr,
                    dispatcher_id,
                    spinlock,
                    from_workers,
                    to_workers,
                };
                
                let lcore: u32 = u32::from_str(list_of_cores[lcore_idx])?;
                lcore_idx += 1;
                unsafe { (*spinlock).set() };
                let arg_ptr: *mut std::os::raw::c_void = &mut arg as *mut _ as *mut std::os::raw::c_void;
                unsafe { rte_eal_remote_launch(Some(dispatcher_wrapper), arg_ptr, lcore) };

                unsafe { (*spinlock).lock() };
            }

            unsafe { rte_eal_mp_wait_lcore() };
        }
    }

    usage(&args[0]);

    Ok(())
}

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
    collections::dpdk_spinlock::DPDKSpinLock,
    runtime::libdpdk::{
        rte_lcore_count,
        rte_get_timer_hz,
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
};
use ::rand::{
    Rng,
    SeedableRng,
    rngs::{
        self, 
        StdRng,
    },
};
use ::std::{
    env,
    sync::Arc,
    str::FromStr,
    time::Duration,
    net::SocketAddr,
};

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
            flow_tcp.src_port = u16::to_be(i + 1);
            flow_tcp_mask.src_port = u16::MAX;
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

pub enum FakeWorker {
    Null,
    Sqrt,
    Multiplication,
    StridedMem(Vec<u8>, usize),
    PointerChase(Vec<usize>),
    RandomMem(Vec<u8>, Vec<usize>),
    StreamingMem(Vec<u8>),
}

/// Associate Functions for FakeWorker
impl FakeWorker {
    pub fn create(spec: &str) -> Result<Self, &str> {
        let mut rng: StdRng = rngs::StdRng::from_seed([0 as u8; 32]);

        let tokens: Vec<&str> = spec.split(":").collect();
        assert!(tokens.len() > 0);

        match tokens[0] {
            "null" => Ok(FakeWorker::Null),
            "sqrt" => Ok(FakeWorker::Sqrt),
            "multiplication" => Ok(FakeWorker::Multiplication),
            "stridedmem" | "randmem" | "memstream" | "pointerchase" => {
                assert!(tokens.len() > 1);
                let size: usize = tokens[1].parse().unwrap();
                let buf = (0..size).map(|_| rng.gen()).collect();
                match tokens[0] {
                    "stridedmem" => {
                        assert!(tokens.len() > 2);
                        let stride: usize = tokens[2].parse().unwrap();
                        Ok(FakeWorker::StridedMem(buf, stride))
                    }
                    "pointerchase" => {
                        assert!(tokens.len() > 2);
                        let seed: u64 = tokens[2].parse().unwrap();
                        let mut rng: StdRng = rngs::StdRng::from_seed([seed as u8; 32]);
                        let nwords = size / 8;
                        let buf: Vec<usize> = (0..nwords).map(|_| rng.gen::<usize>() % nwords).collect();
                        Ok(FakeWorker::PointerChase(buf))
                    }
                    "randmem" => {
                        let sched = (0..size).map(|_| rng.gen::<usize>() % size).collect();
                        Ok(FakeWorker::RandomMem(buf, sched))
                    }
                    "memstream" => Ok(FakeWorker::StreamingMem(buf)),
                    _ => unreachable!(),
                }
            }
            _ => Err("bad fakework spec"),
        }
    }

    fn warmup_cache(&self) {
        match *self {
            FakeWorker::RandomMem(ref buf, ref sched) => {
                for i in 0..sched.len() {
                    test::black_box::<u8>(buf[sched[i]]);
                }
            }
            FakeWorker::StridedMem(ref buf, _stride) => {
                for i in 0..buf.len() {
                    test::black_box::<u8>(buf[i]);
                }
            }
            FakeWorker::PointerChase(ref buf) => {
                for i in 0..buf.len() {
                    test::black_box::<usize>(buf[i]);
                }
            }
            FakeWorker::StreamingMem(ref buf) => {
                for i in 0..buf.len() {
                    test::black_box::<u8>(buf[i]);
                }
            }
            _ => (),
        }
    }

    pub fn time(&self, iterations: u64, ticks_per_ns: f64) -> u64 {
        let rounds: usize = 100;
        let mut sum: f64 = 0.0;

        for _ in 0..rounds {
            let seed: u64 = rand::thread_rng().gen::<u64>();
            self.warmup_cache();
            let t0: u64 = unsafe { x86::time::rdtsc() };
            self.work(iterations, seed);
            let t1: u64 = unsafe { x86::time::rdtsc() };

            sum += ((t1 - t0) as f64)/ticks_per_ns;
        }

        (sum/(rounds as f64)) as u64
    }

    pub fn calibrate(&self, target_ns: u64, ticks_per_ns: f64) -> u64 {
        match *self {
            _ => {
                let mut iterations: u64 = 1;

                while self.time(iterations, ticks_per_ns) < target_ns {
                    iterations *= 2;
                }
                while self.time(iterations, ticks_per_ns) > target_ns {
                    iterations -= 1;
                }

                println!("{} ns: {} iterations", target_ns, iterations);

                iterations
            }
        }
    }

    pub fn work(&self, iters: u64, randomness: u64) {
        match *self {
            FakeWorker::Null => { },
            FakeWorker::Sqrt => {
                let k = 2350845.545;
                for i in 0..iters {
                    test::black_box(f64::sqrt(k * i as f64));
                }
            }
            FakeWorker::Multiplication => {
                let k = randomness;
                for i in 0..iters {
                    test::black_box(k * i);
                }
            }
            FakeWorker::StridedMem(ref buf, stride) => {
                let mut idx = randomness as usize % buf.len();
                let blen = buf.len();
                for _i in 0..iters as usize {
                    test::black_box::<u8>(buf[idx]);
                    idx += stride;
                    if idx >= blen {
                        idx -= blen;
                    }
                }
            }
            FakeWorker::RandomMem(ref buf, ref sched) => {
                for i in 0..iters as usize {
                    test::black_box::<u8>(buf[sched[i % sched.len()]]);
                }
            }
            FakeWorker::PointerChase(ref buf) => {
                let mut idx = randomness as usize % buf.len();
                for _i in 0..iters {
                    idx = buf[idx];
                    test::black_box::<usize>(idx);
                }
            }
            FakeWorker::StreamingMem(ref buf) => {
                for _ in 0..iters {
                    for i in (0..buf.len()).step_by(64) {
                        test::black_box::<u8>(buf[i]);
                    }
                }
            }
        }
    }
}

struct WorkerArg {
    worker_id: u16,
    addr: SocketAddr,
    spec: Arc<String>,
    spinlock: *mut DPDKSpinLock,
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
    let addr: SocketAddr = args.addr;
    let worker_id: u16 = args.worker_id;

    // Create the LibOS
    let mut libos: LibOS = match LibOS::new(LibOSName::Catnip, None) {
        Ok(libos) => libos,
        Err(e) => panic!("failed to initialize libos: {:?}", e),
    };

    // Create the FakeWorker
    let fakework: FakeWorker = FakeWorker::create(args.spec.as_str()).unwrap();
    fakework.warmup_cache();

    // Setup peer.
    let sockqd: QDesc = match libos.socket(AF_INET, SOCK_STREAM, 0) {
        Ok(qd) => qd,
        Err(e) => panic!("failed to create socket: {:?}", e.cause),
    };

    // Bind the socket
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

    let timeout: Option<Duration> = None;
    let mut qts: Vec<QToken> = Vec::with_capacity(1024);

    // Accept incoming connection.
    if let Ok(qt) = libos.accept(sockqd) {
        qts.push(qt);
    }

    loop {
        // Wait for some event.
        if let Ok ((idx, qr)) = libos.wait_any(&qts, timeout) {
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
                demikernel::runtime::types::demi_opcode_t::DEMI_OPC_PUSH => {
                    // Pop the next request.
                    let qd: QDesc = qr.qr_qd.into();
                    if let Ok(qt) = libos.pop(qd, Some(REQUEST_SIZE)) {
                        qts.push(qt);
                    }
                }
                demikernel::runtime::types::demi_opcode_t::DEMI_OPC_POP => {
                    // Process the request.
                    let sga: demi_sgarray_t = unsafe { qr.qr_value.sga };

                    let ptr: *mut u8 = sga.sga_segs[0].sgaseg_buf as *mut u8;
                    unsafe {
                        let iterations: u64 = *((ptr.offset(32)) as *mut u64);
                        let randomness: u64 = *((ptr.offset(40)) as *mut u64);
                        *((ptr.offset(24)) as *mut u64) = worker_id as u64;
                        fakework.work(iterations, randomness);
                    }

                    // Push the reply.
                    let qd: QDesc = qr.qr_qd.into();
                    if let Ok(qt) = libos.push(qd, &sga) {
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
    println!("{} MODE address CORES nr_threads FAKEWORK\n", program_name);
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

    if args.len() >= 3 {
        if args[1] == "--calibrate" {
            let fakework: FakeWorker = FakeWorker::create(args[2].as_str()).unwrap();

            // Initialize DPDK EAL.
            {
                match LibOS::init(1, 1) {
                    Ok(()) => (),
                    Err(e) => anyhow::bail!("{:?}", e),
                }
            }

            let ticks_per_ns: f64 = unsafe { rte_get_timer_hz() as f64 / 1000000000.0 }; 

            let calibrated: f64 = if args.len() > 3 {
                let target_ns: u64 = u64::from_str(&args[3])?;

                fakework.calibrate(target_ns, ticks_per_ns) as f64
            } else {
                let target_ns: u64 = 1000;
                let instructions: u64 = fakework.calibrate(target_ns, ticks_per_ns);

                let calibrated: f64 = (target_ns as f64)/(instructions as f64);

                calibrated
            };

            println!("\nCALIBRATION:{:?}\n", calibrated);
        }
    }

    if args.len() >= 6 {
        if args[1] == "--server" {
            let addr: SocketAddr = SocketAddr::from_str(&args[2])?;
            let list_of_cores: Vec<&str> = args[3].split(":").collect();
            let nr_workers: usize = usize::from_str(&args[4])?;
            let spec: Arc<String> = Arc::new(args[5].clone());

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
            {
                let rx_queues: u16 = nr_workers as u16;
                let tx_queues: u16 = nr_workers as u16;
                match LibOS::init(rx_queues, tx_queues) {
                    Ok(()) => (),
                    Err(e) => anyhow::bail!("{:?}", e),
                }
            }
            
            // Ensure the number of lcores.
            unsafe {
                if rte_lcore_count() < ((nr_workers + 1) as u32) || list_of_cores.len() < (nr_workers + 1) as usize {
                    panic!("The number of DPDK lcores should be at least {:?}", nr_workers + 1);
                }
            }

            // Install flow rules to steer the incoming packets.
            flow_affinity(nr_workers);

            let mut lcore_idx: usize = 1;
            let spinlock: *mut DPDKSpinLock = Box::into_raw(Box::new(DPDKSpinLock::new()));

            for worker_id in 0..nr_workers {
                let mut arg: WorkerArg = WorkerArg {
                    addr,
                    worker_id: worker_id as u16,
                    spec: Arc::clone(&spec),
                    spinlock,
                };

                let lcore: u32 = u32::from_str(list_of_cores[lcore_idx])?;
                lcore_idx += 1;
                unsafe { (*spinlock).set() };
                let arg_ptr: *mut std::os::raw::c_void = &mut arg as *mut _ as *mut std::os::raw::c_void;
                unsafe { rte_eal_remote_launch(Some(worker_wrapper), arg_ptr, lcore) };

                unsafe { (*spinlock).lock() };
            }

            unsafe { rte_eal_mp_wait_lcore() };
        }
    }

    usage(&args[0]);

    Ok(())
}

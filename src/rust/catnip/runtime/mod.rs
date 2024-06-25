// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

pub mod memory;

//======================================================================================================================
// Imports
//======================================================================================================================

use self::memory::{
    consts::DEFAULT_MAX_BODY_SIZE,
    MemoryManager,
};
use crate::{
    demikernel::config::Config,
    expect_some,
    inetstack::protocols::ethernet2::MIN_PAYLOAD_SIZE,
    runtime::{
        fail::Fail,
        libdpdk::{
            rte_eal_init,
            rte_eth_conf,
            rte_socket_id,
            rte_eth_dev_configure,
            rte_eth_dev_count_avail,
            rte_eth_dev_get_mtu,
            rte_eth_dev_info_get,
            rte_eth_dev_set_mtu,
            rte_eth_dev_start,
            rte_eth_macaddr_get,
            rte_eth_find_next_owned_by,
            rte_eth_rx_mq_mode_RTE_ETH_MQ_RX_RSS as RTE_ETH_MQ_RX_RSS,
            rte_eth_rx_mq_mode_RTE_ETH_MQ_RX_NONE as RTE_ETH_MQ_RX_NONE,
            rte_eth_tx_mq_mode_RTE_ETH_MQ_TX_NONE as RTE_ETH_MQ_TX_NONE,
            rte_eth_rx_queue_setup,
            rte_eth_rxconf,
            rte_eth_txconf,
            rte_eth_tx_queue_setup,
            rte_ether_addr,
            rte_eth_rss_ip,
            rte_eth_rss_tcp,
            rte_eth_rss_udp,
            RTE_ETHER_MAX_LEN,
            RTE_ETH_DEV_NO_OWNER,
            RTE_PKTMBUF_HEADROOM,
            RTE_ETHER_MAX_JUMBO_FRAME_LEN,
            rte_eth_rx_offload_ip_cksum,
            rte_eth_tx_offload_ip_cksum,
            rte_eth_rx_offload_tcp_cksum,
            rte_eth_tx_offload_tcp_cksum,
            rte_eth_rx_offload_udp_cksum,
            rte_eth_tx_offload_udp_cksum,
            rte_eth_rx_burst,
            rte_eth_tx_burst,
            rte_mbuf,
            // rte_pktmbuf_chain,
        },
        memory::DemiBuffer,
        network::{
            consts::RECEIVE_BATCH_SIZE,
            types::MacAddress,
            NetworkRuntime,
            PacketBuf,
        },
        SharedObject,
    },
};
use ::arrayvec::ArrayVec;
use ::std::mem;

use ::std::{
    mem::MaybeUninit,
    net::Ipv4Addr,
    ops::{
        Deref,
        DerefMut,
    },
    sync::{
        Arc,
        Once, 
        Mutex,
    },
};

//======================================================================================================================
// Global Variables
//======================================================================================================================

static INIT: Once = Once::new();
static mut NEXT_QUEUE_ID: Option<Arc<Mutex<u16>>> = None;
static mut GLOBAL_PORT_ID: Option<Arc<Mutex<u16>>> = None;
static mut GLOBAL_LINK_ADDR: Option<Arc<Mutex<MacAddress>>> = None;
static mut GLOBAL_MEMORY_MANAGER: Option<Arc<Mutex<Arc<MemoryManager>>>> = None;

//======================================================================================================================
// Structures
//======================================================================================================================

/// DPDK Runtime
pub struct DPDKRuntime {
    mm: Arc<MemoryManager>,
    port_id: u16,
    queue_id: u16,
    link_addr: MacAddress,
    ipv4_addr: Ipv4Addr,
}

#[derive(Clone)]
pub struct SharedDPDKRuntime(SharedObject<DPDKRuntime>);

//======================================================================================================================
// Associate Functions
//======================================================================================================================

/// Associate Functions for DPDK Runtime
impl SharedDPDKRuntime {
    /// Initializes DPDK.
    pub fn initialize_dpdk(config: &Config) -> Result<(Arc<MemoryManager>, u16, MacAddress), Fail> {
        // std::env::set_var("MLX5_SHUT_UP_BF", "1");
        // std::env::set_var("MLX5_SINGLE_THREADED", "1");
        // std::env::set_var("MLX4_SINGLE_THREADED", "1");

        let eal_init_args = config.eal_init_args().unwrap();
        let eal_init_refs = eal_init_args.iter().map(|s| s.as_ptr() as *mut u8).collect::<Vec<_>>();
        let ret: libc::c_int = unsafe { rte_eal_init(eal_init_refs.len() as i32, eal_init_refs.as_ptr() as *mut _) };
        if ret < 0 {
            let rte_errno: libc::c_int = unsafe { dpdk_rs::rte_errno() };
            let cause: String = format!("EAL initialization failed (rte_errno={:?})", rte_errno);
            error!("initialize_dpdk(): {}", cause);
            return Err(Fail::new(libc::EIO, &cause));
        }
        let nb_ports: u16 = unsafe { rte_eth_dev_count_avail() };
        if nb_ports == 0 {
            return Err(Fail::new(libc::EIO, "No ethernet ports available"));
        }
        trace!("DPDK reports that {} ports (interfaces) are available.", nb_ports);

        let max_body_size: usize = if config.enable_jumbo_frames()? {
            (RTE_ETHER_MAX_JUMBO_FRAME_LEN + RTE_PKTMBUF_HEADROOM) as usize
        } else {
            DEFAULT_MAX_BODY_SIZE
        };

        let memory_manager = match MemoryManager::new(max_body_size) {
            Ok(manager) => manager,
            Err(e) => {
                let cause: String = format!("Failed to set up memory manager: {:?}", e);
                error!("initialize_dpdk(): {}", cause);
                return Err(Fail::new(libc::EIO, &cause));
            },
        };

        let owner: u64 = RTE_ETH_DEV_NO_OWNER as u64;
        let port_id: u16 = unsafe { rte_eth_find_next_owned_by(0, owner) as u16 };

        let local_link_addr: MacAddress = unsafe {
            let mut m: MaybeUninit<rte_ether_addr> = MaybeUninit::zeroed();
            // TODO: Why does bindgen say this function doesn't return an int?
            rte_eth_macaddr_get(port_id, m.as_mut_ptr());
            MacAddress::new(m.assume_init().addr_bytes)
        };
        if local_link_addr.is_nil() || !local_link_addr.is_unicast() {
            let cause: String = format!("Invalid mac address: {:?}", local_link_addr);
            error!("initialize_dpdk(): {}", cause);
            return Err(Fail::new(libc::EINVAL, &cause));
        }

        let memory_manager = Arc::new(memory_manager);

        unsafe {
            INIT.call_once(|| {
                NEXT_QUEUE_ID = Some(Arc::new(Mutex::new(0)));
                GLOBAL_PORT_ID = Some(Arc::new(Mutex::new(port_id)));
                GLOBAL_LINK_ADDR = Some(Arc::new(Mutex::new(local_link_addr)));
                GLOBAL_MEMORY_MANAGER = Some(Arc::new(Mutex::new(memory_manager.clone())));
            });
        }

        Ok((memory_manager, port_id, local_link_addr))
    }

    /// Initializes a DPDK port.
    pub fn initialize_dpdk_port(
        port_id: u16,
        memory_manager: Arc<MemoryManager>,
        rx_queues: u16,
        tx_queues: u16,
        config: &Config,
    ) -> Result<(), Fail> {
        let rx_ring_size: u16 = 2048;
        let tx_ring_size: u16 = 2048;

        let dev_info: dpdk_rs::rte_eth_dev_info = unsafe {
            let mut d: MaybeUninit<dpdk_rs::rte_eth_dev_info> = MaybeUninit::zeroed();
            rte_eth_dev_info_get(port_id, d.as_mut_ptr());
            d.assume_init()
        };

        println!("dev_info: {:?}", dev_info);
        let mut port_conf: rte_eth_conf = unsafe { MaybeUninit::zeroed().assume_init() };

        port_conf.rxmode.mq_mode = if rx_queues > 1 {
            RTE_ETH_MQ_RX_RSS
        } else {
            RTE_ETH_MQ_RX_NONE
        };
        port_conf.txmode.mq_mode = RTE_ETH_MQ_TX_NONE;

        port_conf.rxmode.max_lro_pkt_size = if config.enable_jumbo_frames().unwrap() {
            RTE_ETHER_MAX_JUMBO_FRAME_LEN
        } else {
            RTE_ETHER_MAX_LEN
        };
        port_conf.rx_adv_conf.rss_conf.rss_key = 0 as *mut _;
        port_conf.rx_adv_conf.rss_conf.rss_hf = unsafe { (rte_eth_rss_ip() | rte_eth_rss_udp() | rte_eth_rss_tcp()) as u64 } & dev_info.flow_type_rss_offloads;

        if config.tcp_checksum_offload().unwrap() {
            port_conf.rxmode.offloads |= unsafe { (rte_eth_rx_offload_ip_cksum() | rte_eth_rx_offload_tcp_cksum()) as u64 };
            port_conf.txmode.offloads |= unsafe { (rte_eth_tx_offload_ip_cksum() | rte_eth_tx_offload_tcp_cksum()) as u64 };
        }
        if config.udp_checksum_offload().unwrap() {
            port_conf.rxmode.offloads |= unsafe { (rte_eth_rx_offload_ip_cksum() | rte_eth_rx_offload_udp_cksum()) as u64 };
            port_conf.txmode.offloads |= unsafe { (rte_eth_tx_offload_ip_cksum() | rte_eth_tx_offload_udp_cksum()) as u64 };
        }

        unsafe {
            if rte_eth_dev_configure(
                port_id,
                rx_queues,
                tx_queues,
                &port_conf as *const _,
            ) != 0 {
                let cause: String = format!("Failed to configure the interface");
                error!("initialize_dpdk_port(): {}", cause);
                return Err(Fail::new(libc::EIO, &cause));
            }
        }

        unsafe {
            let mtu = config.mtu().unwrap();
            if rte_eth_dev_set_mtu(port_id, mtu) != 0 {
                let cause: String = format!("Failed to set mtu {:?}", mtu);
                error!("initialize_dpdk_port(): {}", cause);
                return Err(Fail::new(libc::EIO, &cause));
            }
            let mut dpdk_mtu: u16 = 0u16;
            if (rte_eth_dev_get_mtu(port_id, &mut dpdk_mtu as *mut _)) != 0 {
                let cause: String = format!("Failed to get mtu");
                error!("initialize_dpdk_port(): {}", cause);
                return Err(Fail::new(libc::EIO, &cause));
            }
            if dpdk_mtu != mtu {
                let cause: String = format!("Failed to set MTU to {}, got back {}", mtu, dpdk_mtu);
                error!("initialize_dpdk_port(): {}", cause);
                return Err(Fail::new(libc::EIO, &cause));
            }
        }

        let socket_id: u32 = unsafe { rte_socket_id() };

        let mut rx_conf: rte_eth_rxconf = dev_info.default_rxconf;
        rx_conf.offloads = port_conf.rxmode.offloads;
        rx_conf.rx_drop_en = 1;

        let mut tx_conf: rte_eth_txconf = dev_info.default_txconf;
        tx_conf.offloads = port_conf.txmode.offloads;

        unsafe {
            for i in 0..rx_queues {
                if rte_eth_rx_queue_setup(
                    port_id,
                    i,
                    rx_ring_size,
                    socket_id,
                    &rx_conf as *const _,
                    memory_manager.body_pool(),
                ) != 0 {
                    let cause: String = format!("Failed to set up rx queue");
                    error!("initialize_dpdk_port(): {}", cause);
                    return Err(Fail::new(libc::EIO, &cause));
                }
            }
            for i in 0..tx_queues {
                if rte_eth_tx_queue_setup(
                    port_id,
                    i,
                    tx_ring_size,
                    socket_id,
                    &tx_conf as *const _,
                ) != 0 {
                    let cause: String = format!("Failed to set up rx queue");
                    error!("initialize_dpdk_port(): {}", cause);
                    return Err(Fail::new(libc::EIO, &cause));
                }
            }
            if rte_eth_dev_start(port_id) != 0 {
                let cause: String = format!("Invalid port id");
                error!("initialize_dpdk_port(): {}", cause);
                return Err(Fail::new(libc::EIO, &cause));
            }
        }

        Ok(())
    }

    pub fn get_link_addr(&self) -> MacAddress {
        self.link_addr
    }

    pub fn get_ip_addr(&self) -> Ipv4Addr {
        self.ipv4_addr
    }
}

//======================================================================================================================
// Imports
//======================================================================================================================

impl Deref for SharedDPDKRuntime {
    type Target = DPDKRuntime;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for SharedDPDKRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

/// Network Runtime Trait Implementation for DPDK Runtime
impl NetworkRuntime for SharedDPDKRuntime {
    fn new(config: &Config) -> Result<Self, Fail> {
        let global_qid: Arc<Mutex<u16>> = unsafe { NEXT_QUEUE_ID.as_ref().unwrap().clone() };

        let queue_id: u16 = {
            let mut qid = global_qid.lock().unwrap();

            let queue_id: u16 = *qid;
            *qid += 1;
            drop(qid);

            queue_id
        };

        let (mm, port_id, link_addr): (Arc<MemoryManager>, u16, MacAddress) = unsafe {
            (
                GLOBAL_MEMORY_MANAGER.as_ref().unwrap().clone().lock().unwrap().clone(),
                *GLOBAL_PORT_ID.as_ref().unwrap().clone().lock().unwrap(),
                *GLOBAL_LINK_ADDR.as_ref().unwrap().clone().lock().unwrap(),
            )
        };

        Ok(Self(SharedObject::<DPDKRuntime>::new(DPDKRuntime {
            mm,
            port_id,
            queue_id,
            link_addr,
            ipv4_addr: config.local_ipv4_addr()?,
        })))
    }

    fn transmit(&mut self, mut buf: Box<dyn PacketBuf>) {
        let headers_size: usize = buf.header_size();
        debug_assert!(headers_size < u16::MAX as usize);

        let mut mbuf: DemiBuffer = match buf.take_body() {
            Some(mut body) => {
                debug_assert_eq!(body.is_dpdk_allocated(), true);
                match body.prepend(headers_size.try_into().unwrap()) {
                    Ok(()) => body,
                    Err(e) => panic!("failed to prepend mbuf: {:?}", e.cause),
                }
            }
            None => {
                let packet_size: usize = if headers_size < MIN_PAYLOAD_SIZE { MIN_PAYLOAD_SIZE } else { headers_size };
                let mut mbuf: DemiBuffer = match self.mm.alloc_header_mbuf(packet_size) {
                    Ok(mbuf) => mbuf,
                    Err(e) => panic!("failed to allocate mbuf: {:?}", e.cause),
                };

                if headers_size < MIN_PAYLOAD_SIZE {
                    let padding_bytes: usize = MIN_PAYLOAD_SIZE - headers_size;
                    let padding_buf: &mut [u8] = &mut mbuf[headers_size..][..padding_bytes];
                    for byte in padding_buf {
                        *byte = 0;
                    }
                }

                mbuf
            }
        };

        let headers_ptr: &mut [u8] = &mut mbuf;
        buf.write_header(headers_ptr);

        let mut mbuf_ptr: *mut rte_mbuf = expect_some!(mbuf.into_mbuf(), "mbuf cannot be empty");
        unsafe { (*mbuf_ptr).ol_flags |= 1 << 52 }; // RTE_MBUF_F_TX_TCP_CKSUM
        let num_sent: u16 = unsafe { rte_eth_tx_burst(self.port_id, self.queue_id, &mut mbuf_ptr, 1) };
        debug_assert_eq!(num_sent, 1);
    }

    fn receive(&mut self) -> ArrayVec<DemiBuffer, RECEIVE_BATCH_SIZE> {
        let mut out = ArrayVec::new();

        let mut packets: [*mut rte_mbuf; RECEIVE_BATCH_SIZE] = unsafe { mem::zeroed() };
        let nb_rx = unsafe { rte_eth_rx_burst(self.port_id, self.queue_id, packets.as_mut_ptr(), RECEIVE_BATCH_SIZE as u16) };
        assert!(nb_rx as usize <= RECEIVE_BATCH_SIZE);

        {
            for &packet in &packets[..nb_rx as usize] {
                // Safety: `packet` is a valid pointer to a properly initialized `rte_mbuf` struct.
                let buf: DemiBuffer = unsafe { DemiBuffer::from_mbuf(packet) };
                out.push(buf);
            }
        }

        out
    }
}

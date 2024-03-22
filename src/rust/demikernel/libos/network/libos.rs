// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//==============================================================================
// Imports
//==============================================================================

use crate::{
    collections::async_queue::SharedAsyncQueue, 
    demikernel::libos::{
        network::queue::SharedNetworkQueue, 
        SharedDPDKRuntime, 
        SharedInetStack
    }, 
    inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock,
    pal::constants::SOMAXCONN, runtime::{
        fail::Fail, limits, memory::{
            DemiBuffer,
            MemoryRuntime,
        }, network::{
            socket::SocketId,
            transport::NetworkTransport,
            unwrap_socketaddr,
        }, queue::{
            downcast_queue,
            IoQueue,
            Operation,
            OperationResult,
        }, scheduler::Yielder, types::{
            demi_qresult_t,
            demi_sgarray_t,
        }, QDesc, QToken, SharedBetweenCores, SharedDemiRuntime, SharedObject
    }, QType
};
use ::futures::FutureExt;
use ::socket2::{
    Domain,
    Protocol,
    Type,
};
use std::str::FromStr;
use ::std::{
    net::{
        Ipv4Addr,
        SocketAddr,
        SocketAddrV4,
    },
    ops::{
        Deref,
        DerefMut,
    },
    pin::Pin,
};

//======================================================================================================================
// Structures
//======================================================================================================================

/// [NetworkLibOS] represents a multi-queue Catnap library operating system that provides the Demikernel API on top of
/// the Linux/POSIX API. [NetworkLibOS] is stateless and purely contains multi-queue functionality necessary to run the
/// Catnap libOS. All state is kept in the [runtime] and [qtable].
/// TODO: Move [qtable] into [runtime] so all state is contained in the PosixRuntime.
pub struct NetworkLibOS<T: NetworkTransport> {
    id: usize,
    /// Underlying runtime.
    runtime: SharedDemiRuntime,
    /// Underlying network transport.
    transport: T,
    /// Shared structure between cores.
    shared_between_cores: *mut SharedBetweenCores,
    ///
    shared_queue: SharedAsyncQueue<QDesc>,
    ///
    ready_indices: Vec<usize>,
}

#[derive(Clone)]
pub struct SharedNetworkLibOS<T: NetworkTransport>(SharedObject<NetworkLibOS<T>>);

//======================================================================================================================
// Associate Functions
//======================================================================================================================

/// Associate Functions for Catnap LibOS
impl<T: NetworkTransport> SharedNetworkLibOS<T> {
    /// Instantiates a Catnap LibOS.
    pub fn new(id: usize, mut runtime: SharedDemiRuntime, transport: T, shared_between_cores: *mut SharedBetweenCores) -> Self {
        let yielder: Yielder = Yielder::new();
        let me: Self = Self(SharedObject::new(NetworkLibOS::<T> {
            id,
            runtime: runtime.clone(),
            transport,
            shared_between_cores, 
            shared_queue: SharedAsyncQueue::<QDesc>::default(),
            ready_indices: Vec::<usize>::with_capacity(1_000_000_000),
        }));
        let _ = runtime.insert_background_coroutine(
            "background for established connections",
            Box::pin(me.clone().poll_for_established(yielder).fuse()));

        me
    }

    /// Creates a socket. This function contains the libOS-level functionality needed to create a SharedNetworkQueue that
    /// wraps the underlying POSIX socket.
    pub fn socket(&mut self, domain: Domain, typ: Type, _protocol: Protocol) -> Result<QDesc, Fail> {
        trace!("socket() domain={:?}, type={:?}, protocol={:?}", domain, typ, _protocol);

        // Parse communication domain.
        if domain != Domain::IPV4 {
            return Err(Fail::new(libc::ENOTSUP, "communication domain not supported"));
        }

        // Parse socket type.
        if (typ != Type::STREAM) && (typ != Type::DGRAM) {
            let cause: String = format!("socket type not supported (type={:?})", typ);
            error!("socket(): {}", cause);
            return Err(Fail::new(libc::ENOTSUP, &cause));
        }

        // Create underlying queue.
        let queue: SharedNetworkQueue<T> = SharedNetworkQueue::new(domain, typ, &mut self.transport)?;
        let qd: QDesc = unsafe { (*self.shared_between_cores).alloc_queue(queue) };
        // let qd: QDesc = self.runtime.alloc_queue(queue);
        Ok(qd)
    }

    /// Binds a socket to a local endpoint. This function contains the libOS-level functionality needed to bind a
    /// SharedNetworkQueue to a local address.
    pub fn bind(&mut self, qd: QDesc, mut local: SocketAddr) -> Result<(), Fail> {
        trace!("bind() qd={:?}, local={:?}", qd, local);

        let localv4: SocketAddrV4 = unwrap_socketaddr(local)?;
        // Check if we are binding to the wildcard address. We only support this for UDP sockets right now.
        // FIXME: https://github.com/demikernel/demikernel/issues/189
        if localv4.ip() == &Ipv4Addr::UNSPECIFIED && self.get_shared_queue(&qd)?.get_qtype() != QType::UdpSocket {
            let cause: String = format!("cannot bind to wildcard address (qd={:?})", qd);
            error!("bind(): {}", cause);
            return Err(Fail::new(libc::ENOTSUP, &cause));
        }

        // Check if this is an ephemeral port.
        if SharedDemiRuntime::is_private_ephemeral_port(local.port()) {
            // Allocate ephemeral port from the pool.
            self.runtime.reserve_ephemeral_port(local.port())?
        }

        // Check if we are binding to the wildcard port. We only support this for UDP sockets right now.
        // FIXME: https://github.com/demikernel/demikernel/issues/582
        if local.port() == 0 {
            if self.get_shared_queue(&qd)?.get_qtype() != QType::UdpSocket {
                let cause: String = format!("cannot bind to port 0 (qd={:?})", qd);
                error!("bind(): {}", cause);
                return Err(Fail::new(libc::ENOTSUP, &cause));
            } else {
                // Allocate an ephemeral port.
                let new_port: u16 = self.runtime.alloc_ephemeral_port()?;
                local.set_port(new_port);
            }
        }

        // Check wether the address is in use.
        if self.runtime.addr_in_use(localv4) {
            let cause: String = format!("address is already bound to a socket (qd={:?}", qd);
            error!("bind(): {}", &cause);
            return Err(Fail::new(libc::EADDRINUSE, &cause));
        }

        // Issue bind operation.
        if let Err(e) = self.get_shared_queue(&qd)?.bind(local) {
            // Rollback ephemeral port allocation.
            if SharedDemiRuntime::is_private_ephemeral_port(local.port()) {
                if self.runtime.free_ephemeral_port(local.port()).is_err() {
                    warn!("bind(): leaking ephemeral port (port={})", local.port());
                }
            }
            Err(e)
        } else {
            // Insert into address to queue descriptor table.
            self.runtime
                .insert_socket_id_to_qd(SocketId::Passive(localv4.clone()), qd);
            Ok(())
        }
    }

    /// Sets a SharedNetworkQueue and its underlying socket as a passive one. This function contains the libOS-level
    /// functionality to move the SharedNetworkQueue and underlying socket into the listen state.
    pub fn listen(&mut self, qd: QDesc, backlog: usize) -> Result<(), Fail> {
        trace!("listen() qd={:?}, backlog={:?}", qd, backlog);

        // We use this API for testing, so we must check again.
        if !((backlog > 0) && (backlog <= SOMAXCONN as usize)) {
            let cause: String = format!("invalid backlog length: {:?}", backlog);
            warn!("{}", cause);
            return Err(Fail::new(libc::EINVAL, &cause));
        }

        // Issue listen operation.
        self.get_shared_queue(&qd)?.listen(backlog)
    }

    /// Synchronous cross-queue code to start accepting a connection. This function schedules the asynchronous
    /// coroutine and performs any necessary synchronous, multi-queue operations at the libOS-level before beginning
    /// the accept.
    pub fn accept(&mut self, qd: QDesc) -> Result<QToken, Fail> {
        trace!("accept(): qd={:?}", qd);

        if self.id != 0 {
            log::warn!("[w{:?}] This LibOS issues a fake accept coroutine", self.id);

            let coroutine_constructor = || -> Result<QToken, Fail> {
                let task_name: String = format!("[w{:?}] FakeAcceptCoroutine for qd={:?}", self.id, qd);
                let coroutine_factory =
                    |yielder| -> Pin<Box<Operation>> { Box::pin(self.clone().fake_accept_coroutine(self.shared_queue.clone(), yielder).fuse()) };

                self.runtime
                    .clone()
                    .insert_coroutine_with_tracking(&task_name, coroutine_factory, qd)
            };

            return coroutine_constructor();
        }

        let mut queue: SharedNetworkQueue<T> = self.get_shared_queue(&qd)?;
        let coroutine_constructor = || -> Result<QToken, Fail> {
            let task_name: String = format!("[w{:?}] NetworkLibOS::accept for qd={:?}", self.id, qd);
            let coroutine_factory =
                |yielder| -> Pin<Box<Operation>> { Box::pin(self.clone().accept_coroutine(qd, yielder).fuse()) };
            self.runtime
                .clone()
                .insert_coroutine_with_tracking(&task_name, coroutine_factory, qd)
        };

        queue.accept(coroutine_constructor)
    }

    async fn fake_accept_coroutine(self, mut shared_queue: SharedAsyncQueue<QDesc>, yielder: Yielder) -> (QDesc, OperationResult) {
        trace!("[w{:?}] fake_accept_coroutine()...", self.id);
        match shared_queue.pop(&yielder).await {
            Ok(qd) => {
                trace!("[w{:?}] fake_accept_coroutine() completed", self.id);
                (qd, OperationResult::Accept((qd, SocketAddrV4::from_str("1.1.1.1:1").unwrap())))
            }
            Err(e) => {
                (self.get_qd(), OperationResult::Failed(e))
            },
        }
    }

    /// Asynchronous cross-queue code for accepting a connection. This function returns a coroutine that runs
    /// asynchronously to accept a connection and performs any necessary multi-queue operations at the libOS-level after
    /// the accept succeeds or fails.
    async fn accept_coroutine(mut self, qd: QDesc, yielder: Yielder) -> (QDesc, OperationResult) {
        // Grab the queue, make sure it hasn't been closed in the meantime.
        // This will bump the Rc refcount so the coroutine can have it's own reference to the shared queue data
        // structure and the SharedNetworkQueue will not be freed until this coroutine finishes.
        let mut queue: SharedNetworkQueue<T> = match self.get_shared_queue(&qd) {
            Ok(queue) => queue.clone(),
            Err(e) => return (qd, OperationResult::Failed(e)),
        };
        // Wait for the accept operation to complete.
        match queue.accept_coroutine(yielder).await {
            Ok(new_queue) => {
                // TODO: Do we need to add this to the socket id to queue descriptor table?
                // It is safe to call except here because the new queue is connected and it should be connected to a
                // remote address.
                let addr: SocketAddr = new_queue
                    .remote()
                    .expect("An accepted socket must have a remote address");
                let local: SocketAddrV4 = unwrap_socketaddr(new_queue.local().unwrap()).expect("we only support IPv4");
                let remote: SocketAddrV4 = unwrap_socketaddr(new_queue.remote().unwrap()).expect("we only support IPv4");
                // let new_qd: QDesc = self.runtime.alloc_queue(new_queue);
                let new_qd: QDesc = unsafe { (*self.shared_between_cores).alloc_queue(new_queue) };
                // FIXME: add IPv6 support; https://github.com/microsoft/demikernel/issues/935
                unsafe {
                    if let Some(sock) = (*self.shared_between_cores).get_mut_on_addresses(SocketId::Active(local, remote)) {
                        let cb = (*sock).get_cb();
                        (*self.shared_between_cores).add_background(new_qd, cb);
                    }
                }
                (
                    qd,
                    OperationResult::Accept((new_qd, unwrap_socketaddr(addr).expect("we only support IPv4"))),
                )
            },
            Err(e) => {
                warn!("accept() listening_qd={:?}: {:?}", qd, &e);
                (qd, OperationResult::Failed(e))
            },
        }
    }

    /// Synchronous code to establish a connection to a remote endpoint. This function schedules the asynchronous
    /// coroutine and performs any necessary synchronous, multi-queue operations at the libOS-level before beginning
    /// the connect.
    pub fn connect(&mut self, qd: QDesc, remote: SocketAddr) -> Result<QToken, Fail> {
        trace!("connect() qd={:?}, remote={:?}", qd, remote);

        // FIXME: add IPv6 support; https://github.com/microsoft/demikernel/issues/935
        let mut queue: SharedNetworkQueue<T> = self.get_shared_queue(&qd)?;
        let coroutine_constructor = || -> Result<QToken, Fail> {
            let task_name: String = format!("NetworkLibOS::connect for qd={:?}", qd);
            let coroutine_factory = |yielder| -> Pin<Box<Operation>> {
                Box::pin(self.clone().connect_coroutine(qd, remote, yielder).fuse())
            };
            self.runtime
                .clone()
                .insert_coroutine_with_tracking(&task_name, coroutine_factory, qd)
        };

        queue.connect(coroutine_constructor)
    }

    /// Asynchronous code to establish a connection to a remote endpoint. This function returns a coroutine that runs
    /// asynchronously to connect a queue and performs any necessary multi-queue operations at the libOS-level after
    /// the connect succeeds or fails.
    async fn connect_coroutine(mut self, qd: QDesc, remote: SocketAddr, yielder: Yielder) -> (QDesc, OperationResult) {
        // Grab the queue, make sure it hasn't been closed in the meantime.
        // This will bump the Rc refcount so the coroutine can have it's own reference to the shared queue data
        // structure and the SharedNetworkQueue will not be freed until this coroutine finishes.
        let mut queue: SharedNetworkQueue<T> = match self.get_shared_queue(&qd) {
            Ok(queue) => queue.clone(),
            Err(e) => return (qd, OperationResult::Failed(e)),
        };
        // Wait for connect operation to complete.
        match queue.connect_coroutine(remote, yielder).await {
            Ok(()) => {
                // TODO: Do we need to add this to socket id to queue descriptor table?
                (qd, OperationResult::Connect)
            },
            Err(e) => {
                warn!("connect() failed (qd={:?}, error={:?})", qd, e.cause);
                (qd, OperationResult::Failed(e))
            },
        }
    }

    /// Synchronous code to asynchronously close a queue. This function schedules the coroutine that asynchronously
    /// runs the close and any synchronous multi-queue functionality before the close begins.
    pub fn async_close(&mut self, qd: QDesc) -> Result<QToken, Fail> {
        trace!("async_close() qd={:?}", qd);

        let mut queue: SharedNetworkQueue<T> = self.get_shared_queue(&qd)?;
        let coroutine_constructor = || -> Result<QToken, Fail> {
            let task_name: String = format!("NetworkLibOS::close for qd={:?}", qd);
            let coroutine_factory =
                |yielder| -> Pin<Box<Operation>> { Box::pin(self.clone().close_coroutine(qd, yielder).fuse()) };
            self.runtime
                .clone()
                .insert_coroutine_with_tracking(&task_name, coroutine_factory, qd)
        };

        queue.close(coroutine_constructor)
    }

    /// Asynchronous code to close a queue. This function returns a coroutine that runs asynchronously to close a queue
    /// and the underlying POSIX socket and performs any necessary multi-queue operations at the libOS-level after
    /// the close succeeds or fails.
    async fn close_coroutine(mut self, qd: QDesc, yielder: Yielder) -> (QDesc, OperationResult) {
        // Grab the queue, make sure it hasn't been closed in the meantime.
        // This will bump the Rc refcount so the coroutine can have it's own reference to the shared queue data
        // structure and the SharedNetworkQueue will not be freed until this coroutine finishes.
        let mut queue: SharedNetworkQueue<T> = match self.runtime.get_shared_queue(&qd) {
            Ok(queue) => queue,
            Err(e) => return (qd, OperationResult::Failed(e)),
        };
        // Wait for close operation to complete.
        match queue.close_coroutine(yielder).await {
            Ok(()) => {
                // If the queue was bound, remove from the socket id to queue descriptor table.
                if let Some(local) = queue.local() {
                    // FIXME: add IPv6 support; https://github.com/microsoft/demikernel/issues/935
                    self.runtime.remove_socket_id_to_qd(&SocketId::Passive(
                        unwrap_socketaddr(local).expect("we only support IPv4"),
                    ));

                    // Check if this is an ephemeral port.
                    if SharedDemiRuntime::is_private_ephemeral_port(local.port())
                        && queue.get_qtype() == QType::UdpSocket
                    {
                        // Allocate ephemeral port from the pool, to leave  ephemeral port allocator in a consistent state.
                        if let Err(e) = self.runtime.free_ephemeral_port(local.port()) {
                            let cause: String = format!("close(): Could not free ephemeral port");
                            warn!("{}: {:?}", cause, e);
                        }
                    }
                }
                // Remove the queue from the queue table. Expect is safe here because we looked up the queue to
                // schedule this coroutine and no other close coroutine should be able to run due to state machine
                // checks.
                self.runtime
                    .free_queue::<SharedNetworkQueue<T>>(&qd)
                    .expect("queue should exist");
                (qd, OperationResult::Close)
            },
            Err(e) => {
                warn!("async_close() qd={:?}: {:?}", qd, &e);
                (qd, OperationResult::Failed(e))
            },
        }
    }

    /// Synchronous code to push [buf] to a SharedNetworkQueue and its underlying POSIX socket. This function schedules the
    /// coroutine that asynchronously runs the push and any synchronous multi-queue functionality before the push
    /// begins.
    pub fn push(&mut self, qd: QDesc, sga: &demi_sgarray_t) -> Result<QToken, Fail> {
        // let buf: DemiBuffer = self.runtime.clone_sgarray(sga)?;
        let token: std::ptr::NonNull<u8> = unsafe { std::ptr::NonNull::new_unchecked(sga.sga_buf as *mut u8) };
        let buf: DemiBuffer = unsafe { DemiBuffer::from_raw(token) };

        if buf.len() == 0 {
            let cause: String = format!("zero-length buffer");
            warn!("push(): {}", cause);
            return Err(Fail::new(libc::EINVAL, &cause));
        };

        // let cb: *mut SharedControlBlock<SharedDPDKRuntime> = unsafe { *(*(*self.shared_between_cores).qd_to_cb).get_mut(&qd).unwrap() };
        let cb: *mut SharedControlBlock<SharedDPDKRuntime> = unsafe { (*(*self.shared_between_cores).qd_to_cb).get_mut(qd).unwrap() };

        let coroutine_constructor = || -> Result<QToken, Fail> {
            // let task_name: String = format!("NetworkLibOS::push for qd={:?}", qd);
            let task_name = "NetworkLibOS::push for qd=";
            let coroutine_factory =
                |yielder| -> Pin<Box<Operation>> { Box::pin(self.clone().push_coroutine(qd, cb, buf, yielder).fuse()) };
            self.runtime
                .clone()
                .insert_coroutine_with_tracking(task_name, coroutine_factory, qd)
        };
        // queue.push(coroutine_constructor)

        coroutine_constructor()
    }

    /// Asynchronous code to push [buf] to a SharedNetworkQueue and its underlying POSIX socket. This function returns a
    /// coroutine that runs asynchronously to push a queue and its underlying POSIX socket and performs any necessary
    /// multi-queue operations at the libOS-level after the push succeeds or fails.
    async fn push_coroutine(self, qd: QDesc, cb: *mut SharedControlBlock<SharedDPDKRuntime>, buf: DemiBuffer, _yielder: Yielder) -> (QDesc, OperationResult) {
        let buf_ptr = buf.into_mbuf().unwrap();
        unsafe { (*(*cb).aux_push_queue).enqueue(buf_ptr).unwrap() };
        (qd, OperationResult::Push)
    }

    /// Synchronous code to pushto [buf] to [remote] on a SharedNetworkQueue and its underlying POSIX socket. This
    /// function schedules the coroutine that asynchronously runs the pushto and any synchronous multi-queue
    /// functionality after pushto begins.
    pub fn pushto(&mut self, qd: QDesc, sga: &demi_sgarray_t, remote: SocketAddr) -> Result<QToken, Fail> {
        trace!("pushto() qd={:?}", qd);

        let buf: DemiBuffer = self.runtime.clone_sgarray(sga)?;
        if buf.len() == 0 {
            return Err(Fail::new(libc::EINVAL, "zero-length buffer"));
        }

        let mut queue: SharedNetworkQueue<T> = self.get_shared_queue(&qd)?;
        let coroutine_constructor = || -> Result<QToken, Fail> {
            // let task_name: String = format!("NetworkLibOS::pushto for qd={:?}", qd);
            let task_name = "NetworkLibOS::pushto for qd=";
            let coroutine_factory = |yielder| -> Pin<Box<Operation>> {
                Box::pin(self.clone().pushto_coroutine(qd, buf, remote, yielder).fuse())
            };
            self.runtime
                .clone()
                .insert_coroutine_with_tracking(task_name, coroutine_factory, qd)
        };

        queue.push(coroutine_constructor)
    }

    /// Asynchronous code to pushto [buf] to [remote] on a SharedNetworkQueue and its underlying POSIX socket. This function
    /// returns a coroutine that runs asynchronously to pushto a queue and its underlying POSIX socket and performs any
    /// necessary multi-queue operations at the libOS-level after the pushto succeeds or fails.
    async fn pushto_coroutine(
        mut self,
        qd: QDesc,
        buf: DemiBuffer,
        remote: SocketAddr,
        yielder: Yielder,
    ) -> (QDesc, OperationResult) {
        // Grab the queue, make sure it hasn't been closed in the meantime.
        // This will bump the Rc refcount so the coroutine can have it's own reference to the shared queue data
        // structure and the SharedNetworkQueue will not be freed until this coroutine finishes.
        let mut queue: SharedNetworkQueue<T> = match self.get_shared_queue(&qd) {
            Ok(queue) => queue,
            Err(e) => return (qd, OperationResult::Failed(e)),
        };
        // Wait for push to complete.
        match queue.push_coroutine(buf, Some(remote), yielder).await {
            Ok(()) => (qd, OperationResult::Push),
            Err(e) => {
                warn!("pushto() qd={:?}: {:?}", qd, &e);
                (qd, OperationResult::Failed(e))
            },
        }
    }

    /// Synchronous code to pop data from a SharedNetworkQueue and its underlying POSIX socket of optional [size]. This
    /// function schedules the asynchronous coroutine and performs any necessary synchronous, multi-queue operations
    /// at the libOS-level before beginning the pop.
    pub fn pop(&mut self, qd: QDesc, size: Option<usize>) -> Result<QToken, Fail> {
        trace!("[w{:?}] pop() qd={:?}, size={:?}", self.id, qd, size);

        // We just assert 'size' here, because it was previously checked at PDPIX layer.
        debug_assert!(size.is_none() || ((size.unwrap() > 0) && (size.unwrap() <= limits::POP_SIZE_MAX)));

        // let cb: *mut SharedControlBlock<SharedDPDKRuntime> = unsafe { *(*(*self.shared_between_cores).qd_to_cb).get_mut(&qd).unwrap() };
        let cb: *mut SharedControlBlock<SharedDPDKRuntime> = unsafe { (*(*self.shared_between_cores).qd_to_cb).get_mut(qd).unwrap() };
        
        let coroutine_constructor = || -> Result<QToken, Fail> {
            // let task_name: String = format!("[w{:?}] NetworkLibOS::pop for qd={:?}", self.id, qd);
            let task_name = "NetworkLibOS::pop for qd";
            let coroutine_factory =
                |yielder| -> Pin<Box<Operation>> { Box::pin(self.clone().pop_coroutine(qd, cb, size, yielder).fuse()) };
            self.runtime
                .clone()
                .insert_coroutine_with_tracking(task_name, coroutine_factory, qd)
        };
        // queue.pop(coroutine_constructor)

        coroutine_constructor()
    }

    /// Asynchronous code to pop data from a SharedNetworkQueue and its underlying POSIX socket of optional [size]. This
    /// function returns a coroutine that asynchronously runs pop and performs any necessary multi-queue operations at
    /// the libOS-level after the pop succeeds or fails.
    async fn pop_coroutine(self, qd: QDesc, cb: *mut SharedControlBlock<SharedDPDKRuntime>, _size: Option<usize>, yielder: Yielder) -> (QDesc, OperationResult) {
        loop {
            if unsafe { (*(*cb).lock).trylock() } {
                if let Some(buf) = unsafe { (*cb).try_pop() } {
                    unsafe { (*(*cb).lock).unlock() }
                    return (qd, OperationResult::Pop(None, buf));
                }
                unsafe { (*(*cb).lock).unlock() }
            }

            match yielder.yield_once().await {
                Ok(_) => continue,
                Err(e) => panic!("Error: {:?}", e.cause)
            }
        }
    }

    /// This function gets a shared queue reference out of the I/O queue table. The type if a ref counted pointer to the
    /// queue itself.
    fn get_shared_queue(&mut self, qd: &QDesc) -> Result<SharedNetworkQueue<T>, Fail> {
        // self.runtime.get_shared_queue::<SharedNetworkQueue<T>>(qd)
        unsafe { (*self.shared_between_cores).get_shared_queue::<SharedNetworkQueue<T>>(qd) }
    }

    /// This exposes the transport for testing purposes.
    pub fn get_transport(&self) -> T {
        self.transport.clone()
    }

    /// This exposes the transport for testing purposes.
    pub fn get_runtime(&self) -> SharedDemiRuntime {
        self.runtime.clone()
    }

    #[allow(unused)]
    pub fn set_qd(&mut self, qd: QDesc) {
        unsafe { (*self.shared_between_cores).set_qd(qd) }
    }

    #[allow(unused)]
    pub fn get_qd(&self) -> QDesc {
        unsafe { (*self.shared_between_cores).get_qd() }
    }

    #[allow(unused)]
    pub fn wait_for_something(&mut self, qts: &[QToken], output: &mut Vec<demi_qresult_t>) {
        let mut a = self.ready_indices.clone();
        while output.is_empty() {
            a.clear();
            self.runtime.poll(&mut a);

            for qt in qts {
                if let Ok(true) = self.runtime.has_completed(*qt) {
                    if let Ok(qr) = self.runtime.remove_coroutine_and_get_result(*qt) {
                        output.push(qr);
                    }
                }
            }
        }
    }

    #[allow(unused)]
    pub async fn poll_for_established(mut self, yielder: Yielder) {
        let id = self.id;
        loop {
            if let Ok((qd, cb)) = unsafe { (*(*self.shared_between_cores).conn_list)[id].pop(&yielder).await } {
                let network = ((&self.transport as &dyn std::any::Any).downcast_ref::<SharedInetStack<SharedDPDKRuntime>>().unwrap().clone()).get_network();

                let poll_yielder = Yielder::new();
                let coroutine = unsafe { (*cb).poll(poll_yielder, network).fuse() };
                let _ = self.runtime.insert_background_coroutine(
                    // format!("[w{:?}] Inetstack::TCP::established::background for qd={:?}", id, qd).as_str(), 
                    "Inetstack::TCP::established::background for qd=",
                    Box::pin(coroutine)
                );

                log::warn!("[w{:?}] Waking up the FakeAcceptCoroutine", id);
                self.shared_queue.push(qd);
            }
        }
    }

}

//======================================================================================================================
// Trait Implementations
//======================================================================================================================

impl<T: NetworkTransport> Drop for NetworkLibOS<T> {
    // Releases all sockets allocated by Catnap.
    fn drop(&mut self) {
        for boxed_queue in self.runtime.get_mut_qtable().drain() {
            match downcast_queue::<SharedNetworkQueue<T>>(boxed_queue) {
                Ok(mut queue) => {
                    if let Err(e) = queue.hard_close() {
                        error!("close() failed (error={:?}", e);
                    }
                },
                Err(_) => {
                    error!("drop(): attempting to drop something that is not a SharedNetworkQueue");
                },
            }
        }
    }
}

impl<T: NetworkTransport> Deref for SharedNetworkLibOS<T> {
    type Target = NetworkLibOS<T>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T: NetworkTransport> DerefMut for SharedNetworkLibOS<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

impl<T: NetworkTransport> MemoryRuntime for SharedNetworkLibOS<T> {
    fn clone_sgarray(&self, sga: &demi_sgarray_t) -> Result<DemiBuffer, Fail> {
        self.transport.clone_sgarray(sga)
    }

    fn into_sgarray(&self, buf: DemiBuffer) -> Result<demi_sgarray_t, Fail> {
        self.transport.into_sgarray(buf)
    }

    fn sgaalloc(&self, size: usize) -> Result<demi_sgarray_t, Fail> {
        self.transport.sgaalloc(size)
    }

    fn sgafree(&self, sga: demi_sgarray_t) -> Result<(), Fail> {
        self.transport.sgafree(sga)
    }
}

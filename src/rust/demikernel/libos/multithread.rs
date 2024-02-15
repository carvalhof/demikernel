// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use super::singlethread::SingleThreadLibOS;
use crate::{
    runtime::{
        fail::Fail,
        types::{
            demi_qresult_t,
            demi_sgarray_t,
        },
        QDesc,
        QToken,
    },
    LibOSName,
};
use ::std::{
    net::SocketAddr,
    sync::mpsc::{
        self,
        SendError,
    },
    thread::{
        self,
        JoinHandle,
    },
    time::Duration,
};

//======================================================================================================================
// Structures
//======================================================================================================================

/// LibOS operations.
enum LibOSOperation {
    CreatePipe(mpsc::Sender<LibOSResult>, String),
    OpenPipe(mpsc::Sender<LibOSResult>, String),
    Socket(mpsc::Sender<LibOSResult>, libc::c_int, libc::c_int, libc::c_int),
    Bind(mpsc::Sender<LibOSResult>, QDesc, SocketAddr),
    Listen(mpsc::Sender<LibOSResult>, QDesc, usize),
    Accept(mpsc::Sender<LibOSResult>, QDesc),
    Connect(mpsc::Sender<LibOSResult>, QDesc, SocketAddr),
    Close(mpsc::Sender<LibOSResult>, QDesc),
    AsyncClose(mpsc::Sender<LibOSResult>, QDesc),
    Push(mpsc::Sender<LibOSResult>, QDesc, demi_sgarray_t),
    Pushto(mpsc::Sender<LibOSResult>, QDesc, demi_sgarray_t, SocketAddr),
    Pop(mpsc::Sender<LibOSResult>, QDesc, Option<usize>),
    Wait(mpsc::Sender<LibOSResult>, QToken, Option<Duration>),
    WaitAny(mpsc::Sender<LibOSResult>, Vec<QToken>, Option<Duration>),
    SGAAlloc(mpsc::Sender<LibOSResult>, usize),
    SGAFree(mpsc::Sender<LibOSResult>, demi_sgarray_t),
    Exit(),
}

/// Results for LibOS operations.
enum LibOSResult {
    CreatePipe(Result<QDesc, Fail>),
    OpenPipe(Result<QDesc, Fail>),
    Socket(Result<QDesc, Fail>),
    Bind(Result<(), Fail>),
    Listen(Result<(), Fail>),
    Accept(Result<QToken, Fail>),
    Connect(Result<QToken, Fail>),
    Close(Result<(), Fail>),
    AsyncClose(Result<QToken, Fail>),
    Push(Result<QToken, Fail>),
    Pushto(Result<QToken, Fail>),
    Pop(Result<QToken, Fail>),
    Wait(Result<demi_qresult_t, Fail>),
    WaitAny(Result<(usize, demi_qresult_t), Fail>),
    SGAAlloc(Result<demi_sgarray_t, Fail>),
    SGAFree(Result<(), Fail>),
}

/// A multithreaded LibOS.
pub struct MultiThreadLibOS {
    thread: Option<JoinHandle<Result<(), Fail>>>,
    operation_tx: mpsc::Sender<LibOSOperation>,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl MultiThreadLibOS {
    /// Instantiates a Multithread LibOS.
    pub fn new(libos_name: LibOSName) -> Result<Self, Fail> {
        let (operation_tx, operation_rx): (mpsc::Sender<LibOSOperation>, mpsc::Receiver<LibOSOperation>) =
            mpsc::channel::<LibOSOperation>();
        let thread: JoinHandle<Result<(), Fail>> = thread::spawn(|| -> Result<(), Fail> {
            let libos: SingleThreadLibOS = SingleThreadLibOS::new(libos_name)?;
            Self::run(libos, operation_rx);
            Ok(())
        });
        Ok(MultiThreadLibOS {
            thread: Some(thread),
            operation_tx,
        })
    }

    /// Creates a new memory queue and connect to consumer end.
    pub fn create_pipe(&mut self, name: &str) -> Result<QDesc, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::CreatePipe(tx, name.to_string())) {
            Ok(LibOSResult::CreatePipe(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "create_pipe() received unexpected result")),
        }
    }

    /// Opens an existing memory queue and connects to producer end.
    pub fn open_pipe(&mut self, name: &str) -> Result<QDesc, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::OpenPipe(tx, name.to_string())) {
            Ok(LibOSResult::OpenPipe(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "open_pipe() received unexpected result")),
        }
    }

    /// Creates a socket.
    pub fn socket(
        &mut self,
        domain: libc::c_int,
        socket_type: libc::c_int,
        protocol: libc::c_int,
    ) -> Result<QDesc, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Socket(tx, domain, socket_type, protocol)) {
            Ok(LibOSResult::Socket(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "socket() received unexpected result")),
        }
    }

    /// Binds a socket to a local address.
    pub fn bind(&mut self, sockqd: QDesc, local: SocketAddr) -> Result<(), Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Bind(tx, sockqd, local)) {
            Ok(LibOSResult::Bind(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "bind() received unexpected result")),
        }
    }

    /// Marks a socket as a passive one.
    pub fn listen(&mut self, sockqd: QDesc, backlog: usize) -> Result<(), Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Listen(tx, sockqd, backlog)) {
            Ok(LibOSResult::Listen(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "listen() received unexpected result")),
        }
    }

    /// Accepts an incoming connection on a TCP socket.
    pub fn accept(&mut self, sockqd: QDesc) -> Result<QToken, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Accept(tx, sockqd)) {
            Ok(LibOSResult::Accept(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "accept() received unexpected result")),
        }
    }

    /// Initiates a connection with a remote TCP socket.
    pub fn connect(&mut self, sockqd: QDesc, remote: SocketAddr) -> Result<QToken, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Connect(tx, sockqd, remote)) {
            Ok(LibOSResult::Connect(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "connect() received unexpected result")),
        }
    }

    /// Synchronously closes an I/O queue.
    pub fn close(&mut self, qd: QDesc) -> Result<(), Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Close(tx, qd)) {
            Ok(LibOSResult::Close(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "close() received unexpected result")),
        }
    }

    /// Asynchronously closes an I/O queue.
    pub fn async_close(&mut self, qd: QDesc) -> Result<QToken, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::AsyncClose(tx, qd)) {
            Ok(LibOSResult::AsyncClose(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "async_close() received unexpected result")),
        }
    }

    /// Pushes a scatter-gather array to an I/O queue.
    pub fn push(&mut self, qd: QDesc, sga: &demi_sgarray_t) -> Result<QToken, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Push(tx, qd, sga.clone())) {
            Ok(LibOSResult::Push(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "push() received unexpected result")),
        }
    }

    /// Pushes a scatter-gather array to a UDP socket.
    pub fn pushto(&mut self, qd: QDesc, sga: &demi_sgarray_t, to: SocketAddr) -> Result<QToken, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Pushto(tx, qd, sga.clone(), to)) {
            Ok(LibOSResult::Pushto(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "pushto() received unexpected result")),
        }
    }

    /// Pops data from a an I/O queue.
    pub fn pop(&mut self, qd: QDesc, size: Option<usize>) -> Result<QToken, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Pop(tx, qd, size)) {
            Ok(LibOSResult::Pop(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "pop() received unexpected result")),
        }
    }

    /// Waits for a pending I/O operation to complete or a timeout to expire.
    pub fn wait(&mut self, qt: QToken, timeout: Option<Duration>) -> Result<demi_qresult_t, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::Wait(tx, qt, timeout)) {
            Ok(LibOSResult::Wait(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "wait() received unexpected result")),
        }
    }

    /// Waits for any of the given pending I/O operations to complete or a timeout to expire.
    pub fn wait_any(&mut self, qts: &[QToken], timeout: Option<Duration>) -> Result<(usize, demi_qresult_t), Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::WaitAny(tx, qts.to_vec(), timeout)) {
            Ok(LibOSResult::WaitAny(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "wait_any() received unexpected result")),
        }
    }

    /// Allocates a scatter-gather array.
    pub fn sgaalloc(&mut self, size: usize) -> Result<demi_sgarray_t, Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::SGAAlloc(tx, size)) {
            Ok(LibOSResult::SGAAlloc(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "sgaalloc() received unexpected result")),
        }
    }

    /// Releases a scatter-gather array.
    pub fn sgafree(&mut self, sga: demi_sgarray_t) -> Result<(), Fail> {
        let (tx, rx): (mpsc::Sender<LibOSResult>, mpsc::Receiver<LibOSResult>) = mpsc::channel();
        match self.dispatch(rx, LibOSOperation::SGAFree(tx, sga)) {
            Ok(LibOSResult::SGAFree(result)) => result,
            Err(err) => Err(err),
            _ => Err(Fail::new(libc::EIO, "sgafree() received unexpected result")),
        }
    }

    /// Dispatches an operation to the LibOS thread.
    fn dispatch(&mut self, rx: mpsc::Receiver<LibOSResult>, operation: LibOSOperation) -> Result<LibOSResult, Fail> {
        if let Err(e) = self.operation_tx.send(operation) {
            return Err(Fail::new(libc::EIO, &format!("operation failed: {:?}", e)));
        }
        match rx.recv() {
            Ok(result) => Ok(result),
            Err(err) => Err(Fail::new(libc::EIO, &format!("operation failed: {:?}", err))),
        }
    }

    /// Handles result dispatching.
    fn handle_result(result: Result<(), SendError<LibOSResult>>) {
        error!(
            "run(): failed to send operation result to application thread: {:?}",
            result
        );
    }

    /// Runs the LibOS operations.
    fn run(libos: SingleThreadLibOS, operation_rx: mpsc::Receiver<LibOSOperation>) {
        let mut libos: SingleThreadLibOS = libos;
        loop {
            if let Ok(operation) = operation_rx.recv() {
                match operation {
                    LibOSOperation::CreatePipe(tx, name) => {
                        Self::handle_result(tx.send(LibOSResult::CreatePipe(libos.create_pipe(&name))));
                    },
                    LibOSOperation::OpenPipe(tx, name) => {
                        Self::handle_result(tx.send(LibOSResult::OpenPipe(libos.open_pipe(&name))));
                    },
                    LibOSOperation::Socket(tx, domain, socket_type, protocol) => {
                        Self::handle_result(tx.send(LibOSResult::Socket(libos.socket(domain, socket_type, protocol))));
                    },
                    LibOSOperation::Bind(tx, sockqd, local) => {
                        Self::handle_result(tx.send(LibOSResult::Bind(libos.bind(sockqd, local))));
                    },
                    LibOSOperation::Listen(tx, sockqd, backlog) => {
                        Self::handle_result(tx.send(LibOSResult::Listen(libos.listen(sockqd, backlog))));
                    },
                    LibOSOperation::Accept(tx, sockqd) => {
                        Self::handle_result(tx.send(LibOSResult::Accept(libos.accept(sockqd))));
                    },
                    LibOSOperation::Connect(tx, sockqd, remote) => {
                        Self::handle_result(tx.send(LibOSResult::Connect(libos.connect(sockqd, remote))));
                    },
                    LibOSOperation::Close(tx, qd) => {
                        Self::handle_result(tx.send(LibOSResult::Close(libos.close(qd))));
                    },
                    LibOSOperation::AsyncClose(tx, qd) => {
                        Self::handle_result(tx.send(LibOSResult::AsyncClose(libos.async_close(qd))));
                    },
                    LibOSOperation::Push(tx, qd, sga) => {
                        Self::handle_result(tx.send(LibOSResult::Push(libos.push(qd, &sga))));
                    },
                    LibOSOperation::Pushto(tx, qd, sga, to) => {
                        Self::handle_result(tx.send(LibOSResult::Pushto(libos.pushto(qd, &sga, to))));
                    },
                    LibOSOperation::Pop(tx, qd, size) => {
                        Self::handle_result(tx.send(LibOSResult::Pop(libos.pop(qd, size))));
                    },
                    LibOSOperation::Wait(tx, qt, timeout) => {
                        Self::handle_result(tx.send(LibOSResult::Wait(libos.wait(qt, timeout))));
                    },
                    LibOSOperation::WaitAny(tx, qts, timeout) => {
                        Self::handle_result(tx.send(LibOSResult::WaitAny(libos.wait_any(&qts, timeout))));
                    },
                    LibOSOperation::SGAAlloc(tx, size) => {
                        Self::handle_result(tx.send(LibOSResult::SGAAlloc(libos.sgaalloc(size))));
                    },
                    LibOSOperation::SGAFree(tx, sga) => {
                        Self::handle_result(tx.send(LibOSResult::SGAFree(libos.sgafree(sga))));
                    },
                    LibOSOperation::Exit() => {
                        break;
                    },
                }
            }
        }
    }
}

//======================================================================================================================
// Trait Implementations
//======================================================================================================================

impl Drop for MultiThreadLibOS {
    fn drop(&mut self) {
        if let Err(e) = self.operation_tx.send(LibOSOperation::Exit()) {
            error!("drop(): failed to send exit operation to libos thread: {:?}", e);
        } else {
            let thread: JoinHandle<Result<(), Fail>> = self.thread.take().expect("libos should have a thread");
            let _ = thread.join();
        }
    }
}

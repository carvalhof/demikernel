// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

mod acknowledger;
mod retransmitter;
mod sender;

use self::{
    acknowledger::acknowledger,
    retransmitter::retransmitter,
    sender::sender,
};
use crate::{
    async_timer,
    inetstack::protocols::tcp::established::ctrlblk::SharedControlBlock,
    runtime::{
        network::NetworkRuntime,
        QDesc,
    },
};
use ::futures::{
    channel::mpsc,
    pin_mut,
    FutureExt,
};

pub async fn background<N: NetworkRuntime>(cb: *mut SharedControlBlock<N>, _dead_socket_tx: mpsc::UnboundedSender<QDesc>) {
    let acknowledger = async_timer!("tcp::established::background::acknowledger", acknowledger(cb)).fuse();
    pin_mut!(acknowledger);

    let retransmitter = async_timer!("tcp::established::background::retransmitter", retransmitter(cb)).fuse();
    pin_mut!(retransmitter);

    let sender = async_timer!("tcp::established::background::sender", sender(cb)).fuse();
    pin_mut!(sender);

    let receiver = async_timer!("tcp::established::background::receiver", unsafe { (*cb).poll() }).fuse();
    pin_mut!(receiver);

    let r = futures::select_biased! {
        r = receiver => r,
        r = acknowledger => r,
        r = retransmitter => r,
        r = sender => r,
    };
    error!("Connection terminated: {:?}", r);
}

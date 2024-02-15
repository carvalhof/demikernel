// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

pub mod memory;
pub mod multithread;
pub mod name;
pub mod network;
pub mod singlethread;

#[cfg(not(feature = "multithread"))]
pub use singlethread::SingleThreadLibOS as LibOS;

#[cfg(feature = "multithread")]
pub use multithread::MultiThreadLibOS as LibOS;

use windows::{
    core::{
        HRESULT,
        HSTRING,
    },
    Win32::{
        Foundation::{
            RtlNtStatusToDosError,
            ERROR_ABANDONED_WAIT_0,
            ERROR_ACCESS_DENIED,
            ERROR_ALREADY_EXISTS,
            ERROR_INSUFFICIENT_BUFFER,
            ERROR_INVALID_HANDLE,
            ERROR_INVALID_PARAMETER,
            ERROR_IO_INCOMPLETE,
            ERROR_IO_PENDING,
            ERROR_MORE_DATA,
            ERROR_NOT_ENOUGH_MEMORY,
            ERROR_OPERATION_ABORTED,
            ERROR_SUCCESS,
            NTSTATUS,
            STATUS_ABANDONED,
            STATUS_SUCCESS,
            STATUS_TIMEOUT,
            WAIT_TIMEOUT,
            WIN32_ERROR,
        },
        Networking::WinSock::{
            self,
            WSAGetLastError,
            WSABASEERR,
            WSA_ERROR,
            WSA_IO_PENDING,
        },
    },
};

use crate::runtime::fail::Fail;

use super::overlapped::OverlappedResult;

pub fn try_translate_wsa_error(err: WSA_ERROR) -> Option<libc::errno_t> {
    if err.0 < WSABASEERR.0 {
        return None;
    }

    let value: libc::errno_t = match err {
        // Winsock errors with errno equivalents
        WinSock::WSAEINTR => libc::EINTR,
        WinSock::WSAEBADF => libc::EBADF,
        WinSock::WSAEACCES => libc::EACCES,
        WinSock::WSAEFAULT => libc::EFAULT,
        WinSock::WSAEINVAL => libc::EINVAL,
        WinSock::WSAEMFILE => libc::EMFILE,
        WinSock::WSAEWOULDBLOCK => libc::EWOULDBLOCK,
        WinSock::WSAEINPROGRESS => libc::EINPROGRESS,
        WinSock::WSAEALREADY => libc::EALREADY,
        WinSock::WSAENOTSOCK => libc::ENOTSOCK,
        WinSock::WSAEDESTADDRREQ => libc::EDESTADDRREQ,
        WinSock::WSAEMSGSIZE => libc::EMSGSIZE,
        WinSock::WSAEPROTOTYPE => libc::EPROTOTYPE,
        WinSock::WSAENOPROTOOPT => libc::ENOPROTOOPT,
        WinSock::WSAEPROTONOSUPPORT => libc::EPROTONOSUPPORT,
        WinSock::WSAEOPNOTSUPP => libc::EOPNOTSUPP,
        WinSock::WSAEAFNOSUPPORT => libc::EAFNOSUPPORT,
        WinSock::WSAEADDRINUSE => libc::EADDRINUSE,
        WinSock::WSAEADDRNOTAVAIL => libc::EADDRNOTAVAIL,
        WinSock::WSAENETDOWN => libc::ENETDOWN,
        WinSock::WSAENETUNREACH => libc::ENETUNREACH,
        WinSock::WSAENETRESET => libc::ENETRESET,
        WinSock::WSAECONNABORTED => libc::ECONNABORTED,
        WinSock::WSAECONNRESET => libc::ECONNRESET,
        WinSock::WSAENOBUFS => libc::ENOBUFS,
        WinSock::WSAEISCONN => libc::EISCONN,
        WinSock::WSAENOTCONN => libc::ENOTCONN,
        WinSock::WSAETIMEDOUT => libc::ETIMEDOUT,
        WinSock::WSAECONNREFUSED => libc::ECONNREFUSED,
        WinSock::WSAELOOP => libc::ELOOP,
        WinSock::WSAENAMETOOLONG => libc::ENAMETOOLONG,
        WinSock::WSAEHOSTUNREACH => libc::EHOSTUNREACH,
        WinSock::WSAENOTEMPTY => libc::ENOTEMPTY,
        WinSock::WSAESOCKTNOSUPPORT => libc::EPROTONOSUPPORT,
        WinSock::WSAEPFNOSUPPORT => libc::EPROTONOSUPPORT,

        // Winsock errors with errno equivalents missing from Rust libc
        WinSock::WSAESHUTDOWN => libc::EINVAL,       /*libc::ESHUTDOWN*/
        WinSock::WSAETOOMANYREFS => libc::EINVAL,    /*libc::ETOOMANYREFS*/
        WinSock::WSAEHOSTDOWN => libc::EHOSTUNREACH, /*libc::EHOSTDOWN*/
        WinSock::WSAEPROCLIM => libc::ENOMEM,        /*libc::EPROCLIM*/
        WinSock::WSAEUSERS => libc::ENOMEM,          /*libc::EUSERS*/
        WinSock::WSAEDQUOT => libc::ENOSPC,          /*libc::EDQUOT*/
        WinSock::WSAESTALE => libc::EINVAL,          /*libc::ESTALE*/
        WinSock::WSAEREMOTE => libc::EINVAL,         /*libc::EREMOTE*/

        // Winsock errors without direct errno equivalence.
        //
        // System state errors
        WinSock::WSASYSNOTREADY => libc::EFAULT,
        WinSock::WSAVERNOTSUPPORTED => libc::EFAULT,
        WinSock::WSANOTINITIALISED => libc::ENODEV,
        WinSock::WSAEPROVIDERFAILEDINIT => libc::ENODEV,
        WinSock::WSASERVICE_NOT_FOUND => libc::ENODEV,
        WinSock::WSATYPE_NOT_FOUND => libc::ENODEV,

        // Operation state errors
        WinSock::WSATRY_AGAIN => libc::EAGAIN,
        WinSock::WSA_E_CANCELLED => libc::ECANCELED,
        WinSock::WSANO_RECOVERY => libc::EFAULT,
        WinSock::WSAEDISCON => libc::ENOTCONN,

        // Condition/invariant violation
        WinSock::WSASYSCALLFAILURE => libc::EFAULT,

        /* The following codes are also represented in WIN32_ERROR codes and will not be checked here:
        WSA_INVALID_HANDLE is ERROR_INVALID_HANDLE
        WSA_INVALID_PARAMETER is ERROR_INVALID_PARAMETER
        WSA_IO_INCOMPLETE is ERROR_IO_INCOMPLETE
        WSA_IO_PENDING is WSA_IO_PENDING
        WSA_OPERATION_ABORTED is ERROR_OPERATION_ABORTED
        WSA_NOT_ENOUGH_MEMORY is ERROR_NOT_ENOUGH_MEMORY */
        // Everything else.
        _ => return None,
    };

    Some(value)
}

fn wsa_error_to_win_error(err: WSA_ERROR) -> windows::core::Error {
    let hresult: HRESULT = WIN32_ERROR(err.0 as u32).into();
    windows::core::Error::new(hresult, HSTRING::new())
}

pub fn last_overlapped_wsa_error(ok_result: Option<OverlappedResult>) -> Result<Option<OverlappedResult>, Fail> {
    let wsa_error: WSA_ERROR = unsafe { WSAGetLastError() };
    if wsa_error == WSA_IO_PENDING {
        Ok(ok_result)
    } else {
        let error: windows::core::Error = wsa_error_to_win_error(wsa_error);
        if error.code().is_ok() {
            Ok(ok_result)
        } else {
            Err(error.into())
        }
    }
}

pub fn get_overlapped_api_result(
    api_success: bool,
    completion_key: usize,
    bytes_transferred: u32,
) -> Result<Option<OverlappedResult>, Fail> {
    if api_success {
        Ok(Some(OverlappedResult::new(
            completion_key,
            STATUS_SUCCESS,
            bytes_transferred,
        )))
    } else {
        last_overlapped_wsa_error(None)
    }
}

pub fn expect_last_wsa_error() -> Fail {
    // Safety: FFI; no major considerations.
    wsa_error_to_win_error(unsafe { WSAGetLastError() }).into()
}

/// Translate a small subset of Win32 error codes which we may be interested in distinguishing to errno_t.
pub fn translate_win32_error(error: WIN32_ERROR) -> libc::errno_t {
    // WSA_ERROR and WIN32_ERROR share a domain. As HRESULTs, they are in the same class.
    if let Some(err) = try_translate_wsa_error(WSA_ERROR(error.0 as i32)) {
        return err;
    }

    // WAIT_TIMEOUT is a WAIT_EVENT, but the error code is a WIN32_ERROR.
    const ERROR_WAIT_TIMEOUT: WIN32_ERROR = WIN32_ERROR(WAIT_TIMEOUT.0);

    match error {
        ERROR_ACCESS_DENIED => libc::EACCES,
        ERROR_NOT_ENOUGH_MEMORY => libc::ENOMEM,
        ERROR_ALREADY_EXISTS => libc::EEXIST,
        ERROR_INVALID_HANDLE => libc::EINVAL,
        ERROR_INVALID_PARAMETER => libc::EINVAL,
        ERROR_IO_INCOMPLETE => libc::EIO,
        ERROR_IO_PENDING => libc::EINPROGRESS,
        ERROR_OPERATION_ABORTED => libc::ECANCELED,
        ERROR_INSUFFICIENT_BUFFER => libc::EOVERFLOW,
        ERROR_MORE_DATA => libc::EOVERFLOW,
        ERROR_WAIT_TIMEOUT => libc::ETIMEDOUT,

        // This can have multiple meanings; generally it is associated with a wait object which is closed while some
        // thread is waiting. EBADF indicates the handle is now invalid.
        ERROR_ABANDONED_WAIT_0 => libc::EBADF,
        _ => libc::EFAULT,
    }
}

pub fn translate_ntstatus(status: NTSTATUS) -> WIN32_ERROR {
    match status {
        // A few common error codes to skip for fast path.
        STATUS_SUCCESS => ERROR_SUCCESS,
        STATUS_TIMEOUT => WIN32_ERROR(WAIT_TIMEOUT.0),
        STATUS_ABANDONED => ERROR_ABANDONED_WAIT_0,

        // Lookup via Windows API.
        status => unsafe { WIN32_ERROR(RtlNtStatusToDosError(status)) },
    }
}

impl From<WSA_ERROR> for Fail {
    fn from(value: WSA_ERROR) -> Self {
        let error: windows::core::Error = wsa_error_to_win_error(value);
        error.into()
    }
}

impl From<WIN32_ERROR> for Fail {
    fn from(value: WIN32_ERROR) -> Self {
        let error: windows::core::Error = windows::core::Error::new(value.into(), HSTRING::new());
        error.into()
    }
}

impl From<&windows::core::Error> for Fail {
    fn from(value: &windows::core::Error) -> Self {
        let cause: String = format!("{}", value);
        let errno: libc::errno_t = match WIN32_ERROR::from_error(value) {
            Some(error) => translate_win32_error(error),
            None => libc::EFAULT,
        };
        Fail::new(errno, cause.as_str())
    }
}

impl From<windows::core::Error> for Fail {
    fn from(value: windows::core::Error) -> Self {
        Self::from(&value)
    }
}

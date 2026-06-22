//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    fmt,
    fs::{remove_file, File, OpenOptions},
    os::unix::net::SocketAddr,
    sync::Arc,
    time::Duration,
};

#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;

use tokio::select;

use async_trait::async_trait;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::RwLock as AsyncRwLock,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zenoh_core::{zasyncread, zasyncwrite};
use zenoh_link_commons::{
    LinkAuthId, LinkManagerUnicastTrait, LinkUnicast, LinkUnicastTrait, NewLinkChannelSender,
};
use zenoh_protocol::{
    core::{EndPoint, Locator, Priority},
    transport::BatchSize,
};
use zenoh_result::{zerror, ZResult};

use super::{
    get_unix_path_as_string, UNIXSOCKSTREAM_ACCEPT_THROTTLE_TIME, UNIXSOCKSTREAM_DEFAULT_MTU,
    UNIXSOCKSTREAM_LOCATOR_PREFIX,
};

pub struct LinkUnicastUnixSocketStream {
    // The underlying socket as returned from the tokio library
    socket: UnsafeCell<UnixStream>,
    // The Unix domain socket source path
    src_locator: Locator,
    // The Unix domain socket destination path (random UUIDv4)
    dst_locator: Locator,
}

unsafe impl Sync for LinkUnicastUnixSocketStream {}

impl LinkUnicastUnixSocketStream {
    fn new(socket: UnixStream, src_path: &str, dst_path: &str) -> LinkUnicastUnixSocketStream {
        LinkUnicastUnixSocketStream {
            socket: UnsafeCell::new(socket),
            src_locator: Locator::new(UNIXSOCKSTREAM_LOCATOR_PREFIX, src_path, "").unwrap(),
            dst_locator: Locator::new(UNIXSOCKSTREAM_LOCATOR_PREFIX, dst_path, "").unwrap(),
        }
    }

    #[allow(clippy::mut_from_ref)]
    fn get_mut_socket(&self) -> &mut UnixStream {
        unsafe { &mut *self.socket.get() }
    }
}

#[async_trait]
impl LinkUnicastTrait for LinkUnicastUnixSocketStream {
    async fn close(&self) -> ZResult<()> {
        tracing::trace!("Closing UnixSocketStream link: {}", self);
        // Close the underlying UnixSocketStream socket
        let res = self.get_mut_socket().shutdown().await;
        tracing::trace!("UnixSocketStream link shutdown {}: {:?}", self, res);
        res.map_err(|e| zerror!(e).into())
    }

    async fn write(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<usize> {
        self.get_mut_socket().write(buffer).await.map_err(|e| {
            let e = zerror!("Write error on UnixSocketStream link {}: {}", self, e);
            tracing::trace!("{}", e);
            e.into()
        })
    }

    async fn write_all(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<()> {
        self.get_mut_socket().write_all(buffer).await.map_err(|e| {
            let e = zerror!("Write error on UnixSocketStream link {}: {}", self, e);
            tracing::trace!("{}", e);
            e.into()
        })
    }

    async fn read(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<usize> {
        self.get_mut_socket().read(buffer).await.map_err(|e| {
            let e = zerror!("Read error on UnixSocketStream link {}: {}", self, e);
            tracing::trace!("{}", e);
            e.into()
        })
    }

    async fn read_exact(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<()> {
        self.get_mut_socket()
            .read_exact(buffer)
            .await
            .map(|_len| ())
            .map_err(|e| {
                let e = zerror!("Read error on UnixSocketStream link {}: {}", self, e);
                tracing::trace!("{}", e);
                e.into()
            })
    }

    #[inline(always)]
    fn get_src(&self) -> &Locator {
        &self.src_locator
    }

    #[inline(always)]
    fn get_dst(&self) -> &Locator {
        &self.dst_locator
    }

    #[inline(always)]
    fn get_mtu(&self) -> BatchSize {
        *UNIXSOCKSTREAM_DEFAULT_MTU
    }

    #[inline(always)]
    fn get_interface_names(&self) -> Vec<String> {
        vec![] // Unix sockets don't have network interfaces
    }

    #[inline(always)]
    fn is_reliable(&self) -> bool {
        super::IS_RELIABLE
    }

    #[inline(always)]
    fn is_streamed(&self) -> bool {
        true
    }

    #[inline(always)]
    fn get_auth_id(&self) -> &LinkAuthId {
        &LinkAuthId::UnixsockStream
    }
}

impl Drop for LinkUnicastUnixSocketStream {
    fn drop(&mut self) {
        // Close the underlying UnixSocketStream socket
        let _ = zenoh_runtime::ZRuntime::Acceptor
            .block_in_place(async move { self.get_mut_socket().shutdown().await });
    }
}

impl fmt::Display for LinkUnicastUnixSocketStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", &self.src_locator, &self.dst_locator)?;
        Ok(())
    }
}

impl fmt::Debug for LinkUnicastUnixSocketStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixSocketStream")
            .field("src", &self.src_locator)
            .field("dst", &self.dst_locator)
            .finish()
    }
}

/*************************************/
/*          LISTENER                 */
/*************************************/
struct ListenerUnixSocketStream {
    endpoint: EndPoint,
    token: CancellationToken,
    handle: JoinHandle<ZResult<()>>,
    lock_file: Option<File>,
}

impl ListenerUnixSocketStream {
    fn new(
        endpoint: EndPoint,
        token: CancellationToken,
        handle: JoinHandle<ZResult<()>>,
        lock_file: Option<File>,
    ) -> ListenerUnixSocketStream {
        ListenerUnixSocketStream {
            endpoint,
            token,
            handle,
            lock_file,
        }
    }

    async fn stop(&self) {
        self.token.cancel();
    }
}

pub struct LinkManagerUnicastUnixSocketStream {
    manager: NewLinkChannelSender,
    listeners: Arc<AsyncRwLock<HashMap<String, ListenerUnixSocketStream>>>,
}

impl fmt::Debug for LinkManagerUnicastUnixSocketStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkManagerUnicastUnixSocketStream")
            .field("manager", &self.manager)
            .field("listeners", &"..")
            .finish()
    }
}

impl LinkManagerUnicastUnixSocketStream {
    pub fn new(manager: NewLinkChannelSender) -> Self {
        Self {
            manager,
            listeners: Arc::new(AsyncRwLock::new(HashMap::new())),
        }
    }
}

fn to_socket_addr(path: &str) -> ZResult<(SocketAddr, bool)> {
    if let Some(abstract_name) = path.strip_prefix('@') {
        #[cfg(target_os = "linux")]
        {
            let addr = SocketAddr::from_abstract_name(abstract_name.as_bytes())
                .map_err(|e| zerror!("{}", e))?;
            Ok((addr, true))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = abstract_name;
            Err(zerror!("Abstract Unix sockets ('@') are only supported on Linux").into())
        }
    } else {
        let addr = SocketAddr::from_pathname(path).map_err(|e| zerror!("{}", e))?;
        Ok((addr, false))
    }
}

fn socket_addr_to_string(addr: &SocketAddr) -> Option<String> {
    if let Some(path) = addr.as_pathname() {
        path.to_str().map(|s| s.to_owned())
    } else {
        #[cfg(target_os = "linux")]
        {
            addr.as_abstract_name()
                .map(|name| format!("@{}", String::from_utf8_lossy(name)))
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }
}

fn tokio_socket_addr_to_string(addr: &tokio::net::unix::SocketAddr) -> Option<String> {
    if let Some(path) = addr.as_pathname() {
        path.to_str().map(|s| s.to_owned())
    } else if addr.is_unnamed() {
        None
    } else {
        #[cfg(target_os = "linux")]
        {
            let std_addr = unsafe {
                let raw_addr = addr as *const tokio::net::unix::SocketAddr
                    as *const std::os::unix::net::SocketAddr;
                &*raw_addr
            };
            std_addr
                .as_abstract_name()
                .map(|name| format!("@{}", String::from_utf8_lossy(name)))
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }
}

#[async_trait]
impl LinkManagerUnicastTrait for LinkManagerUnicastUnixSocketStream {
    async fn new_link(&self, endpoint: EndPoint) -> ZResult<LinkUnicast> {
        let path = get_unix_path_as_string(endpoint.address());
        let (addr, _) = to_socket_addr(&path)?;

        // Create the UnixSocketStream connection
        let std_stream = std::os::unix::net::UnixStream::connect_addr(&addr).map_err(|e| {
            let e = zerror!(
                "Can not create a new UnixSocketStream link bound to {:?}: {}",
                path,
                e
            );
            tracing::warn!("{}", e);
            e
        })?;

        std_stream
            .set_nonblocking(true)
            .map_err(|e| zerror!("{}", e))?;

        let src_addr = std_stream.local_addr().map_err(|e| {
            let e = zerror!(
                "Can not create a new UnixSocketStream link bound to {:?}: {}",
                path,
                e
            );
            tracing::warn!("{}", e);
            e
        })?;

        // We do need the dst_addr value, we just need to check that is valid
        let _dst_addr = std_stream.peer_addr().map_err(|e| {
            let e = zerror!(
                "Can not create a new UnixSocketStream link bound to {:?}: {}",
                path,
                e
            );
            tracing::warn!("{}", e);
            e
        })?;

        let local_path_str = match socket_addr_to_string(&src_addr) {
            Some(p) => p,
            None => format!("{}", Uuid::new_v4()),
        };

        let stream = UnixStream::from_std(std_stream).map_err(|e| zerror!("{}", e))?;
        let remote_path_str = path.as_str();

        let link: Arc<dyn LinkUnicastTrait> = Arc::new(LinkUnicastUnixSocketStream::new(
            stream,
            &local_path_str,
            remote_path_str,
        ));

        Ok(LinkUnicast::from(link))
    }

    async fn new_listener(&self, mut endpoint: EndPoint) -> ZResult<Locator> {
        let path = get_unix_path_as_string(endpoint.address());
        let (addr, is_abstract) = to_socket_addr(&path)?;

        // Because of the lack of SO_REUSEADDR we have to check if the
        // file is still there and if it is not used by another process.
        // In order to do so we use a separate lock file.
        // If the lock CAN NOT be acquired means that another process is
        // holding the lock NOW, therefore we cannot use the socket.
        // Kernel guarantees that the lock is release if the owner exists
        // or crashes.

        // If the lock CAN be acquired means no one is using the socket.
        // Therefore we can unlink the socket file and create the new one with
        // bind(2)

        let mut lock_file_opt = None;

        if !is_abstract {
            let lock_file_path = format!("{path}.lock");
            let lock_file_path_clone = lock_file_path.clone();
            let path_clone = path.clone();

            let lock_result = tokio::task::spawn_blocking(move || {
                use std::os::unix::fs::MetadataExt;

                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&lock_file_path_clone)
                    .map_err(|e| {
                        zerror!(
                            "Can not create a new UnixSocketStream listener on {} - Unable to open lock file: {}",
                            path_clone, e
                        )
                    })?;

                if let Err(err) = file.try_lock() {
                    return Err(match err {
                        std::fs::TryLockError::WouldBlock => zerror!(
                            "Can not create a new UnixSocketStream listener on {} - Unable to acquire lock: WouldBlock",
                            path_clone
                        ),
                        std::fs::TryLockError::Error(e) => zerror!(
                            "Can not create a new UnixSocketStream listener on {} - Unable to acquire lock: {}",
                            path_clone, e
                        ),
                    });
                }

                let meta_fd = file.metadata().map_err(|e| zerror!("{}", e))?;
                let meta_disk = std::fs::metadata(&lock_file_path_clone).map_err(|e| zerror!("{}", e))?;
                if meta_fd.dev() != meta_disk.dev() || meta_fd.ino() != meta_disk.ino() {
                    return Err(zerror!(
                        "Can not create a new UnixSocketStream listener on {} - Lockfile was substituted during acquisition",
                        path_clone
                    ));
                }

                let _ = remove_file(path_clone);
                Ok(file)
            })
            .await
            .map_err(|e| zerror!("Spawn blocking join error: {}", e))?;

            match lock_result {
                Ok(file) => {
                    lock_file_opt = Some(file);
                }
                Err(e) => {
                    tracing::warn!("{}", e);
                    return Err(e.into());
                }
            }
        }

        // Bind the Unix socket
        let std_listener = std::os::unix::net::UnixListener::bind_addr(&addr).map_err(|e| {
            let e = zerror!(
                "Can not create a new UnixSocketStream listener on {}: {}",
                path,
                e
            );
            tracing::warn!("{}", e);
            e
        })?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| zerror!("{}", e))?;

        let local_addr = std_listener.local_addr().map_err(|e| {
            let e = zerror!(
                "Can not create a new UnixSocketStream listener on {}: {}",
                path,
                e
            );
            tracing::warn!("{}", e);
            e
        })?;

        let local_path_str = socket_addr_to_string(&local_addr).ok_or_else(|| {
            let e = zerror!("Can not create a new UnixSocketStream listener on {}", path);
            tracing::warn!("{}", e);
            e
        })?;

        let socket = UnixListener::from_std(std_listener).map_err(|e| zerror!("{}", e))?;

        // Update the endpoint with the actual local path
        endpoint = EndPoint::new(
            endpoint.protocol(),
            &local_path_str,
            endpoint.metadata(),
            endpoint.config(),
        )?;

        // Spawn the accept loop for the listener
        let token = CancellationToken::new();
        let c_token = token.clone();
        let mut listeners = zasyncwrite!(self.listeners);

        let task = {
            let manager = self.manager.clone();
            let listeners = self.listeners.clone();
            let path = local_path_str.to_owned();

            async move {
                // Wait for the accept loop to terminate
                let res = accept_task(socket, c_token, manager).await;
                zasyncwrite!(listeners).remove(&path);
                res
            }
        };
        let handle = zenoh_runtime::ZRuntime::Acceptor.spawn(task);

        let locator = endpoint.to_locator();
        let listener = ListenerUnixSocketStream::new(endpoint, token, handle, lock_file_opt);
        listeners.insert(local_path_str.to_owned(), listener);

        Ok(locator)
    }

    async fn del_listener(&self, endpoint: &EndPoint) -> ZResult<()> {
        let path = get_unix_path_as_string(endpoint.address());

        // Stop the listener
        let listener = zasyncwrite!(self.listeners).remove(&path).ok_or_else(|| {
            let e = zerror!(
                "Can not delete the UnixSocketStream listener because it has not been found: {}",
                path
            );
            tracing::trace!("{}", e);
            e
        })?;

        // Send the stop signal
        listener.stop().await;
        listener.handle.await??;

        // Release the lock safely
        if let Some(file) = listener.lock_file {
            let lock_file_path = format!("{path}.lock");
            let path_clone = path.clone();

            tokio::task::spawn_blocking(move || {
                let _ = remove_file(path_clone);

                let tmp = remove_file(lock_file_path);
                tracing::trace!("UnixSocketStream Domain Socket removal result: {:?}", tmp);

                let _ = file.unlock();
            })
            .await
            .map_err(|e| zerror!("Spawn blocking join error: {}", e))?;
        }

        Ok(())
    }

    async fn get_listeners(&self) -> Vec<EndPoint> {
        zasyncread!(self.listeners)
            .values()
            .map(|x| x.endpoint.clone())
            .collect()
    }

    async fn get_locators(&self) -> Vec<Locator> {
        zasyncread!(self.listeners)
            .values()
            .map(|x| x.endpoint.to_locator())
            .collect()
    }
}

async fn accept_task(
    socket: UnixListener,
    token: CancellationToken,
    manager: NewLinkChannelSender,
) -> ZResult<()> {
    let src_addr = socket.local_addr().map_err(|e| {
        zerror!("Can not accept UnixSocketStream connections: {}", e);
        tracing::warn!("{}", e);
        e
    })?;

    let src_path = match tokio_socket_addr_to_string(&src_addr) {
        Some(p) => p,
        None => {
            let e = zerror!(
                "Can not create a new UnixSocketStream link bound to {:?}",
                src_addr
            );
            tracing::warn!("{}", e);
            return Err(e.into());
        }
    };

    tracing::trace!(
        "Ready to accept UnixSocketStream connections on: {}",
        src_path
    );

    loop {
        select! {
            _ = token.cancelled() => break,

            res = socket.accept() => {
                let (stream, _) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        // Throttle the accept loop upon an error
                        // NOTE: This might be due to various factors. However, the most common case is that
                        //       the process has reached the maximum number of open files in the system. On
                        //       Linux systems this limit can be changed by using the "ulimit" command line
                        //       tool. In case of systemd-based systems, this can be changed by using the
                        //       "sysctl" command line tool.
                        tracing::warn!("{}. Hint: increase the system open file limit.", e);
                        tokio::time::sleep(Duration::from_micros(*UNIXSOCKSTREAM_ACCEPT_THROTTLE_TIME)).await;
                        continue;
                    }
                };

                let dst_path = Uuid::new_v4().to_string();

                tracing::debug!("Accepted UnixSocketStream connection on: {:?}", src_addr);

                let link: Arc<dyn LinkUnicastTrait> = Arc::new(
                    LinkUnicastUnixSocketStream::new(stream, &src_path, &dst_path),
                );

                if let Err(e) = manager.send_async(LinkUnicast::from(link)).await {
                    tracing::error!("{}-{}: {}", file!(), line!(), e);
                }
            }
        }
    }

    Ok(())
}

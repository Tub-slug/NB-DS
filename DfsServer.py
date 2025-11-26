# dfs_server.py
import grpc
from concurrent import futures
import sys
import traceback
import threading
import queue
import socket
import uuid
import time

import DfsService_pb2
import DfsService_pb2_grpc

import extentService_pb2
import extentService_pb2_grpc

import lockService_pb2
import lockService_pb2_grpc

import lockCacheService_pb2
import lockCacheService_pb2_grpc


#ExtentCache
class ExtentCache:
    def __init__(self, extent_stub):
        self.extent_stub = extent_stub
        self.cache = {}
        self.lock = threading.Lock()
        print("[EXT_CACHE] Extent Cache initialized")

    def get(self, dfs_name):
        with self.lock:
            if dfs_name in self.cache:
                entry = self.cache[dfs_name]
                if not entry['present']:
                    # It was cached as "deleted"
                    print(f"[EXT_CACHE] get({dfs_name}): hit (deleted)")
                    return None
                # Cache hit
                print(f"[EXT_CACHE] get({dfs_name}): hit")
                return entry['data']
        
        # Cache miss, fetch from server
        print(f"[EXT_CACHE] get({dfs_name}): miss (fetching)")
        try:
            ext_req = extentService_pb2.GetRequest(fileName=dfs_name)
            ext_resp = self.extent_stub.get(ext_req)
            
            data = None
            present = False
            if ext_resp.HasField("fileData"):
                data = ext_resp.fileData
                present = True
            
            # Store in cache
            with self.lock:
                self.cache[dfs_name] = {'data': data, 'dirty': False, 'present': present}
            
            return data
        except Exception as e:
            print(f"[EXT_CACHE] get() RPC error: {e}")
            return None

    def put(self, dfs_name, data=None):
        with self.lock:
            if data is not None:
                # Write/Create
                print(f"[EXT_CACHE] put({dfs_name}): write (dirty)")
                self.cache[dfs_name] = {'data': data, 'dirty': True, 'present': True}
            else:
                # Delete
                print(f"[EXT_CACHE] put({dfs_name}): delete (dirty)")
                self.cache[dfs_name] = {'data': None, 'dirty': True, 'present': False}

    def update(self, dfs_name):
        entry = None
        with self.lock:
            if dfs_name not in self.cache or not self.cache[dfs_name]['dirty']:
                # Not dirty or not in cache
                print(f"[EXT_CACHE] update({dfs_name}): not dirty or not in cache")
                return
            
            # Copy entry to release lock during RPC
            entry = self.cache[dfs_name].copy()
        
        if entry:
            print(f"[EXT_CACHE] update({dfs_name}): writing back to server")
            try:
                if entry['present']:
                    # Write/Create
                    ext_req = extentService_pb2.PutRequest(fileName=dfs_name, fileData=entry['data'])
                else:
                    # Delete
                    ext_req = extentService_pb2.PutRequest(fileName=dfs_name)
                
                # Make RPC call
                self.extent_stub.put(ext_req)
                
                # Mark as clean
                with self.lock:
                    if dfs_name in self.cache: # Check again
                        self.cache[dfs_name]['dirty'] = False
                        print(f"[EXT_CACHE] update({dfs_name}): marked as clean")
            except Exception as e:
                print(f"[EXT_CACHE] update() RPC error: {e}")

    def flush(self, dfs_name):
        with self.lock:
            if dfs_name in self.cache:
                print(f"[EXT_CACHE] flush({dfs_name}): removing from cache")
                del self.cache[dfs_name]
            else:
                print(f"[EXT_CACHE] flush({dfs_name}): not in cache")


class LockCache:
    """
    Lock cache states:
    - none: client knows nothing about this lock
    - free: client owns the lock, no thread has it
    - locked: client owns the lock, a thread has it
    - acquiring: client is acquiring ownership
    - releasing: client is releasing ownership
    """
    
    def __init__(self, lock_stub, client_id, extent_cache):
        self.lock_stub = lock_stub
        self.client_id = client_id
        self.extent_cache = extent_cache
        
        self.locks = {}
        self.lock = threading.Lock()
        
        # Sequence number per lock
        self.next_seq = {}
        
        # Queue for releaser task
        self.release_queue = queue.Queue()
        
        # Flag to stop background tasks
        self.running = True
        
        # Start releaser task
        self.releaser_thread = threading.Thread(target=self._releaser_task, daemon=True)
        self.releaser_thread.start()
        
        print(f"[LOCK_CACHE] Lock Cache initialized for client {client_id}")
    
    def _get_next_seq(self, lock_id):
        if lock_id not in self.next_seq:
            self.next_seq[lock_id] = 0
        seq = self.next_seq[lock_id]
        self.next_seq[lock_id] += 1
        return seq
    
    def acquire(self, lock_id):
        print(f"[LOCK_CACHE] acquire({lock_id}) called")
        
        with self.lock:
            if lock_id not in self.locks:
                self.locks[lock_id] = {
                    'status': 'none',
                    'seq': -1,
                    'condition': threading.Condition(self.lock),
                    'revoke_requested': False,
                    'retry_received': False
                }
            
            lock_entry = self.locks[lock_id]
            condition = lock_entry['condition']
        
        with condition:
            while True:
                status = lock_entry['status']
                
                if status == 'free':
                    # check if revoke was requested
                    if lock_entry['revoke_requested']:
                        #release first
                        lock_entry['status'] = 'releasing'
                        seq = lock_entry['seq']
                        lock_entry['revoke_requested'] = False
                        
                        print(f"[LOCK_CACHE] Lock {lock_id} free, but revoke requested. Updating/flushing.")
                        condition.release()
                        try:
                            self.extent_cache.update(lock_id)
                            self.extent_cache.flush(lock_id)
                        finally:
                            condition.acquire()
                        
                        # Queue for releaser
                        self.release_queue.put((lock_id, seq))
                        
                        # Go to acquiring state
                        lock_entry['status'] = 'acquiring'
                        new_seq = self._get_next_seq(lock_id)
                        lock_entry['seq'] = new_seq
                        lock_entry['retry_received'] = False
                        
                        # Try to acquire from server
                        condition.release()
                        try:
                            success = self._acquire_from_server(lock_id, new_seq)
                        finally:
                            condition.acquire()
                        
                        if success:
                            lock_entry['status'] = 'locked'
                            print(f"[LOCK_CACHE] Lock {lock_id} acquired from server after revoke")
                            return
                        else:
                            # Wait for retry
                            print(f"[LOCK_CACHE] Lock {lock_id} busy, waiting for retry")
                            continue
                    else:
                        #take it
                        lock_entry['status'] = 'locked'
                        print(f"[LOCK_CACHE] Lock {lock_id} acquired from cache")
                        return
                
                elif status == 'none':
                    #need acquire from server
                    lock_entry['status'] = 'acquiring'
                    seq = self._get_next_seq(lock_id)
                    lock_entry['seq'] = seq
                    lock_entry['retry_received'] = False
                    
                    # Release condition lock to make RPC
                    condition.release()
                    try:
                        success = self._acquire_from_server(lock_id, seq)
                    finally:
                        condition.acquire()
                    
                    if success:
                        lock_entry['status'] = 'locked'
                        print(f"[LOCK_CACHE] Lock {lock_id} acquired from server")
                        return
                    else:
                        # Server RETRY
                        print(f"[LOCK_CACHE] Lock {lock_id} busy, waiting for retry")
                        continue
                
                elif status == 'acquiring':
                    if lock_entry['retry_received']:
                        lock_entry['retry_received'] = False
                        
                        # Check if revoke was requested while waiting
                        if lock_entry['revoke_requested']:
                            # Don't acquire, go back to none
                            lock_entry['status'] = 'none'
                            lock_entry['revoke_requested'] = False
                            print(f"[LOCK_CACHE] Lock {lock_id} revoked during acquisition, restarting")
                            # Continue loop to try again from 'none' state
                            continue
                        
                        # Try to acquire again
                        seq = lock_entry['seq']
                        condition.release()
                        try:
                            success = self._acquire_from_server(lock_id, seq)
                        finally:
                            condition.acquire()
                        
                        if success:
                            lock_entry['status'] = 'locked'
                            print(f"[LOCK_CACHE] Lock {lock_id} acquired after retry")
                            return
                        else:
                            # Still busy
                            print(f"[LOCK_CACHE] Lock {lock_id} still busy after retry")
                            continue
                    else:
                        # Another thread is acquiring
                        condition.wait()
                
                elif status == 'locked':
                    # Another thread on this client has it
                    condition.wait()
                
                elif status == 'releasing':
                    # Being released
                    condition.wait()
        
    def release(self, lock_id):
        print(f"[LOCK_CACHE] release({lock_id}) called")
        
        with self.lock:
            if lock_id not in self.locks:
                print(f"[LOCK_CACHE] Warning: releasing unknown lock {lock_id}")
                return
            
            lock_entry = self.locks[lock_id]
            condition = lock_entry['condition']
        
        with condition:
            if lock_entry['status'] != 'locked':
                print(f"[LOCK_CACHE] Warning: releasing lock {lock_id} in state {lock_entry['status']}")
                return
            
            # Always update extent before potentially releasing
            print(f"[LOCK_CACHE] Lock {lock_id} being released. Updating extent.")
            condition.release()
            try:
                self.extent_cache.update(lock_id)
            finally:
                condition.acquire()
            
            # Check if revoke was requested
            if lock_entry['revoke_requested']:
                # Need to release back to server and flush
                lock_entry['status'] = 'releasing'
                seq = lock_entry['seq']
                lock_entry['revoke_requested'] = False
                
                print(f"[LOCK_CACHE] Lock {lock_id} released due to revoke. Flushing.")
                condition.release()
                try:
                    self.extent_cache.flush(lock_id)
                finally:
                    condition.acquire()
                
                # Queue for releaser task
                self.release_queue.put((lock_id, seq))
                
                # Set to none loose ownership
                lock_entry['status'] = 'none'
                condition.notify_all()
                print(f"[LOCK_CACHE] Lock {lock_id} queued for release to server")
            else:
                # Cache it
                lock_entry['status'] = 'free'
                condition.notify_all()
                print(f"[LOCK_CACHE] Lock {lock_id} cached locally")
    
    def _acquire_from_server(self, lock_id, seq):
        """
        Make RPC call to server to acquire lock.
        Returns True if granted, False if RETRY.
        """
        try:
            req = lockService_pb2.AcquireRequest(
                lockId=lock_id,
                ownerId=self.client_id,
                sequence=seq
            )
            resp = self.lock_stub.acquire(req, timeout=10.0)
            return resp.success
        except Exception as e:
            print(f"[LOCK_CACHE] Error in acquire RPC: {e}")
            traceback.print_exc()
            return False
    
    def _release_to_server(self, lock_id, seq):
        """
        Make RPC call to server to release lock.
        """
        try:
            req = lockService_pb2.ReleaseRequest(
                lockId=lock_id,
                ownerId=self.client_id
            )
            self.lock_stub.release(req, timeout=10.0)
            print(f"[LOCK_CACHE] Released {lock_id} to server")
        except Exception as e:
            print(f"[LOCK_CACHE] Error in release RPC: {e}")
            traceback.print_exc()
    
    def _releaser_task(self):
        """
        Background task that releases locks to the server.
        """
        print("[LOCK_CACHE] Releaser task started")
        while self.running:
            try:
                item = self.release_queue.get(timeout=0.5)
                lock_id, seq = item
                self._release_to_server(lock_id, seq)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[LOCK_CACHE] Releaser error: {e}")
                traceback.print_exc()
        
        print("[LOCK_CACHE] Releaser task stopped")
    
    def handle_retry(self, lock_id, seq_num):
        """
        Called when server sends retry()
        """
        print(f"[LOCK_CACHE] Received retry for {lock_id}, seq={seq_num}")
        
        with self.lock:
            if lock_id not in self.locks:
                print(f"[LOCK_CACHE] Retry for unknown lock {lock_id}")
                return
            
            lock_entry = self.locks[lock_id]
            condition = lock_entry['condition']
        
        with condition:
            # Mark that retry was received
            if lock_entry['seq'] == seq_num:
                lock_entry['retry_received'] = True
                condition.notify_all()
                print(f"[LOCK_CACHE] Retry marked for {lock_id}, waking threads")
            else:
                print(f"[LOCK_CACHE] Retry seq mismatch for {lock_id}: expected {lock_entry['seq']}, got {seq_num}")
    
    def handle_revoke(self, lock_id):
        """
        Called when server sends revoke() 
        """
        print(f"[LOCK_CACHE] Received revoke for {lock_id}")
        
        with self.lock:
            if lock_id not in self.locks:
                print(f"[LOCK_CACHE] Revoke for unknown lock {lock_id}")
                return
            
            lock_entry = self.locks[lock_id]
            condition = lock_entry['condition']
        
        with condition:
            # Mark that revoke was requested
            lock_entry['revoke_requested'] = True
            
            if lock_entry['status'] == 'free':
                # No one is using, release immediately
                
                print(f"[LOCK_CACHE] Lock {lock_id} free, processing revoke immediately. Updating/flushing.")
                condition.release()
                try:
                    self.extent_cache.update(lock_id)
                    self.extent_cache.flush(lock_id)
                finally:
                    condition.acquire()
                
                lock_entry['status'] = 'releasing'
                seq = lock_entry['seq']
                self.release_queue.put((lock_id, seq))
                lock_entry['status'] = 'none'
                lock_entry['revoke_requested'] = False
                condition.notify_all()
                print(f"[LOCK_CACHE] Lock {lock_id} released immediately")
            
            elif lock_entry['status'] == 'acquiring':
                # Still acquiring
                print(f"[LOCK_CACHE] Lock {lock_id} marked for revoke (acquiring)")
                condition.notify_all() 
            
            elif lock_entry['status'] == 'locked':
                # Someone using it
                print(f"[LOCK_CACHE] Lock {lock_id} will be released when thread releases it")
            
            elif lock_entry['status'] == 'releasing':
                # Already being released
                print(f"[LOCK_CACHE] Lock {lock_id} already being released")
    
    def stop(self):
        """Stop the lock cache."""
        print("[LOCK_CACHE] Stopping lock cache...")
        self.running = False
        if self.releaser_thread and self.releaser_thread.is_alive():
            self.releaser_thread.join(timeout=2.0)


class LockCacheService(lockCacheService_pb2_grpc.LockCacheServiceServicer):
    """
    gRPC service that receives retry() and revoke() calls from the lock server.
    """
    
    def __init__(self, lock_cache):
        self.lock_cache = lock_cache
    
    def retry(self, request):
        self.lock_cache.handle_retry(request.lockId, request.sequence) 
        return lockCacheService_pb2.RetryResponse()
    
    def revoke(self, request):
        self.lock_cache.handle_revoke(request.lockId)
        return lockCacheService_pb2.RevokeResponse()


class DfsService(DfsService_pb2_grpc.DfsServiceServicer):
    # __init__ to accept stubs and caches from serve()
    def __init__(self, extent_stub, lock_stub, client_id, lock_cache, extent_cache):
        self.extent_stub = extent_stub
        self.lock_stub = lock_stub
        self.lock_cache = lock_cache
        self.extent_cache = extent_cache 

    def _acquire_lock(self, path):
        self.lock_cache.acquire(path)

    def _release_lock(self, path):
        self.lock_cache.release(path)

    def dir(self, request, context):
        directory = request.directoryName
        if not directory.endswith("/"):
            directory = directory + "/"

        try:
            self._acquire_lock(directory)
            try:
                data_bytes = self.extent_cache.get(directory)
            finally:
                self._release_lock(directory)
        except Exception as e:
            print(f"[DFS] dir() error: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.DirResponse() 

        try:
            # Handle None return from cache
            if data_bytes is None:
                return DfsService_pb2.DirResponse()
            data = data_bytes.decode("utf-8")
            if data == "":
                return DfsService_pb2.DirResponse(success=True, dirList=[])
            entries = [e for e in data.split("\n") if e != ""]
            return DfsService_pb2.DirResponse(success=True, dirList=entries)
        except Exception as e:
            print(f"[DFS] dir() error parsing: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.DirResponse()

    def mkdir(self, request, context):
        directory = request.directoryName
        if not directory.endswith("/"):
            directory = directory + "/"

        if directory == "/":
            return DfsService_pb2.MkdirResponse(success=False)

        stripped_path = directory.strip('/')
        parts = stripped_path.split('/')
        name = parts[-1]
        
        parent_path = "/"
        if len(parts) > 1:
            parent_path = '/' + '/'.join(parts[:-1]) + '/'

        try:
            # 1. Lock and update parent directory
            self._acquire_lock(parent_path)
            try:
                parent_content_bytes = self.extent_cache.get(parent_path)
                
                # Decode, or start fresh if None
                parent_content = ""
                if parent_content_bytes is not None:
                    parent_content = parent_content_bytes.decode('utf-8')

                # Check if entry already exists
                entries = [e for e in parent_content.split('\n') if e]
                if name + "/" in entries:
                    # Directory already exists
                    return DfsService_pb2.MkdirResponse(success=False)

                # Add new directory entry (with trailing slash)
                new_parent_content = parent_content + name + "/\n"
                
                # Put it back and update immediately
                self.extent_cache.put(parent_path, new_parent_content.encode('utf-8'))
                self.extent_cache.update(parent_path)

            finally:
                self._release_lock(parent_path)

            # 2. Create the new directory's own empty extent
            self._acquire_lock(directory)
            try:
                # Data is b"" for a new directory
                self.extent_cache.put(directory, b"")
                # Update immediately to persist to extent server
                self.extent_cache.update(directory)
            finally:
                self._release_lock(directory)
            
            return DfsService_pb2.MkdirResponse(success=True)
        except Exception as e:
            print(f"[DFS] mkdir() error: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.MkdirResponse(success=False)

    def rmdir(self, request, context):
        directory = request.directoryName
        if not directory.endswith("/"):
            directory = directory + "/"

        # Don't allow deleting root
        if directory == "/":
            return DfsService_pb2.RmdirResponse(success=False)

        # Get parent and new directory name
        stripped_path = directory.strip('/')
        parts = stripped_path.split('/')
        name = parts[-1]
        
        parent_path = "/"
        if len(parts) > 1:
            parent_path = '/' + '/'.join(parts[:-1]) + '/'

        try:
            # 1. Lock and update parent directory
            self._acquire_lock(parent_path)
            try:
                parent_content_bytes = self.extent_cache.get(parent_path)
                
                if parent_content_bytes is not None:
                    parent_content = parent_content_bytes.decode('utf-8')
                    entries = [e for e in parent_content.split('\n') if e]
                    
                    # Remove the directory entry
                    entry_to_remove = name + "/"
                    if entry_to_remove in entries:
                        entries.remove(entry_to_remove)
                        new_parent_content = "\n".join(entries) + "\n"
                        self.extent_cache.put(parent_path, new_parent_content.encode('utf-8'))
                        self.extent_cache.update(parent_path)
                    else:
                        # Entry not in parent, which is strange but we can proceed
                        pass
            finally:
                self._release_lock(parent_path)

            # 2. Delete the directory's own extent
            self._acquire_lock(directory)
            try:
                # Use extent_cache.put() with None for deletion
                self.extent_cache.put(directory, None)
                self.extent_cache.update(directory)
            finally:
                self._release_lock(directory)
            
            return DfsService_pb2.RmdirResponse(success=True)
        except Exception as e:
            print(f"[DFS] rmdir() error: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.RmdirResponse(success=False)

    def get(self, request, context):
        filename = request.fileName
        if filename.endswith("/"):
            return DfsService_pb2.GetResponse()

        try:
            self._acquire_lock(filename)
            try:
                # Use extent_cache.get()
                data_bytes = self.extent_cache.get(filename)
            finally:
                self._release_lock(filename)
        except Exception as e:
            print(f"[DFS] get() error: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.GetResponse()

        try:
            # Handle None return from cache
            if data_bytes is None:
                return DfsService_pb2.GetResponse()
            return DfsService_pb2.GetResponse(fileData=data_bytes)
        except Exception as e:
            print(f"[DFS] get() error parsing: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.GetResponse()

    def put(self, request, context):
        filename = request.fileName
        if filename.endswith("/"):
            return DfsService_pb2.PutResponse(success=False)

        if not request.HasField("fileData"):
            return DfsService_pb2.PutResponse(success=False)

        # Get parent and file name
        stripped_path = filename.strip('/')
        parts = stripped_path.split('/')
        name = parts[-1]
        
        parent_path = "/"
        if len(parts) > 1:
            parent_path = '/' + '/'.join(parts[:-1]) + '/'

        try:
            # 1. Lock and update parent directory
            self._acquire_lock(parent_path)
            try:
                parent_content_bytes = self.extent_cache.get(parent_path)
                
                parent_content = ""
                if parent_content_bytes is not None:
                    parent_content = parent_content_bytes.decode('utf-8')

                # Check if entry already exists
                entries = [e for e in parent_content.split('\n') if e]
                if name in entries:
                    # File already exists, this is an overwrite, so no need to update parent dir list
                    pass
                else:
                    # Add new file entry and update immediately
                    new_parent_content = parent_content + name + "\n"
                    self.extent_cache.put(parent_path, new_parent_content.encode('utf-8'))
                    self.extent_cache.update(parent_path)

            finally:
                self._release_lock(parent_path)

            # 2. Create/update the file's own extent
            self._acquire_lock(filename)
            try:
                self.extent_cache.put(filename, request.fileData)
                # Update immediately to persist to extent server
                self.extent_cache.update(filename)
            finally:
                self._release_lock(filename)
            
            return DfsService_pb2.PutResponse(success=True)
        except Exception as e:
            print(f"[DFS] put() error: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.PutResponse(success=False)

    def delete(self, request, context):
        filename = request.fileName
        if filename.endswith("/"):
            return DfsService_pb2.DeleteResponse(success=False)

        # Get parent and file name
        stripped_path = filename.strip('/')
        parts = stripped_path.split('/')
        name = parts[-1]
        
        parent_path = "/"
        if len(parts) > 1:
            parent_path = '/' + '/'.join(parts[:-1]) + '/'

        try:
            # 1. Lock and update parent directory
            self._acquire_lock(parent_path)
            try:
                parent_content_bytes = self.extent_cache.get(parent_path)
                
                if parent_content_bytes is not None:
                    parent_content = parent_content_bytes.decode('utf-8')
                    entries = [e for e in parent_content.split('\n') if e]
                    
                    # Remove the file entry
                    if name in entries:
                        entries.remove(name)
                        new_parent_content = "\n".join(entries) + "\n"
                        self.extent_cache.put(parent_path, new_parent_content.encode('utf-8'))
                        # Update immediately to persist deletion
                        self.extent_cache.update(parent_path)
                    else:
                        # Entry not in parent, which is strange but we can proceed
                        pass
            finally:
                self._release_lock(parent_path)

            # 2. Delete the file's own extent
            self._acquire_lock(filename)
            try:
                # Use extent_cache.put() with None for deletion
                self.extent_cache.put(filename, None)
                # Update immediately to persist deletion
                self.extent_cache.update(filename)
            finally:
                self._release_lock(filename)
            
            return DfsService_pb2.DeleteResponse(success=True)
        except Exception as e:
            print(f"[DFS] delete() error: {e}", file=sys.stderr)
            traceback.print_exc()
            return DfsService_pb2.DeleteResponse(success=False)

    def stop(self, request, context):
        try:
            try:
                self.extent_stub.stop(extentService_pb2.StopRequest())
            except Exception:
                pass
            try:
                self.lock_stub.stop(lockService_pb2.StopRequest())
            except Exception:
                pass
        finally:
            global server, lock_cache_inst
            print("[DFS] Stopping DFS server...")
            if lock_cache_inst:
                lock_cache_inst.stop()
            if server:
                server.stop(0)
        return DfsService_pb2.StopResponse()

def get_local_ip():
    """Get local IP address (not localhost)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1' # Fallback
    finally:
        s.close()
    return IP


# Global variables
server = None
lock_cache_inst = None


def serve(port, extent_addr, lock_addr):
    global server, lock_cache_inst
    
    # Get local IP and create client ID
    local_ip = get_local_ip()
    client_name = str(uuid.uuid4())[:8]
    client_id = f"{local_ip}:{port}:{client_name}"
    
    print(f"[DFS] Client ID: {client_id}")
    
    #Create stubs and caches here to pass to constructors
    
    # Create extent channel and stub
    extent_channel = grpc.insecure_channel(extent_addr)
    extent_stub = extentService_pb2_grpc.ExtentServiceStub(extent_channel)
    
    # Create lock channel and stub
    lock_channel = grpc.insecure_channel(lock_addr)
    lock_stub = lockService_pb2_grpc.LockServiceStub(lock_channel)
    
    # Create extent cache
    extent_cache_inst = ExtentCache(extent_stub)
    
    # Create lock cache
    lock_cache_inst = LockCache(lock_stub, client_id, extent_cache_inst)
    
    # Create server with both services
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add DFS service 
    DfsService_pb2_grpc.add_DfsServiceServicer_to_server(
        DfsService(extent_stub, lock_stub, client_id, lock_cache_inst, extent_cache_inst), 
        server
    )
    
    # Add Lock Cache service
    lockCacheService_pb2_grpc.add_LockCacheServiceServicer_to_server(
        LockCacheService(lock_cache_inst),
        server
    )
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"DFS Service started on port {port}, extent: {extent_addr}, lock: {lock_addr}")
    print(f"DFS Service hosting both DfsService and LockCacheService on [::]:{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python dfs_server.py <port> <extent_host:port> <lock_host:port>")
        sys.exit(1)
    port = int(sys.argv[1])
    extent_addr = sys.argv[2]
    lock_addr = sys.argv[3]
    serve(port, extent_addr, lock_addr)
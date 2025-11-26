# lockServer.py
import grpc
from concurrent import futures
import threading
import time
import queue
import sys

import lockService_pb2
import lockService_pb2_grpc
import lockCacheService_pb2 
import lockCacheService_pb2_grpc


class LockService(lockService_pb2_grpc.LockServiceServicer):
    def __init__(self):
        self.locks = {}
        self.lock = threading.Lock()
        
        # Queues for pending requests
        # lockId -> list of (client_id, seq)
        self.waiting_clients = {}
        
        # Queues for Revoker and Retrier tasks
        self.revoke_queue = queue.Queue()
        self.retry_queue = queue.Queue()
        
        # Flag to stop background tasks
        self.running = True
        
        # Start background tasks
        self.revoker_thread = threading.Thread(target=self._revoker_task, daemon=True)
        self.retrier_thread = threading.Thread(target=self._retrier_task, daemon=True)
        self.revoker_thread.start()
        self.retrier_thread.start()
        
        print("[LOCK_SERVER] Lock Service initialized with caching support")

    def acquire(self, request, context):
        lock_id = request.lockId
        owner_id = request.ownerId
        seq_num = request.sequence
        
        print(f"[LOCK_SERVER] acquire({lock_id}) from {owner_id}, seq={seq_num}")
        
        with self.lock:
            if lock_id not in self.locks:
                # Lock doesn't exist, create and grant it
                self.locks[lock_id] = {
                    'owner': owner_id,
                    'seq': seq_num,
                    'revoke_pending': False  
                }
                print(f"[LOCK_SERVER] Lock {lock_id} granted to {owner_id}")
                return lockService_pb2.AcquireResponse(success=True)
            
            lock_entry = self.locks[lock_id]
            
            if lock_entry['owner'] == owner_id:
                # Same client re-acquiring (update sequence)
                lock_entry['seq'] = seq_num
                lock_entry['revoke_pending'] = False 
                print(f"[LOCK_SERVER] Lock {lock_id} re-granted to {owner_id}")
                return lockService_pb2.AcquireResponse(success=True)
            
            # Lock is owned by another client
            # Add to waiting list
            if lock_id not in self.waiting_clients:
                self.waiting_clients[lock_id] = []
            
            # Check if waiting 
            already_waiting = any(c == owner_id for c, s in self.waiting_clients[lock_id])
            if not already_waiting:
                self.waiting_clients[lock_id].append((owner_id, seq_num))
            
            #Only queue revoke if one isn't pending
            if not lock_entry.get('revoke_pending', False):
                lock_entry['revoke_pending'] = True
                # Queue a revoke request for current owner
                current_owner = lock_entry['owner']
                self.revoke_queue.put((lock_id, current_owner)) 
                print(f"[LOCK_SERVER] Queued REVOKE for {lock_id} to {current_owner}")
            else:
                print(f"[LOCK_SERVER] Revoke already pending for {lock_id}")
            
            print(f"[LOCK_SERVER] Lock {lock_id} busy (owner={lock_entry['owner']}), sending RETRY to {owner_id}")
            return lockService_pb2.AcquireResponse(success=False)

    def release(self, request, context):

        lock_id = request.lockId
        owner_id = request.ownerId
        
        print(f"[LOCK_SERVER] release({lock_id}) from {owner_id}")
        
        with self.lock:
            if lock_id not in self.locks:
                print(f"[LOCK_SERVER] Warning: release for non-existent lock {lock_id}")
                return lockService_pb2.ReleaseResponse()
            
            lock_entry = self.locks[lock_id]
            
            # Verify owner
            if lock_entry['owner'] != owner_id:
                print(f"[LOCK_SERVER] Warning: release from non-owner for {lock_id}")
                return lockService_pb2.ReleaseResponse()
            
            # Check if there are waiting clients
            if lock_id in self.waiting_clients and len(self.waiting_clients[lock_id]) > 0:
                # Grant to next waiting client
                next_client, next_seq = self.waiting_clients[lock_id].pop(0)
                lock_entry['owner'] = next_client
                lock_entry['seq'] = next_seq
                lock_entry['revoke_pending'] = False 
                
                # Send retry to the next client
                self.retry_queue.put((lock_id, next_client, next_seq))
                
                print(f"[LOCK_SERVER] Lock {lock_id} transferred to {next_client}")
                
                # Clean up empty waiting list
                if len(self.waiting_clients[lock_id]) == 0:
                    del self.waiting_clients[lock_id]
            else:
                # No waiting clients, mark as free but keep owner 
                lock_entry['revoke_pending'] = False
                print(f"[LOCK_SERVER] Lock {lock_id} released and cached by {owner_id}")
        
        return lockService_pb2.ReleaseResponse()

    def _revoker_task(self):
        print("[LOCK_SERVER] Revoker task started")
        while self.running:
            try:
                # Wait for revoke request with timeout
                item = self.revoke_queue.get(timeout=0.5)
                lock_id, client_id = item
                
                print(f"[LOCK_SERVER] Revoker: sending revoke to {client_id} for {lock_id}") 
                
                # Parse client_id: "host:port:name"
                try:
                    parts = client_id.split(':')
                    if len(parts) >= 2:
                        host = parts[0]
                        port = parts[1]
                        client_addr = f"{host}:{port}"
                        
                        # Create channel and stub
                        channel = grpc.insecure_channel(client_addr)
                        stub = lockCacheService_pb2_grpc.LockCacheServiceStub(channel)
                        
                        # Send revoke
                        req = lockCacheService_pb2.RevokeRequest(
                            lockId=lock_id
                        )
                        try:
                            stub.revoke(req, timeout=2.0)
                            print(f"[LOCK_SERVER] Revoke sent successfully to {client_id}")
                        except Exception as e:
                            print(f"[LOCK_SERVER] Error calling revoke: {e}")
                        finally:
                            try:
                                channel.close()
                            except:
                                pass
                    else:
                        print(f"[LOCK_SERVER] Invalid client_id format: {client_id}")
                except Exception as e:
                    print(f"[LOCK_SERVER] Error sending revoke to {client_id}: {e}")
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[LOCK_SERVER] Revoker error: {e}")
        
        print("[LOCK_SERVER] Revoker task stopped")

    def _retrier_task(self):
        print("[LOCK_SERVER] Retrier task started")
        while self.running:
            try:
                # Wait for retry request with timeout
                item = self.retry_queue.get(timeout=0.5)
                lock_id, client_id, seq_num = item
                
                print(f"[LOCK_SERVER] Retrier: sending retry to {client_id} for {lock_id}, seq={seq_num}")
                
                # Parse client_id: "host:port:name"
                try:
                    parts = client_id.split(':')
                    if len(parts) >= 2:
                        host = parts[0]
                        port = parts[1]
                        client_addr = f"{host}:{port}"
                        
                        # Create channel and stub
                        channel = grpc.insecure_channel(client_addr)
                        stub = lockCacheService_pb2_grpc.LockCacheServiceStub(channel)
                        
                        # Send retry
                        req = lockCacheService_pb2.RetryRequest(
                            lockId=lock_id,
                            sequence=seq_num 
                        )
                        try:
                            stub.retry(req, timeout=2.0)
                            print(f"[LOCK_SERVER] Retry sent successfully to {client_id}")
                        except Exception as e:
                            print(f"[LOCK_SERVER] Error calling retry: {e}")
                        finally:
                            try:
                                channel.close()
                            except:
                                pass
                    else:
                        print(f"[LOCK_SERVER] Invalid client_id format: {client_id}")
                except Exception as e:
                    print(f"[LOCK_SERVER] Error sending retry to {client_id}: {e}")
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[LOCK_SERVER] Retrier error: {e}")
        
        print("[LOCK_SERVER] Retrier task stopped")

    def stop(self, request, context):
        global server
        print("[LOCK_SERVER] Stopping server...")
        self.running = False
        # Wait for threads to finish
        time.sleep(1)
        server.stop(0)
        return lockService_pb2.StopResponse()


def serve(port: int):
    global server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lockService_pb2_grpc.add_LockServiceServicer_to_server(LockService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[LOCK_SERVER] Server started on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    serve(port)
# extent_server.py
import os
import grpc
from concurrent import futures

# importnuté pre gRPC
import extentService_pb2
import extentService_pb2_grpc


class extentService(extentService_pb2_grpc.ExtentServiceServicer):
    def __init__(self, root_path):
        self.root_path = os.path.abspath(root_path)
        os.makedirs(self.root_path, exist_ok=True)
        print(f"[SERVER] Extent root path: {self.root_path} (vytvorené ak nebolo)")

    def _to_real_path(self, dfs_path):
        dfs_path = dfs_path.lstrip("/")
        full = os.path.join(self.root_path, dfs_path)
        return full

    def get(self, request, context):
        dfs_name = request.fileName
        real_path = self._to_real_path(dfs_name)
        print(f"[SERVER] get(): {dfs_name} => {real_path}")

        try:
            # ak priečinok
            if dfs_name.endswith("/"):
                if not os.path.isdir(real_path):
                    print(f"[WARN] dir {real_path} nexistuje?")
                    return extentService_pb2.GetResponse()
                stuff = []
                for n in os.listdir(real_path):
                    fp = os.path.join(real_path, n)
                    if os.path.isdir(fp):
                        n += "/"
                    stuff.append(n)
                data = "\n".join(stuff).encode("utf-8")
                return extentService_pb2.GetResponse(fileData=data)
            else:
                # file načítanie
                if not os.path.isfile(real_path):
                    print(f"[WARN] súbor {real_path} neexistuje")
                    return extentService_pb2.GetResponse()
                with open(real_path, "rb") as f:
                    dat = f.read()
                return extentService_pb2.GetResponse(fileData=dat)
        except Exception as e:
            print(f"[ERR] get() zlyhalo: {e}")
            return extentService_pb2.GetResponse()  # empty > null

    def put(self, request, context):
        dfs_name = request.fileName
        real_path = self._to_real_path(dfs_name)
        data_present = request.HasField("fileData")

        # info log
        print(f"[SERVER] put(): {dfs_name} -> {real_path}, data_present={data_present}")

        try:
            # delete (žiadne fileData)
            if not data_present:
                if dfs_name.endswith("/"):
                    # zmazať prázdny priečinok
                    if os.path.isdir(real_path) and not os.listdir(real_path):
                        os.rmdir(real_path)
                        print(f"[INFO] rmdir OK: {real_path}")
                        return extentService_pb2.PutResponse(success=True)
                    else:
                        print(f"[WARN] rmdir FAIL: not empty / not exist")
                        return extentService_pb2.PutResponse(success=False)
                else:
                    # file delete
                    if os.path.exists(real_path):
                        os.remove(real_path)
                        print(f"[INFO] removed file: {real_path}")
                        return extentService_pb2.PutResponse(success=True)
                    print(f"[WARN] delete file FAIL: {real_path}")
                    return extentService_pb2.PutResponse(success=False)

            # make priečinok if no slash
            if dfs_name.endswith("/"):
                os.makedirs(real_path, exist_ok=True)
                print(f"[INFO] mkdir OK: {real_path}")
                return extentService_pb2.PutResponse(success=True)

            # zápis súboru 
            os.makedirs(os.path.dirname(real_path), exist_ok=True)
            with open(real_path, "wb") as f:
                f.write(request.fileData)
            print(f"[INFO] wrote file: {real_path}")
            return extentService_pb2.PutResponse(success=True)

        except Exception as e:
            print(f"[ERR] put() exception: {e}")
            return extentService_pb2.PutResponse(success=False)

    def stop(self, request, context):
        global server
        print("[SERVER] ExtentService stopping....")
        server.stop(0)
        return extentService_pb2.StopResponse()
# basic strt
def serve(port, root_path):
    global server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    extentService_pb2_grpc.add_ExtentServiceServicer_to_server(
        extentService(root_path), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[SERVER] ExtentService beží na porte {port}, root={root_path}")
    server.wait_for_termination()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("python extent_server.py <port> <root_path>")
        sys.exit(1)
    port = int(sys.argv[1])
    root_path = sys.argv[2]
    serve(port, root_path)
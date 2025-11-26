# Script starting/stopping DFS Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 3001) or address where to connect and stop the service (e.g. 127.0.0.1:3001)
# 3. Address and port where to connect Extent Service (format is IP:port, e.g. 127.0.0.1:2001)
# 4. Address and port where to connect Lock Service (format is IP:port, e.g. 127.0.0.1:1001)
#
# Examples how to use the script:
#
#.\dfsservice.ps1 start 3001 127.0.0.1:2001 127.0.0.1:1001
#.\dfsservice.ps1 stop 127.0.0.1:3001
$cmd = $args[0]; $port = $args[1]; $extent = $args[2]; $lock = $args[3]
$py = "python"
$script = ".\DfsServer.py"
if ($cmd -eq "start") {
  Start-Process $py -ArgumentList @($script,$port,$extent,$lock) -NoNewWindow -RedirectStandardOutput "dfs_out.log" -RedirectStandardError "dfs_err.log"
} elseif ($cmd -eq "stop") {
  $code = @"
import grpc
import DfsService_pb2
import DfsService_pb2_grpc
ch = grpc.insecure_channel('127.0.0.1:$port')
stub = DfsService_pb2_grpc.DfsServiceStub(ch)
stub.stop(DfsService_pb2.StopRequest())
print('stop sent')
"@
  $code | & $py -
}

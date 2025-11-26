# Script starting/stopping Lock Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 1001) or address where to connect and stop the service (e.g. 127.0.0.1:1001)
#
# Examples how to use the script:
#
# .\lockservice.ps1 start 1001
# .\lockservice.ps1 stop 127.0.0.1:1001
param(
    [string]$Cmd,
    [string]$Port
)


$PythonPath = "python"
$ServiceScript = ".\lockServer.py"


if ($Cmd -eq "start") {
    Write-Host "Starting Lock Service on port $Port..."
    $Arguments = @($ServiceScript, "$Port")
    Start-Process -FilePath $PythonPath -ArgumentList $Arguments -NoNewWindow `
        -RedirectStandardOutput "lockservice_out.log" `
        -RedirectStandardError "lockservice_err.log"
    Write-Host "Lock Service started (check lockservice_out.log for logs)"
}
elseif ($Cmd -eq "stop") {
    Write-Host "Stopping Lock Service on 127.0.0.1:$Port..."
    $StopScript = @"
import grpc
import lockService_pb2
import lockService_pb2_grpc

address = "127.0.0.1:$Port"
channel = grpc.insecure_channel(address)
stub = lockService_pb2_grpc.LockServiceStub(channel)
stub.stop(lockService_pb2.StopRequest())
print(f"Stop signal sent to {address}")
"@
    $StopScript | & $PythonPath -
}
else {
    Write-Host "Usage: .\lockservice.ps1 start <port> | stop <port>"
}
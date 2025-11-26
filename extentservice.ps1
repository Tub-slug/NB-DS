# Script starting/stopping Extent Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 2001) or address where to connect and stop the service (e.g. 127.0.0.1:2001)
# 3. Root directory where extent will be stored (e.g. C:\extent-root)
#
# Examples how to use the script:
#
# .\extentservice.ps1 start 2001 C:\extent-root
# .\extentservice.ps1 stop 127.0.0.1:2001

# extentservice.ps1

param(
    [string]$Cmd,
    [string]$Port,
    [string]$ExtentRootPath
)


$PythonPath = "python"
$ServiceScript = ".\ExtentServer.py"


if ($Cmd -eq "start") {
    Write-Host "Starting Extent Service on port $Port..."
    $Args = @($ServiceScript, "$Port", "$ExtentRootPath")
    Start-Process -FilePath $PythonPath -ArgumentList $Args `
        -RedirectStandardOutput "extentservice_out.log" `
        -RedirectStandardError "extentservice_err.log" `
        -NoNewWindow
    Write-Host "Extent Service started (check extentservice_out.log for logs)"
}
elseif ($Cmd -eq "stop") {
    Write-Host "Stopping Extent Service on 127.0.0.1:$Port..."
    $StopScript = @"
import grpc
import extentService_pb2
import extentService_pb2_grpc

addr = "127.0.0.1:$Port"
ch = grpc.insecure_channel(addr)
stub = extentService_pb2_grpc.ExtentServiceStub(ch)
stub.stop(extentService_pb2.StopRequest())
print(f'Stop signal sent to {addr}')
"@
    $StopScript | & $PythonPath -
}
else {
    Write-Host "Usage: .\extentservice.ps1 start <port> <extent_root_path> | stop <port>"
}
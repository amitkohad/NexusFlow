param(
    [string]$WorkflowId = "lw-demo-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())",
    [string]$TaskQueue = "lightweight-workflows",
    [string]$SpecFile = "examples/customer_adjustment.json"
)

Write-Host "`n1) Start the process"
temporal workflow start `
  --workflow-id $WorkflowId `
  --type LightweightProcess `
  --task-queue $TaskQueue `
  --input-file $SpecFile

Write-Host "`n2) Query live state"
Start-Sleep -Seconds 3
temporal workflow query --workflow-id $WorkflowId --name status

Write-Host "`n3) Approve the human task"
temporal workflow signal `
  --workflow-id $WorkflowId `
  --name approve `
  --input '{"approved":true,"approver":"ops.manager","comment":"approved in CLI demo"}'

Write-Host "`n4) Wait for final result"
temporal workflow result --workflow-id $WorkflowId

Write-Host "`n5) Show durable event history / audit trail"
temporal workflow show --workflow-id $WorkflowId --detailed

param(
    [Parameter(Mandatory=$true)][string]$InputPath,
    [Parameter(Mandatory=$true)][string]$OutputPath
)

# Offline migration only. Review the generated diff, back up the database and
# upload explicitly; never overwrite a customized template with the default.
$ErrorActionPreference = 'Stop'
$inputFile = (Resolve-Path -LiteralPath $InputPath).Path
$outputFile = [IO.Path]::GetFullPath($OutputPath)
if ($inputFile -eq $outputFile -or (Test-Path -LiteralPath $outputFile)) {
    throw 'Output must be a new file, distinct from the original backup'
}
$sourceFile = Join-Path $PSScriptRoot '..\internal\templates\userdata\github_windows_userdata.tmpl'
$original = [IO.File]::ReadAllText($inputFile)
$source = [IO.File]::ReadAllText($sourceFile)
$helperPattern = '(?ms)^function Start-ExecuteWithRetry \{.*?(?=^function Get-RandomString)'
$oldHelpers = [regex]::Matches($original, $helperPattern)
$newHelpers = [regex]::Matches($source, $helperPattern)
if ($oldHelpers.Count -ne 1 -or $newHelpers.Count -ne 1 -or $original.Contains('function Invoke-GarmWebRequest')) {
    throw 'Unrecognized or already upgraded template; review manually'
}
$updated = $original.Replace($oldHelpers[0].Value, $newHelpers[0].Value)

# Collapse the old registration-token retry block to avoid nested retries.
$registrationPattern = '(?ms)\$GithubRegistrationToken = Start-ExecuteWithRetry -ScriptBlock \{\s*(Invoke-WebRequest -UseBasicParsing[^\r\n]+)\s*\} -MaxRetryCount 5 -RetryInterval 5 -RetryMessage "Retrying download of GitHub registration token\.\.\."'
$registration = [regex]::Matches($updated, $registrationPattern)
if ($registration.Count -ne 1) { throw 'Unrecognized registration-token request' }
$updated = $updated.Replace($registration[0].Value, ('$GithubRegistrationToken = ' + $registration[0].Groups[1].Value))

$requestPattern = '(?:wget|Invoke-WebRequest) -UseBasicParsing(?= -(?:Headers|Method Post))'
$requests = [regex]::Matches($updated, $requestPattern)
if ($requests.Count -ne 7) { throw "Expected seven GARM HTTP calls, found $($requests.Count); review manually" }
$updated = [regex]::Replace($updated, $requestPattern, 'Invoke-GarmWebRequest')
[IO.File]::WriteAllText($outputFile, $updated, [Text.UTF8Encoding]::new($false))
Write-Output 'Generated upgraded template; custom installer behavior outside HTTP calls is preserved'

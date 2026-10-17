<#
.SYNOPSIS
Install the `retp` command line from a GitHub release of geosiris-technologies/retp, on Windows.

.DESCRIPTION
The archive is checked against the release's SHA256SUMS before anything is installed: a
missing entry or a different checksum stops the script, and nothing is written outside a
temporary folder. `retp.exe` goes into a folder of the user
(%LOCALAPPDATA%\Programs\retp by default); the user's PATH is only changed with -AddToPath.

The release archives hold `retp` only. `retp-conformance` comes with
`cargo install retp-conformance`, or the image ghcr.io/geosiris-technologies/retp
(`docker run --rm --entrypoint retp-conformance ...`).

.EXAMPLE
powershell -ExecutionPolicy Bypass -File install-retp.ps1

.EXAMPLE
.\install-retp.ps1 -Version 0.3.0 -AddToPath
#>
[CmdletBinding()]
param(
    # The release to install, X.Y.Z (default: the latest).
    [string] $Version,
    # Where to put retp.exe.
    [string] $InstallDir = (Join-Path $env:LOCALAPPDATA 'Programs\retp'),
    # The processor architecture of the archive (default: detected).
    [ValidateSet('x86_64', 'aarch64')]
    [string] $Arch,
    # Add InstallDir to the user's PATH (never done otherwise).
    [switch] $AddToPath,
    # Replace an existing retp.exe in InstallDir.
    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# Windows PowerShell 5.1 may not offer TLS 1.2 by default; GitHub requires it.
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

$repo = 'geosiris-technologies/retp'

if (-not $Arch) {
    $cpu = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
    switch ($cpu) {
        'AMD64' { $Arch = 'x86_64' }
        'ARM64' { $Arch = 'aarch64' }
        default { throw "No release archive for the architecture ${cpu}: try ``cargo install retp-cli --features mcp``." }
    }
}
$target = "$Arch-pc-windows-msvc"

if (-not $Version) {
    $latest = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/$repo/releases/latest" -Headers @{ 'User-Agent' = 'install-retp.ps1' }
    $Version = $latest.tag_name
}
$Version = $Version -replace '^v', ''

# Refused before downloading anything.
$destination = Join-Path $InstallDir 'retp.exe'
if ((Test-Path -LiteralPath $destination) -and -not $Force) {
    throw "$destination already exists: add -Force to replace it."
}

$archive = "retp-v$Version-$target.zip"
$base = "https://github.com/$repo/releases/download/v$Version"
$work = Join-Path ([IO.Path]::GetTempPath()) ("retp-install-" + [Guid]::NewGuid())
New-Item -ItemType Directory -Path $work | Out-Null
try {
    Write-Host "downloading $archive (release v$Version)"
    Invoke-WebRequest -UseBasicParsing -Uri "$base/$archive" -OutFile (Join-Path $work $archive)
    Invoke-WebRequest -UseBasicParsing -Uri "$base/SHA256SUMS" -OutFile (Join-Path $work 'SHA256SUMS')

    # The line of SHA256SUMS that names this archive exactly ("hash  name" or "hash *name").
    $expected = $null
    foreach ($line in Get-Content -LiteralPath (Join-Path $work 'SHA256SUMS')) {
        $parts = $line.Trim() -split '\s+', 2
        if ($parts.Count -eq 2 -and $parts[1].TrimStart('*') -eq $archive) { $expected = $parts[0].ToLowerInvariant() }
    }
    if (-not $expected) { throw "SHA256SUMS of v$Version does not list $archive`: not installing." }
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $work $archive)).Hash.ToLowerInvariant()
    if ($expected -ne $actual) {
        throw "Checksum mismatch for $archive (expected $expected, got $actual): not installing."
    }
    Write-Host "checksum verified: $actual"

    Expand-Archive -LiteralPath (Join-Path $work $archive) -DestinationPath $work
    $binary = Join-Path $work "retp-v$Version-$target\retp.exe"
    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
        throw "The archive does not hold retp-v$Version-$target\retp.exe."
    }

    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    Copy-Item -LiteralPath $binary -Destination $destination -Force
    Write-Host ("installed " + (& $destination --version) + " into $InstallDir")
}
finally {
    Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
}

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$onPath = ($env:Path -split ';') -contains $InstallDir
if (-not $onPath) {
    if ($AddToPath) {
        $parts = @($userPath -split ';' | Where-Object { $_ })
        if ($parts -notcontains $InstallDir) {
            [Environment]::SetEnvironmentVariable('Path', (($parts + $InstallDir) -join ';'), 'User')
        }
        Write-Host "added $InstallDir to the user's PATH: open a new terminal to use ``retp``"
    }
    else {
        Write-Host "$InstallDir is not on the PATH: call $InstallDir\retp.exe, or rerun with -AddToPath"
    }
}

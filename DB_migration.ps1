<#
.SYNOPSIS
Kickidler PostgreSQL migration helper (source PG 9.3 -> target PG 18).

.DESCRIPTION
This script performs a full migration of one database from an old Kickidler PostgreSQL 9.3
instance to PostgreSQL 18. It is designed to be repeatable and operator-friendly:
- each external command has its own stdout/stderr files
- all steps are timestamped in a main log
- failures throw with step context

Migration flow:
1) Precheck connectivity/auth
2) Dump source DB in custom format
3) Generate restore list (and sanitized list)
4) Export/import globals (roles) unless skipped
5) Recreate target DB with selected encoding/locale
6) Restore pre-data, data, post-data
7) Run optional post-restore sanity checks

.IMPORTANT
The target database is dropped and recreated in Phase 3.

.QUICK FLAGS (SWITCHES)
-AutoInstallPg18   Install PostgreSQL 18 via winget if missing.
-SkipDump          Use existing dump file; do not run pg_dump.
-OnlyGenerateList  Stop after dump + list generation.
-SkipGlobals       Skip globals export/import.
-SkipPrivileges    Restore without GRANT/REVOKE.
-ForceCLocale      Force LC_COLLATE/LC_CTYPE to C on target DB.
-FastRestore       Enable restore speed profile.
-SkipSanityChecks  Skip Phase 5 validation checks.

.PARAMETER SkipDump
Use existing dump file and skip pg_dump step.

.PARAMETER OnlyGenerateList
Stops after creating dump + restore list files; does not restore.

.PARAMETER SkipGlobals
Skips globals export/import (roles and cluster-level grants).

.PARAMETER SkipPrivileges
Adds --no-privileges to pg_restore (skips GRANT/REVOKE replay).

.PARAMETER ForceCLocale
Forces target database locale to LC_COLLATE='C' and LC_CTYPE='C'.

.PARAMETER FastRestore
Opt-in performance mode for restore:
- auto-picks restore jobs when not set explicitly
- sets restore session PGOPTIONS for faster bulk load
- excludes COMMENT objects from restore

.PARAMETER SkipSanityChecks
Skips Phase 5 verification SQL queries after restore.

.PARAMETER AutoInstallPg18
If PG18 binaries are not found, install PostgreSQL 18 automatically via winget.

.PARAMETER Pg18InstallSuperPassword
Superuser password passed to unattended PostgreSQL installer.

.PARAMETER Pg18InstallPort
Server port passed to unattended PostgreSQL installer.

.PARAMETER Pg18InstallLocale
Cluster locale passed to unattended PostgreSQL installer.

.PARAMETER Pg18InstallDataDir
Data directory passed to unattended PostgreSQL installer.
If empty, installer default location is used.

.EXAMPLE
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 -OnlyGenerateList

.EXAMPLE
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 -FastRestore -SkipSanityChecks

.EXAMPLE
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 -AutoInstallPg18

.NOTES
Default paths/ports match common Kickidler setups:
- Source PG 9.3: C:\Program Files\KickidlerNode\pgsql\bin (port 5439)
- Target PG 18:  C:\Program Files\PostgreSQL\18\bin     (port 5433)

Logs:
- Main log:      <WorkDir>\migration_<timestamp>.log
- Transcript:    <WorkDir>\transcript_<timestamp>.txt
- Per-step logs: <WorkDir>\<STEP>_<timestamp>.out.log / .err.log
#>

[CmdletBinding()]
param(
    # ---- Paths / working files ---------------------------------------------------------
    [string]$WorkDir = "C:\migration",

    # ---- Source/target DB names --------------------------------------------------------
    [string]$SourceDbName = "kickidler_node",
    [string]$TargetDbName = "kickidler_node",

    # ---- Source connection (PG 9.3) ----------------------------------------------------
    [string]$SourceHost = "127.0.0.1",
    [int]$SourcePort = 5439,
    [string]$SourceUser = "postgres",

    # ---- Target connection (PG 18) -----------------------------------------------------
    [string]$TargetHost = "127.0.0.1",
    [int]$TargetPort = 5433,
    [string]$TargetUser = "postgres",

    # ---- PostgreSQL bin directories ----------------------------------------------------
    [string]$Pg93Bin = "C:\Program Files\KickidlerNode\pgsql\bin",
    [string]$Pg18Bin = "C:\Program Files\PostgreSQL\18\bin",

    # If PG18 binaries are missing, attempt automatic installation via package manager.
    [switch]$AutoInstallPg18,

    # Installation method used by AutoInstallPg18.
    [ValidateSet('winget')]
    [string]$Pg18InstallMethod = 'winget',

    # Winget package id for PostgreSQL 18.
    [string]$Pg18WingetId = 'PostgreSQL.PostgreSQL.18',

    # Auto-install defaults for PostgreSQL unattended setup.
    [string]$Pg18InstallSuperPassword = 'postgres',
    [int]$Pg18InstallPort = 5433,
    [string]$Pg18InstallLocale = 'en-US',
    # Empty means "use installer default data directory".
    [string]$Pg18InstallDataDir = "",

    # If empty, script creates: <WorkDir>\<SourceDbName>_9.3.dump
    [string]$DumpFile = "",


    # Compression level for pg_dump -F c. 0 disables compression.
    # Use 0 for old PG 9.3 builds without zlib support.
    [ValidateRange(0,9)]
    [int]$DumpCompressionLevel = 0,

    # Use existing dump file and skip pg_dump.
    [switch]$SkipDump = $false,


    # Use a custom pg_restore list file (--use-list).
    # Must be generated from the same dump file.
    [string]$UseListFile = "",

    # Stop after dump/list generation (no target DB changes).
    [switch]$OnlyGenerateList,

    # Skip globals export/import (roles/cluster-level grants).
    [switch]$SkipGlobals,

    # Skip object GRANT/REVOKE during restore.
    [switch]$SkipPrivileges,

    # Use C locale on target DB instead of source locale.
    [switch]$ForceCLocale,

    # Force target DB encoding (for example UTF8). Empty = source encoding.
    [string]$ForceEncoding = "",

    # Application role used for ownership/restore role mapping.
    [string]$AppRole = "kickidler_node",

    # Extra raw pg_restore flags (advanced).
    [string[]]$PgRestoreExtraArgs = @(),

    # Parallel jobs for pg_restore (-j). 0/1 = no parallel mode.
    [int]$RestoreJobs = 0,

    # Opt-in speed profile for restore phase.
    [switch]$FastRestore,

    # Skip post-restore verification SQL checks.
    [switch]$SkipSanityChecks
)


Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Ensure console uses UTF-8 (helps with non-ASCII in logs)
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch {}

# ------------------------------ Helper functions ----------------------------------------

# Create directory if missing.
function Ensure-Directory([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

# Quote one command argument for readable logging.
function Quote-Arg([string]$s) {
    if ($null -eq $s) { return "" }
    if ($s -match '[\s`"]') {
        return '"' + ($s -replace '"','\\"') + '"'
    }
    return $s
}

# Quote PostgreSQL identifier: foo -> "foo", with escaped quotes.
function Quote-Ident([string]$s) {
    if ([string]::IsNullOrEmpty($s)) { throw 'Identifier is empty.' }
    return '"' + ($s -replace '"','""') + '"'
}

# Quote SQL string literal: foo -> 'foo', with escaped apostrophes.
function Quote-Literal([string]$s) {
    if ($null -eq $s) { return 'NULL' }
    return "'" + ($s -replace "'","''") + "'"
}

# Write UTF-8 text without BOM (important for some PostgreSQL tooling inputs).
function Write-TextUtf8NoBom([string]$Path, [string]$Content) {
    $enc = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

# Convert common PostgreSQL boolean outputs to PowerShell bool.
function Test-PgTrue([string]$Value) {
    if ($null -eq $Value) { return $false }
    return ($Value.Trim().ToLower() -in @('t','true','1','yes','y'))
}

# Estimate restore worker count from CPU cores (capped).
function Get-RecommendRestoreJobs([int]$MaxJobs = 8) {
    $cpu = 2
    try { $cpu = [Environment]::ProcessorCount } catch {}
    if ($cpu -le 2) { return 2 }
    $jobs = $cpu - 1
    if ($jobs -gt $MaxJobs) { $jobs = $MaxJobs }
    if ($jobs -lt 2) { $jobs = 2 }
    return $jobs
}

# Build a shell-safe command preview for logs.
function Format-Cmd([string]$Exe, [string[]]$ArgList) {
    $fmtArgs = $ArgList | ForEach-Object { Quote-Arg $_ }
    return "$Exe $($fmtArgs -join ' ')"
}

# Runtime-global paths/names used by logger and step wrapper.
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
Ensure-Directory $WorkDir

$MainLog = Join-Path $WorkDir "migration_${Timestamp}.log"
$TranscriptLog = Join-Path $WorkDir "transcript_${Timestamp}.txt"

if ([string]::IsNullOrWhiteSpace($DumpFile)) {
    $DumpFile = Join-Path $WorkDir "${SourceDbName}_9.3.dump"
}

# Write a timestamped line both to console and main log.
function Write-Log([string]$Message) {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    # Append to main log file (do not emit pipeline output)
    try {
        Add-Content -LiteralPath $MainLog -Value $line -Encoding UTF8
    } catch {
        # Fallback if encoding parameter is not supported (should not happen on PS 5.1+)
        try { $line | Out-File -LiteralPath $MainLog -Append } catch {}
    }
    # Print to console without polluting the pipeline (important for capturing Invoke-External return objects)
    Write-Host $line
}

function Invoke-External {
    param(
        [Parameter(Mandatory=$true)][string]$Step,
        [Parameter(Mandatory=$true)][string]$Exe,
        [Parameter(Mandatory=$true)][string[]]$ArgList,
        [switch]$AllowNonZero
    )

    $stepTs = Get-Date -Format "yyyyMMdd_HHmmss"
    $outFile = Join-Path $WorkDir "${Step}_${stepTs}.out.log"
    $errFile = Join-Path $WorkDir "${Step}_${stepTs}.err.log"

    Write-Log "---- BEGIN $Step ----"
    Write-Log ("CMD: " + (Format-Cmd -Exe $Exe -ArgList $ArgList))
    $oldEap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Exe @ArgList 1> $outFile 2> $errFile
        $exit = $LASTEXITCODE
    } catch {
        # If the process failed to start (rare), write error and mark non-zero exit.
        try { Set-Content -LiteralPath $errFile -Value $_.Exception.Message -Encoding UTF8 } catch {}
        $exit = 1
    } finally {
        $ErrorActionPreference = $oldEap
    }

    Write-Log "STDOUT: $outFile"
    Write-Log "STDERR: $errFile"
    Write-Log "EXIT: $exit"

    if ($exit -ne 0) {
        Write-Log "---- STDERR (tail 80) for $Step ----"
        try {
            $tail = Get-Content -LiteralPath $errFile -Tail 80 -ErrorAction SilentlyContinue
            foreach ($l in $tail) { Write-Log $l }
        } catch {}
        Write-Log "---- STDOUT (tail 80) for $Step ----"
        try {
            $tail2 = Get-Content -LiteralPath $outFile -Tail 80 -ErrorAction SilentlyContinue
            foreach ($l in $tail2) { Write-Log $l }
        } catch {}

        if (-not $AllowNonZero) {
            throw "Step failed: $Step (exit=$exit). Main log: $MainLog | Step logs: $outFile ; $errFile"
        }
    }

    Write-Log "---- END $Step ----"

    return [pscustomobject]@{
        Step = $Step
        Exe = $Exe
        Args = $ArgList
        ExitCode = $exit
        StdOutPath = $outFile
        StdErrPath = $errFile
    }
}

function Assert-FileExists([string]$Path, [string]$What) {
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "$What not found: $Path"
    }
}

# Resolve tool path under bin dir and fail fast if missing.
function Resolve-Bin([string]$BinDir, [string]$ExeName) {
    $p = Join-Path $BinDir $ExeName
    Assert-FileExists -Path $p -What $ExeName
    return $p
}

function Get-CommandPath([string]$Name) {
    try { return (Get-Command $Name -ErrorAction Stop).Source } catch { return $null }
}

function Find-Pg18Bin([string]$PreferredBin, [string]$MajorVersion = '18') {
    # 1) Preferred path from parameter
    if (-not [string]::IsNullOrWhiteSpace($PreferredBin)) {
        $preferredPsql = Join-Path $PreferredBin 'psql.exe'
        if (Test-Path -LiteralPath $preferredPsql) { return $PreferredBin }
    }

    # 2) Standard install location for requested major version
    if (-not [string]::IsNullOrWhiteSpace($env:ProgramFiles)) {
        $exact = Join-Path $env:ProgramFiles "PostgreSQL\$MajorVersion\bin"
        if (Test-Path -LiteralPath (Join-Path $exact 'psql.exe')) { return $exact }
    }

    # 3) Fallback: newest installed PostgreSQL under Program Files
    if (-not [string]::IsNullOrWhiteSpace($env:ProgramFiles)) {
        $root = Join-Path $env:ProgramFiles 'PostgreSQL'
        if (Test-Path -LiteralPath $root) {
            $dirs = Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue |
                Sort-Object -Property Name -Descending
            foreach ($d in $dirs) {
                $bin = Join-Path $d.FullName 'bin'
                if (Test-Path -LiteralPath (Join-Path $bin 'psql.exe')) { return $bin }
            }
        }
    }

    return $null
}

function Read-PsqlScalar {
    param(
        [Parameter(Mandatory=$true)][string]$PsqlExe,
        [Parameter(Mandatory=$true)][string]$HostName,
        [Parameter(Mandatory=$true)][int]$Port,
        [Parameter(Mandatory=$true)][string]$User,
        [Parameter(Mandatory=$true)][string]$Database,
        [Parameter(Mandatory=$true)][string]$Sql,
        [string]$Step = "PSQL_SCALAR"
    )

    # -t (tuples only), -A (unaligned), -F | (field separator)
    $args = @('-h', $HostName, '-p', "$Port", '-U', $User, '-w', '-d', $Database, '-v', 'ON_ERROR_STOP=1', '-t', '-A', '-F', '|', '-c', $Sql)
    $res = Invoke-External -Step $Step -Exe $PsqlExe -ArgList $args
    if ($res -is [array]) { $res = $res[-1] }
    $raw = ""
    try { $raw = (Get-Content -LiteralPath $res.StdOutPath -Raw -ErrorAction SilentlyContinue).Trim() } catch {}
    return $raw
}

function Filter-GlobalsFile([string]$InPath, [string]$OutPath, [string]$DoTag = '$do$') {
    # Filters unsafe or noisy statements when importing globals from 9.3 into 18.
    # Rationale: old GUCs, role settings, and some syntax may fail on modern PG.

    $lines = Get-Content -LiteralPath $InPath -ErrorAction Stop

    $filtered = foreach ($line in $lines) {
        # Drop comments and empty lines
        if ($line -match '^\s*--') { continue }
        if ($line -match '^\s*$') { continue }

        # Drop psql meta-commands (defensive)
        if ($line -match '^\s*\\connect\b') { continue }

        # Drop cluster-level settings
        if ($line -match '^\s*SET\s+') { continue }

        # Drop role config (ALTER ROLE ... SET ...)
        if ($line -match '^\s*ALTER\s+ROLE\s+.*\s+SET\s+') { continue }

        # Make CREATE ROLE/USER idempotent so import doesn't fail when roles already exist
        # pg_dumpall --globals-only typically emits CREATE ROLE/USER on a single line.
        if ($line -match '^\s*CREATE\s+(ROLE|USER)\s+') {
            $m = [regex]::Match($line, '^\s*CREATE\s+(?:ROLE|USER)\s+("([^"]+)"|([^\s;]+))')
            if ($m.Success) {
                $roleName = if (-not [string]::IsNullOrEmpty($m.Groups[2].Value)) { $m.Groups[2].Value } else { $m.Groups[3].Value }
                $roleLit = Quote-Literal $roleName
                # Wrap original CREATE ...; inside a DO block guarded by pg_roles lookup.
                "DO $DoTag BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$roleLit) THEN $line END IF; END $DoTag;"
                continue
            }
        }

        $line
    }

    # Keep encoding explicit and BOM-free for compatibility with PostgreSQL tools.
    Write-TextUtf8NoBom -Path $OutPath -Content ($filtered -join "`r`n")
}

function Ensure-AppRole {
    param(
        [Parameter(Mandatory=$true)][string]$PsqlExe,
        [Parameter(Mandatory=$true)][string]$HostName,
        [Parameter(Mandatory=$true)][int]$Port,
        [Parameter(Mandatory=$true)][string]$User,
        [Parameter(Mandatory=$true)][string]$RoleName
    )

    $roleLit = Quote-Literal $RoleName
    $roleIdent = Quote-Ident $RoleName
    $doTag = '$do$'
    $ensureRoleSql = "DO $doTag BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$roleLit) THEN CREATE ROLE $roleIdent LOGIN; END IF; END $doTag;"
    Invoke-External -Step 'ENSURE_APP_ROLE' -Exe $PsqlExe -ArgList @('-h', $HostName, '-p', "$Port", '-U', $User, '-w', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', $ensureRoleSql) | Out-Null
}

# ------------------------------ Main execution ------------------------------------------
try {
    Start-Transcript -Path $TranscriptLog | Out-Null

    Write-Log "=== Kickidler DB migration v8 ==="
    Write-Log "WorkDir: $WorkDir"
    Write-Log "Source: ${SourceHost}:${SourcePort} db=${SourceDbName} user=${SourceUser}"
    Write-Log "Target: ${TargetHost}:${TargetPort} db=${TargetDbName} user=${TargetUser}"
    Write-Log "Dump:   $DumpFile"
    Write-Log "Main log: $MainLog"
    Write-Log "Transcript: $TranscriptLog"

    # Execution policy hint
    try {
        $ep = Get-ExecutionPolicy -Scope Process
        Write-Log "ExecutionPolicy (Process): $ep"
    } catch {}

    # Resolve required PostgreSQL executables
    $psql93 = Resolve-Bin -BinDir $Pg93Bin -ExeName 'psql.exe'
    $pgdump93 = Resolve-Bin -BinDir $Pg93Bin -ExeName 'pg_dump.exe'
    $pgdumpall93 = Resolve-Bin -BinDir $Pg93Bin -ExeName 'pg_dumpall.exe'

    # Resolve/install PG18 binaries before continuing.
    $resolvedPg18Bin = Find-Pg18Bin -PreferredBin $Pg18Bin -MajorVersion '18'
    if (-not $resolvedPg18Bin) {
        if (-not $AutoInstallPg18) {
            throw "PostgreSQL 18 binaries not found at '$Pg18Bin'. Re-run with -AutoInstallPg18 or set -Pg18Bin to the correct path."
        }

        Write-Log "PG18 binaries not found. AutoInstallPg18 enabled: attempting installation via $Pg18InstallMethod."
        if ($Pg18InstallMethod -eq 'winget') {
            $wingetExe = Get-CommandPath 'winget.exe'
            if ([string]::IsNullOrWhiteSpace($wingetExe)) {
                throw "winget was not found. Install App Installer / winget, or install PostgreSQL 18 manually and set -Pg18Bin."
            }

            # Explicit installer overrides for deterministic unattended PG18 setup.
            # Reference: EDB PostgreSQL installer command line parameters.
            $overrideParts = @(
                '--mode unattended',
                '--unattendedmodeui minimal',
                '--superpassword', (Quote-Arg $Pg18InstallSuperPassword),
                '--serverport', "$Pg18InstallPort",
                '--locale', (Quote-Arg $Pg18InstallLocale),
                '--installer-language en'
            )
            if (-not [string]::IsNullOrWhiteSpace($Pg18InstallDataDir)) {
                $overrideParts += @('--datadir', (Quote-Arg $Pg18InstallDataDir))
            }
            $overrideArgs = ($overrideParts -join ' ')
            $dataDirForLog = '<installer-default>'
            if (-not [string]::IsNullOrWhiteSpace($Pg18InstallDataDir)) { $dataDirForLog = $Pg18InstallDataDir }
            Write-Log "Auto-install PG18 overrides: superuser=postgres, port=$Pg18InstallPort, locale=$Pg18InstallLocale, datadir=$dataDirForLog"

            $installArgs = @(
                'install',
                '--id', $Pg18WingetId,
                '-e',
                '--source', 'winget',
                '--accept-package-agreements',
                '--accept-source-agreements',
                '--disable-interactivity',
                '--silent',
                '--override', $overrideArgs
            )
            Invoke-External -Step 'INSTALL_PG18_WINGET' -Exe $wingetExe -ArgList $installArgs | Out-Null
            Start-Sleep -Seconds 3
        }

        $resolvedPg18Bin = Find-Pg18Bin -PreferredBin $Pg18Bin -MajorVersion '18'
        if (-not $resolvedPg18Bin) {
            throw "PostgreSQL installation step completed, but PG18 binaries are still not detectable. Set -Pg18Bin explicitly and re-run."
        }
    }
    if ($resolvedPg18Bin -ne $Pg18Bin) {
        Write-Log "PG18 bin path auto-detected: $resolvedPg18Bin"
    }
    $Pg18Bin = $resolvedPg18Bin

    $psql18 = Resolve-Bin -BinDir $Pg18Bin -ExeName 'psql.exe'
    $pgrestore18 = Resolve-Bin -BinDir $Pg18Bin -ExeName 'pg_restore.exe'

    Write-Log "=== Precheck connectivity/auth ==="
    Write-Log "Auth note: this script uses -w (never prompt) for psql/pg_dump/pg_restore. Provide credentials via pgpass.conf / PGPASSWORD / PGPASSFILE / service, or ensure host-based auth allows these connections."
    $pgpassPath = $null
    try { if (-not [string]::IsNullOrWhiteSpace($env:APPDATA)) { $pgpassPath = Join-Path $env:APPDATA 'postgresql\pgpass.conf' } } catch {}
    $hasPgpass = ($pgpassPath -and (Test-Path -LiteralPath $pgpassPath))
    $hasPGPASSWORD = (-not [string]::IsNullOrWhiteSpace($env:PGPASSWORD))
    if (-not $hasPgpass -and -not $hasPGPASSWORD) {
        Write-Log "WARNING: No pgpass.conf found and PGPASSWORD is not set. If password auth is required, connectivity/dump/restore steps will fail. Expected pgpass path on Windows: $pgpassPath"
    } else {
        Write-Log "Auth check: PGPASSWORD present=$hasPGPASSWORD ; pgpass.conf present=$hasPgpass ($pgpassPath)"
    }
    Invoke-External -Step 'CHK_PG93' -Exe $psql93 -ArgList @('-h', $SourceHost, '-p', "$SourcePort", '-U', $SourceUser, '-w', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', 'SELECT 1;') | Out-Null
    Invoke-External -Step 'CHK_PG18' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', 'SELECT 1;') | Out-Null

    # Read source DB encoding + locale so target can match unless overridden.
    Write-Log "=== Detect source DB settings (encoding/collation/ctype) ==="
    $sourceDbLit = Quote-Literal $SourceDbName
    $dbInfoSql = "SELECT pg_encoding_to_char(encoding) AS encoding, datcollate, datctype FROM pg_database WHERE datname=$sourceDbLit;"
    $dbInfo = Read-PsqlScalar -PsqlExe $psql93 -HostName $SourceHost -Port $SourcePort -User $SourceUser -Database 'postgres' -Sql $dbInfoSql -Step 'SRC_DB_INFO'

    $srcEncoding = 'UTF8'
    $srcCollate = 'C'
    $srcCtype = 'C'

    if (-not [string]::IsNullOrWhiteSpace($dbInfo) -and ($dbInfo -split '\|').Count -ge 3) {
        $parts = $dbInfo -split '\|'
        $srcEncoding = $parts[0].Trim()
        $srcCollate  = $parts[1].Trim()
        $srcCtype    = $parts[2].Trim()
    }

    if ($ForceCLocale) {
        Write-Log "ForceCLocale enabled: target will use LC_COLLATE='C' LC_CTYPE='C'"
        $tgtCollate = 'C'
        $tgtCtype = 'C'
    } else {
        $tgtCollate = $srcCollate
        $tgtCtype = $srcCtype
    }

    if (-not [string]::IsNullOrWhiteSpace($ForceEncoding)) {
        Write-Log "ForceEncoding enabled: target will use ENCODING='$ForceEncoding' (source was '$srcEncoding')"
        $tgtEncoding = $ForceEncoding
    } else {
        $tgtEncoding = $srcEncoding
    }

    Write-Log "Source DB settings: ENCODING='$srcEncoding' LC_COLLATE='$srcCollate' LC_CTYPE='$srcCtype'"
    Write-Log "Target DB settings: ENCODING='$tgtEncoding' LC_COLLATE='$tgtCollate' LC_CTYPE='$tgtCtype'"

    # Phase 1: produce source dump and restore list artifacts.
    Write-Log "=== Phase 1: Dump source DB (custom format) ==="
    if ($SkipDump) {
        Write-Log "SkipDump enabled: using existing dump file."
        Assert-FileExists -Path $DumpFile -What "DumpFile"
    } else {
        

        $dumpArgs = @('-h', $SourceHost, '-p', "$SourcePort", '-U', $SourceUser, '-w', '-F', 'c', '-Z', "$DumpCompressionLevel", '-f', $DumpFile, $SourceDbName)

        # Run pg_dump; some bundled 9.3 builds may lack zlib, so compression can fail. We retry without compression if needed.
        $dumpRes = Invoke-External -Step 'DUMP_DB' -Exe $pgdump93 -ArgList $dumpArgs -AllowNonZero
        if ($dumpRes -is [array]) { $dumpRes = $dumpRes[-1] }

        if (-not ($dumpRes -and ($dumpRes.PSObject.Properties.Name -contains 'ExitCode'))) {
            throw "Internal error: unexpected result from Invoke-External for DUMP_DB."
        }

        $dumpStderr = ""
        try { $dumpStderr = (Get-Content -LiteralPath $dumpRes.StdErrPath -Raw -ErrorAction SilentlyContinue) } catch {}

        if ($dumpRes.ExitCode -ne 0 -and $dumpStderr -match 'requested compression not available') {
            Write-Log "pg_dump compression is not available in this installation; retrying without compression (-Z 0)."
            $dumpArgs = @('-h', $SourceHost, '-p', "$SourcePort", '-U', $SourceUser, '-w', '-F', 'c', '-Z', '0', '-f', $DumpFile, $SourceDbName)
            $dumpRes = Invoke-External -Step 'DUMP_DB_NOCOMP' -Exe $pgdump93 -ArgList $dumpArgs
            if ($dumpRes -is [array]) { $dumpRes = $dumpRes[-1] }

            $dumpStderr = ""
            try { $dumpStderr = (Get-Content -LiteralPath $dumpRes.StdErrPath -Raw -ErrorAction SilentlyContinue) } catch {}
        }

        if ($dumpRes.ExitCode -ne 0) {
            throw "pg_dump failed (exit=$($dumpRes.ExitCode)). See: $($dumpRes.StdErrPath)"
        }

        # If pg_dump still emitted the known warning, surface it once but continue.
        if ($dumpStderr -match 'requested compression not available') {
            Write-Log "WARNING: pg_dump reported compression is not available; the dump will be uncompressed."
        }
    }

    # Always generate restore list (helps with troubleshooting and selective restore).
    $dumpListFile = Join-Path $WorkDir "dump_list_${Timestamp}.txt"
    Write-Log "=== Generate dump list (for optional selective restore) ==="
    $listRes = Invoke-External -Step 'DUMP_LIST' -Exe $pgrestore18 -ArgList @('--list', $DumpFile)
    if ($listRes -is [array]) { $listRes = $listRes[-1] }
    # pg_restore writes list to stdout; convert it to UTF-8 without BOM (pg_restore --use-list is picky about encoding)
    if ($null -ne $listRes -and (Test-Path -LiteralPath $listRes.StdOutPath)) {
        $listText = ""
        try { $listText = Get-Content -LiteralPath $listRes.StdOutPath -Raw -ErrorAction SilentlyContinue } catch {}
        if (-not [string]::IsNullOrEmpty($listText)) {
            Write-TextUtf8NoBom -Path $dumpListFile -Content $listText
            Write-Log "Dump list saved: $dumpListFile"
        } else {
            Write-Log "WARNING: DUMP_LIST produced empty output: $($listRes.StdOutPath)"
        }
    } else {
        Write-Log "WARNING: Could not locate DUMP_LIST output file: $($listRes.StdOutPath)"
    }

    # Build sanitized restore list to skip objects commonly pre-created in target DB.
    # - public schema is created by templates, so dump's "CREATE SCHEMA public" may fail with "already exists"
    # - plpgsql is usually present by default; restoring its COMMENT can fail due to ownership ("must be owner of extension")
    $AutoRestoreListFile = Join-Path $WorkDir "restore_list_${Timestamp}.txt"
    if (Test-Path -LiteralPath $dumpListFile) {
        try {
            $lines = Get-Content -LiteralPath $dumpListFile -ErrorAction Stop
            $patched = New-Object System.Collections.Generic.List[string]
            foreach ($line in $lines) {
                $l = $line
                if ($l -match '^\s*\d+;\s+\d+\s+\d+\s+SCHEMA\s+-\s+public(\s|$)') { $l = ';' + $l }
                elseif ($l -match '^\s*\d+;\s+\d+\s+\d+\s+COMMENT\s+-\s+SCHEMA\s+public(\s|$)') { $l = ';' + $l }
                elseif ($l -match '^\s*\d+;\s+\d+\s+\d+\s+COMMENT\s+-\s+EXTENSION\s+plpgsql(\s|$)') { $l = ';' + $l }
                $patched.Add($l) | Out-Null
            }
            Write-TextUtf8NoBom -Path $AutoRestoreListFile -Content ($patched -join "`r`n")
            Write-Log "Restore list saved: $AutoRestoreListFile (public schema + plpgsql comment excluded)"
        } catch {
            Write-Log "WARNING: Failed to build sanitized restore list file: $($_.Exception.Message)"
        }
    }

    if ($OnlyGenerateList) {
        Write-Log "OnlyGenerateList enabled: stopping after dump + list generation."
        return
    }

    # Phase 2: migrate global objects (roles/settings), unless explicitly skipped.
    if (-not $SkipGlobals) {
        Write-Log "=== Phase 2: Export globals (best-effort) ==="
        $globalsFile = Join-Path $WorkDir "globals_9.3_${Timestamp}.sql"
        $globalsFiltered = Join-Path $WorkDir "globals_9.3_${Timestamp}.filtered.sql"

        Invoke-External -Step 'DUMP_GLOBALS' -Exe $pgdumpall93 -ArgList @('-h', $SourceHost, '-p', "$SourcePort", '-U', $SourceUser, '-w', '--globals-only', '-f', $globalsFile) | Out-Null
        Filter-GlobalsFile -InPath $globalsFile -OutPath $globalsFiltered
        Write-Log "Globals filtered: $globalsFiltered"

        Write-Log "=== Ensure AppRole exists on PG18 ==="
        Ensure-AppRole -PsqlExe $psql18 -HostName $TargetHost -Port $TargetPort -User $TargetUser -RoleName $AppRole

        Write-Log "=== Import globals to PG18 (filtered) ==="
        Invoke-External -Step 'IMPORT_GLOBALS_PG18' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-f', $globalsFiltered) | Out-Null
    } else {
        Write-Log "SkipGlobals enabled: globals export/import will be skipped."
        Write-Log "Ensuring AppRole exists anyway (minimal) ..."
        Ensure-AppRole -PsqlExe $psql18 -HostName $TargetHost -Port $TargetPort -User $TargetUser -RoleName $AppRole
    }

    # Phase 3: force-disconnect users, drop target DB, create fresh target DB.
    Write-Log "=== Phase 3: Recreate target DB on PG18 ==="
    $targetDbLit = Quote-Literal $TargetDbName
    $terminateSql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$targetDbLit AND pid <> pg_backend_pid();"
    Invoke-External -Step 'TERMINATE_TARGET_CONN' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', $terminateSql) | Out-Null

    $targetDbIdent = Quote-Ident $TargetDbName
    $appRoleIdent2 = Quote-Ident $AppRole
    $dropDbSql = "DROP DATABASE IF EXISTS $targetDbIdent;"
    Invoke-External -Step 'DROP_TARGET_DB' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', $dropDbSql) | Out-Null

    $createDbSql = "CREATE DATABASE $targetDbIdent WITH OWNER=$appRoleIdent2 ENCODING='${tgtEncoding}' LC_COLLATE='${tgtCollate}' LC_CTYPE='${tgtCtype}' TEMPLATE template0;"
    Invoke-External -Step 'CREATE_TARGET_DB' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', $createDbSql) | Out-Null


    # Ensure the built-in public schema is owned by the application role.
    # On some PG installations/template0, public schema may be owned by postgres; this can later cause restore ACL/COMMENT issues.
    $fixPublicSql = "ALTER SCHEMA public OWNER TO $appRoleIdent2; GRANT USAGE,CREATE ON SCHEMA public TO $appRoleIdent2;"
    Invoke-External -Step 'FIX_PUBLIC_SCHEMA_OWNER' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', $TargetDbName, '-v', 'ON_ERROR_STOP=1', '-c', $fixPublicSql) -AllowNonZero | Out-Null

    # Phase 4: restore schema/data/indexes in controlled sections.
    Write-Log "=== Phase 4: Restore dump to PG18 (sectioned) ==="

    $effectiveRestoreJobs = $RestoreJobs
    if ($FastRestore -and $effectiveRestoreJobs -le 1) {
        $effectiveRestoreJobs = Get-RecommendRestoreJobs
        Write-Log "FastRestore enabled: auto-selected RestoreJobs=$effectiveRestoreJobs based on CPU count."
    }

    $commonRestoreArgs = @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', $TargetDbName, '--no-owner', '--role', $AppRole)

    if ($SkipPrivileges) {
        Write-Log "SkipPrivileges enabled: adding --no-privileges to pg_restore."
        $commonRestoreArgs += @('--no-privileges')
    }

    if ($effectiveRestoreJobs -gt 1) {
        Write-Log "RestoreJobs=${effectiveRestoreJobs}: enabling parallel pg_restore (higher CPU/IO load on target)."
        $commonRestoreArgs += @('--jobs', "$effectiveRestoreJobs")
    }

    if ($FastRestore) {
        # Comments can be numerous and are typically non-essential for application runtime.
        $commonRestoreArgs += @('--no-comments')
    }

    # If user didn't provide a custom list file, use the auto-generated sanitized list (helps avoid benign restore conflicts).
    if ([string]::IsNullOrWhiteSpace($UseListFile) -and $null -ne $AutoRestoreListFile -and (Test-Path -LiteralPath $AutoRestoreListFile)) {
        $UseListFile = $AutoRestoreListFile
    }

    if (-not [string]::IsNullOrWhiteSpace($UseListFile)) {
        Assert-FileExists -Path $UseListFile -What 'UseListFile'
        Write-Log "Using restore list file: $UseListFile"
        $commonRestoreArgs += @('--use-list', $UseListFile)
    }

    if ($PgRestoreExtraArgs.Count -gt 0) {
        Write-Log "Extra pg_restore args: $($PgRestoreExtraArgs -join ' ')"
        $commonRestoreArgs += $PgRestoreExtraArgs
    }

    $oldPgOptions = $env:PGOPTIONS
    try {
        if ($FastRestore) {
            # Session-level tuning for faster bulk restore; affects only commands in this process while set.
            $env:PGOPTIONS = '-c synchronous_commit=off -c maintenance_work_mem=512MB'
            Write-Log "FastRestore enabled: PGOPTIONS set for restore sessions (synchronous_commit=off, maintenance_work_mem=512MB)."
        }

        # Pre-data (schema, types, functions, etc.)
        Invoke-External -Step 'RESTORE_PRE_DATA' -Exe $pgrestore18 -ArgList ($commonRestoreArgs + @('--section=pre-data', '--verbose', $DumpFile)) | Out-Null

        # Data
        Invoke-External -Step 'RESTORE_DATA' -Exe $pgrestore18 -ArgList ($commonRestoreArgs + @('--section=data', '--disable-triggers', '--verbose', $DumpFile)) | Out-Null

        # Post-data (indexes, constraints, triggers, etc.)
        Invoke-External -Step 'RESTORE_POST_DATA' -Exe $pgrestore18 -ArgList ($commonRestoreArgs + @('--section=post-data', '--verbose', $DumpFile)) | Out-Null
    } finally {
        $env:PGOPTIONS = $oldPgOptions
    }

    # Phase 5: optional verification queries.
    if ($SkipSanityChecks) {
        Write-Log "SkipSanityChecks enabled: skipping Phase 5 post-restore checks."
    } else {
        Write-Log "=== Phase 5: Post-restore sanity checks ==="

        $countTablesSql = "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');"
        Invoke-External -Step 'CHECK_TABLE_COUNT' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', $TargetDbName, '-v', 'ON_ERROR_STOP=1', '-c', $countTablesSql) | Out-Null

        # Users table name differs across Kickidler DB versions. Do not fail migration if the expected table is absent.
        $userTableCandidates = @(
            'public.users',
            'central.user_account',
            'central.user_accounts',
            'central.user',
            'public.user_account'
        )

        $usersTable = $null
        foreach ($tbl in $userTableCandidates) {
            $stepSuffix = ($tbl -replace '[^A-Za-z0-9]+','_')
            $existsSql = "SELECT to_regclass('$tbl') IS NOT NULL;"
            $existsRaw = Read-PsqlScalar -PsqlExe $psql18 -HostName $TargetHost -Port $TargetPort -User $TargetUser -Database $TargetDbName -Sql $existsSql -Step ("CHK_USERS_TABLE_" + $stepSuffix)
            if (Test-PgTrue $existsRaw) { $usersTable = $tbl; break }
        }

        if ($usersTable) {
            Write-Log "User table detected for sanity check: $usersTable"
            $countUsersSql = "SELECT count(*) AS users FROM $usersTable;"
            Invoke-External -Step 'CHECK_USERS_COUNT' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', $TargetDbName, '-v', 'ON_ERROR_STOP=1', '-c', $countUsersSql) -AllowNonZero | Out-Null
        } else {
            Write-Log ("WARNING: Users table not found (checked: {0}). Skipping user count check." -f ($userTableCandidates -join ', '))
            $hintSql = "SELECT n.nspname AS schema, c.relname AS table FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND c.relname ILIKE '%user%' ORDER BY 1,2 LIMIT 20;"
            Invoke-External -Step 'CHECK_USER_TABLE_HINTS' -Exe $psql18 -ArgList @('-h', $TargetHost, '-p', "$TargetPort", '-U', $TargetUser, '-w', '-d', $TargetDbName, '-v', 'ON_ERROR_STOP=1', '-c', $hintSql) -AllowNonZero | Out-Null
        }
    }

    Write-Log "=== DONE ==="
    Write-Log "If Kickidler services are still pointing to PG 9.3, update their connection settings and restart services."
    Write-Log "Artifacts:"
    Write-Log "  Dump:        $DumpFile"
    Write-Log "  Dump list:   $dumpListFile"
    if (-not $SkipGlobals) {
        Write-Log "  Globals:     $(Join-Path $WorkDir "globals_9.3_${Timestamp}.sql")"
    }
    Write-Log "  Main log:    $MainLog"
    Write-Log "  Transcript:  $TranscriptLog"

} catch {
    Write-Log "ERROR: $($_.Exception.Message)"
    Write-Log "Hint: open the per-step *.err.log / *.out.log files in WorkDir; they contain the full output." 
    throw
} finally {
    try { Stop-Transcript | Out-Null } catch {}
}
    # Runs one external process and writes stdout/stderr to dedicated files.
    # On non-zero exit:
    # - log tails from stderr/stdout
    # - throw unless AllowNonZero is set

    # Execute SQL and return single raw scalar text (trimmed).

    # Create role if missing; idempotent and safe to rerun.

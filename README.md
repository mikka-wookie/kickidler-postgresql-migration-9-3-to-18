# Kickidler PostgreSQL Migration (9.3 -> 18)

PowerShell migration utility for moving a Kickidler database from bundled PostgreSQL 9.3 to PostgreSQL 18 with logging, prechecks, optional auto-install, and sectioned restore.

## What This Script Does

`DB_migration.ps1` performs:

1. Connectivity/auth prechecks for source and target PostgreSQL.
2. Source database dump (`pg_dump`, custom format).
3. Restore list generation and auto-sanitized list creation.
4. Optional globals export/import (roles/cluster-level objects).
5. Target database recreation (drop + create).
6. Sectioned restore (`pre-data`, `data`, `post-data`).
7. Optional post-restore sanity checks.

## Important Safety Note

The script **drops and recreates** the target database in Phase 3.

```sql
DROP DATABASE IF EXISTS "<target_db>";
```

Do not run against a target DB you are not ready to replace.

## Defaults

- Source PG 9.3 binaries: `C:\Program Files\KickidlerNode\pgsql\bin`
- Source port: `5439`
- Target PG 18 binaries: `C:\Program Files\PostgreSQL\18\bin`
- Target port: `5433`
- Work directory: `C:\migration`
- Source/target DB name: `kickidler_node`

## Requirements

- Windows + PowerShell
- Source PostgreSQL 9.3 client tools available:
  - `psql.exe`
  - `pg_dump.exe`
  - `pg_dumpall.exe`
- Target PostgreSQL 18 tools available, or use `-AutoInstallPg18`
- Network/auth access to both clusters
- Credentials supplied non-interactively (script uses `-w`):
  - `%APPDATA%\postgresql\pgpass.conf`, or
  - `PGPASSWORD` / `PGPASSFILE`, or
  - host-based auth rules allowing connection

## Quick Start

### 1) Preflight only (safe)

Generates dump/list artifacts and stops before restore:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 -OnlyGenerateList
```

### 2) Full migration

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1
```

### 3) Auto-install PG18 if missing

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 -AutoInstallPg18
```

### 4) Faster restore profile

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 -FastRestore -SkipSanityChecks
```

## Auto-Install (PG18) Behavior

When `-AutoInstallPg18` is set and PG18 binaries are missing, script installs via `winget` using non-interactive source mode and explicit installer overrides.

Defaults for unattended install:

- superuser password: `postgres`
- server port: `5433`
- locale: `en-US`
- data directory: installer default (unless overridden)

Related parameters:

- `-Pg18InstallMethod` (`winget`)
- `-Pg18WingetId` (default `PostgreSQL.PostgreSQL.18`)
- `-Pg18InstallSuperPassword`
- `-Pg18InstallPort`
- `-Pg18InstallLocale`
- `-Pg18InstallDataDir`

Example with custom data directory:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 -AutoInstallPg18 -Pg18InstallDataDir "C:\Program Files\PostgreSQL\18\data"
```

## Switch Flags

- `-AutoInstallPg18` install PostgreSQL 18 automatically if missing.
- `-SkipDump` use existing dump file.
- `-OnlyGenerateList` stop after dump/list generation.
- `-SkipGlobals` skip globals export/import.
- `-SkipPrivileges` restore without GRANT/REVOKE replay.
- `-ForceCLocale` force target DB locale to `C`.
- `-FastRestore` enable restore speed profile.
- `-SkipSanityChecks` skip post-restore checks.

## Key Parameters

- `-WorkDir` output folder for logs/artifacts.
- `-DumpFile` existing/new dump path.
- `-SourceHost/-SourcePort/-SourceUser/-SourceDbName`
- `-TargetHost/-TargetPort/-TargetUser/-TargetDbName`
- `-Pg93Bin/-Pg18Bin` PostgreSQL binaries paths.
- `-AppRole` role used for ownership/restore role mapping.
- `-RestoreJobs` manual parallel jobs for `pg_restore`.
- `-UseListFile` custom `pg_restore --use-list` file.
- `-ForceEncoding` override target DB encoding.
- `-PgRestoreExtraArgs` extra raw `pg_restore` arguments.

## Migration Phases (Detailed)

### Phase 0: Precheck

- Resolves required binaries.
- Verifies source and target connectivity (`SELECT 1`).
- Warns if no non-interactive credential source is found.

### Phase 1: Dump + List

- Creates custom-format dump (`pg_dump -F c`).
- Handles legacy compression incompatibility by retrying with `-Z 0`.
- Generates dump list (`pg_restore --list`).
- Creates sanitized restore list excluding common pre-existing conflicts (`public` schema, `plpgsql` comment).

### Phase 2: Globals (optional)

- Exports globals (`pg_dumpall --globals-only`).
- Filters unsafe legacy statements for PG18 compatibility.
- Ensures app role exists on target.
- Imports filtered globals.

### Phase 3: Recreate Target DB

- Terminates active connections to target DB.
- Drops target DB.
- Recreates target DB with detected/forced encoding and locale.
- Tries to fix `public` schema ownership/grants.

### Phase 4: Restore

- Uses sectioned restore:
  - `--section=pre-data`
  - `--section=data`
  - `--section=post-data`
- Uses `--no-owner` + `--role <AppRole>`.
- Optional:
  - `--no-privileges` (`-SkipPrivileges`)
  - `--jobs N` (`-RestoreJobs` or `-FastRestore` auto-tune)
  - `--use-list <file>`

### Phase 5: Sanity Checks (optional)

- Counts non-system tables.
- Attempts user-table detection from known candidates and counts rows.
- Can be disabled with `-SkipSanityChecks`.

## Logs and Artifacts

Written under `WorkDir`:

- Main log: `migration_<timestamp>.log`
- Transcript: `transcript_<timestamp>.txt`
- Per-step stdout/stderr:
  - `<STEP>_<timestamp>.out.log`
  - `<STEP>_<timestamp>.err.log`
- Dump and list files:
  - `<SourceDbName>_9.3.dump`
  - `dump_list_<timestamp>.txt`
  - `restore_list_<timestamp>.txt`
- Globals:
  - `globals_9.3_<timestamp>.sql`
  - `globals_9.3_<timestamp>.filtered.sql`

## Troubleshooting

If a step fails:

1. Open `migration_<timestamp>.log`.
2. Open matching `*.err.log` and `*.out.log` for the failed step.
3. Common fixes:
   - auth errors: configure `pgpass.conf` / `PGPASSWORD`
   - ownership/privilege errors: try `-SkipPrivileges`
   - role issues: avoid `-SkipGlobals`, verify `-AppRole`
   - object conflicts: use/edit restore list and pass `-UseListFile`

## Example: Fully Explicit Run

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\DB_migration.ps1 `
  -WorkDir "C:\migration" `
  -SourceHost "127.0.0.1" -SourcePort 5439 -SourceUser "postgres" -SourceDbName "kickidler_node" `
  -TargetHost "127.0.0.1" -TargetPort 5433 -TargetUser "postgres" -TargetDbName "kickidler_node" `
  -Pg93Bin "C:\Program Files\KickidlerNode\pgsql\bin" `
  -Pg18Bin "C:\Program Files\PostgreSQL\18\bin" `
  -AppRole "kickidler_node"
```

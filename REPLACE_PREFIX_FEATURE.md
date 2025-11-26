# Replace Prefix Feature Documentation

## Overview

The `replace_prefix` feature allows S3 Grabber to perform selective file replacement based on filename prefixes. This enables partial updates where only specific files are replaced while preserving others in the target directory.

## Use Cases

- **Monitoring Configuration**: Update monitoring-related files (`monitoring.*`) without affecting application files (`svc-x.*`)
- **Application-Specific Updates**: Update application files (`svc-x.*`) while keeping monitoring configuration intact
- **Selective Deployments**: Deploy configuration updates for specific services without touching unrelated files

## Configuration

Add the optional `replace_prefix` field to your grabber configuration:

```yaml
grabbers:
  monitoring_config:
    shell: "/bin/sh"
    timeout: 5s
    buckets:
      - lithuania
    file: "monitoring.tar.gz"
    path: "/etc/monitoring"
    replace_prefix: "monitoring."  # Only removes files starting with "monitoring."
    commands:
      - "systemctl reload monitoring"
```

## Behavior

### Without `replace_prefix` (Default Behavior)
- **All files** in the target directory are removed before extraction
- Complete replacement of directory contents
- Backward compatible with existing configurations

### With `replace_prefix`
- **Only files matching the prefix** are removed before extraction
- Files not matching the prefix are preserved
- New files from the archive are extracted alongside preserved files

## Examples

### Example 1: Monitoring Archive Extraction

**Before extraction:**
```
/etc/monitoring/
  ├── monitoring.yml      (old)
  ├── monitoring.conf     (old)
  ├── svc-x.yml
  └── other.txt
```

**Archive contains:**
```
monitoring.tar.gz:
  ├── monitoring.yml      (new)
  └── monitoring.new.yml  (new)
```

**Configuration:**
```yaml
file: "monitoring.tar.gz"
path: "/etc/monitoring"
replace_prefix: "monitoring."
```

**After extraction:**
```
/etc/monitoring/
  ├── monitoring.yml      (new - replaced)
  ├── monitoring.new.yml  (new - added)
  ├── svc-x.yml            (preserved)
  └── other.txt           (preserved)
```

### Example 2: Directory Extraction

**Configuration:**
```yaml
dir: "svc-x/"
path: "/etc/svc-x"
replace_prefix: "svc-x."
```

**Behavior:**
- Downloads all files from `svc-x/` prefix in S3
- Removes only files starting with `svc-x.` from `/etc/svc-x`
- Preserves all other files (e.g., `monitoring.yml`, `config.txt`)

### Example 3: Full Replacement (Backward Compatible)

**Configuration:**
```yaml
file: "app.tar.gz"
path: "/etc/app"
# No replace_prefix specified
```

**Behavior:**
- Removes **all files** from `/etc/app`
- Extracts all files from archive (same as before)

## Notes

- Prefix matching uses Go's `filepath.HasPrefix()` function
- Prefix matching is **exact** - `monitoring.` will match `monitoring.yml` but not `monitoringxyz.yml`
- Empty prefix (`""`) means all files are removed (default behavior)
- The feature works identically for both archive (`file:`) and directory (`dir:`) extraction modes


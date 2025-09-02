# OP-ETL Configuration Migration Guide

This document provides guidance for migrating from the legacy split configuration format to the new unified configuration system.

## Overview

The OP-ETL system has been migrated from a complex two-file configuration system to a simpler, single-file approach:

- **Before:** `config.yaml` + `sources.yaml` (split configuration, 835+ lines)
- **After:** `config.yaml` (unified configuration, cleaner format)

## Migration Status

✅ **Migration Complete** - The system is now using the unified configuration format.

## Key Changes

### 1. Single Configuration File
```yaml
# Before: Two separate files
config.yaml       # Global settings
sources.yaml      # Source definitions

# After: One unified file  
config.yaml       # Everything in one place
```

### 2. Explicit Geometry Types
```yaml
# Before: Auto-detection (problematic)
sources:
  - name: "example"
    type: "ogc"
    # geometry type was auto-detected

# After: Explicit declaration (reliable)
sources:
  - name: "example" 
    type: "ogc"
    geometry: "POLYGON"  # Required for OGC sources
```

### 3. Simplified Structure
```yaml
# New unified format
workspace:
  downloads: "./data/downloads"
  staging_gdb: "./data/staging.gdb"
  sde_connection: "./data/connections/prod.sde"

processing:
  aoi_bbox: [610000, 6550000, 700000, 6650000]
  aoi_bbox_wkid: 3006
  target_wkid: 3010

sources:
  - name: "source_name"
    type: "file|ogc"
    authority: "AUTHORITY"
    enabled: true
    # ... source-specific settings
```

## Files Organization

```
config/
├── config.yaml              # 🟢 ACTIVE - Current unified configuration
├── README.md                # Documentation and status
├── migrate_config.py        # Migration script (reference)
├── MIGRATION.md            # This guide
└── legacy/                 # Legacy files (cleanup: 2024-04-01)
    ├── config.yaml         # Original global settings
    └── sources.yaml        # Original source definitions
```

## Migration Script Usage

For reference, the migration script can be used to convert legacy configurations:

```bash
# From the config/ directory
python migrate_config.py --legacy-dir legacy/ --output config_migrated.yaml
```

**Note:** The current system already uses the migrated format, so this script is primarily for reference or future migrations.

## Validation

The new configuration system includes comprehensive validation:

- **Required fields** - All necessary fields must be present
- **Type checking** - Ensures correct data types
- **Geometry validation** - Explicit geometry types for OGC sources
- **Clear error messages** - Actionable feedback when validation fails

## Benefits of the New System

1. **Simpler onboarding** - Single file to understand
2. **No auto-detection** - Explicit configuration prevents surprises  
3. **Better validation** - Comprehensive error checking
4. **Easier maintenance** - Single source of truth
5. **Consultant-friendly** - Clear, explicit configuration

## Legacy Support

Legacy configuration files are preserved in `config/legacy/` for:
- Reference during transition period
- Backup in case of issues
- Historical documentation

**Cleanup Date:** Legacy files will be removed on **2024-04-01**

## Troubleshooting

### Common Issues

1. **Missing geometry type for OGC sources**
   ```yaml
   # ❌ Old way (auto-detection)
   - type: "ogc"
     
   # ✅ New way (explicit)  
   - type: "ogc"
     geometry: "POLYGON"
   ```

2. **Configuration validation errors**
   - Check that all required fields are present
   - Verify data types match expected formats
   - Ensure geometry types are specified for OGC sources

3. **File path issues**
   - Ensure `config.yaml` is in the expected location
   - Check that workspace paths are correct

### Getting Help

If you encounter issues with the new configuration:

1. Check the validation error messages - they provide specific guidance
2. Compare with the working `config.yaml` examples
3. Refer to the legacy files in `config/legacy/` for reference
4. Run the baseline capture system to validate pipeline behavior

## Future Improvements

The unified configuration system enables:
- Better IDE support with schema validation
- Automated configuration testing
- Easier configuration templating
- Simplified documentation

---

*This migration is part of the larger OP-ETL refactoring initiative to reduce complexity from 4,484 lines to ~1,000 lines while maintaining full functionality.*
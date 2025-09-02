# Configuration Files

This directory contains the configuration files for the OP-ETL pipeline.

## Current Status

**During Refactoring Phase 1:**

- `config.yaml` + `sources.yaml` - **Legacy configuration** (split format) - Used by current system
- `config_new.yaml` - **New unified configuration** (single file format) - Target for refactored system

## Migration

The new configuration system replaces the split configuration (config.yaml + sources.yaml) with a single, explicit configuration file that includes:

- Workspace settings
- Processing parameters  
- Complete source definitions with explicit geometry types

## File Descriptions

### Legacy Files (Phase 1 - Keep for backward compatibility)
- `config.yaml` - Global pipeline settings (191 lines)
- `sources.yaml` - Source definitions (644 lines)

### New Format (Phase 1 - Target system)
- `config_new.yaml` - Single unified configuration (366 lines) containing all settings and source definitions

## Next Steps

1. **Phase 1**: Validate new configuration system works with refactored components
2. **Phase 2-4**: Gradually migrate system components to use new configuration
3. **Phase 5**: Remove legacy configuration files once migration is complete

The goal is a single, clear configuration file that eliminates the need for split configurations and makes the system easier to understand and maintain.
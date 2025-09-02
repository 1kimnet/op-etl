# Configuration Files

This directory contains the configuration files for the OP-ETL pipeline.

## 🟢 Active Configuration

**Currently Used:**
- `config.yaml` - **ACTIVE** unified configuration file (single file format)

This is the **only** configuration file currently used by the system.

## 📁 Legacy Files (backup/reference)

- `legacy/config.yaml` - Original global pipeline settings (191 lines)
- `legacy/sources.yaml` - Original source definitions (644 lines)

**Cleanup Schedule:** Legacy files will be removed on **2024-04-01** once migration is fully validated.

## Configuration Format

The current configuration system uses a single, explicit configuration file that includes:

- Workspace settings
- Processing parameters  
- Complete source definitions with explicit geometry types (eliminates auto-detection)
- Comprehensive validation with clear error messages

## Migration Complete

The new unified configuration replaces the previous split configuration system (config.yaml + sources.yaml) with a cleaner, single-file approach that is:

- **Explicit over implicit** - No guessing or auto-detection
- **Consultant-friendly** - Easy to understand and modify
- **Type-safe** - Comprehensive validation prevents configuration errors
- **Maintainable** - Single source of truth for all pipeline configuration

Legacy files are preserved in the `legacy/` folder for reference during the transition period but are no longer actively used by the system.
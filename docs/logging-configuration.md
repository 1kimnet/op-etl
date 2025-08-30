# Logging Configuration

The OP-ETL pipeline now properly respects logging configuration settings from `config.yaml`.

## Configuration Options

### Basic Logging Levels

Set the logging level in your `config.yaml`:

```yaml
logging:
  level: "WARNING"          # File logging level
  console_level: "WARNING"  # Console logging level  
  format: "%(asctime)s - %(levelname)s - %(message)s"
```

### Available Log Levels

- `DEBUG`: Most verbose, shows all messages
- `INFO`: General information messages (default)
- `WARNING`: Warning messages and above
- `ERROR`: Error messages and above
- `CRITICAL`: Only critical messages

### Examples

**To see only warnings and errors** (as requested in the issue):
```yaml
logging:
  level: "WARNING"
  console_level: "WARNING"
```

**To log everything to file but only errors to console**:
```yaml
logging:
  level: "DEBUG"
  console_level: "ERROR"
```

**To use the default INFO level**:
```yaml
logging:
  level: "INFO"
  console_level: "INFO"
```

## What Changed

Previously, the `run.py` file hardcoded the logging level to `INFO`, ignoring any settings in `config.yaml`. Now:

1. **Configuration is loaded first**, then logging is configured
2. **All modules respect** the global logging configuration
3. **File and console levels** can be set independently
4. **Case-insensitive** level names are supported (`"warning"` or `"WARNING"`)

## Backward Compatibility

- Existing code continues to work without changes
- If no logging configuration is provided, defaults to `INFO` level
- Individual modules can still create their own loggers, but they will respect the global level settings

## Testing Your Configuration

After setting your logging level in `config.yaml`, run the pipeline and verify:

- With `level: "WARNING"`: You should only see WARNING and ERROR messages
- With `level: "INFO"`: You should see INFO, WARNING, and ERROR messages  
- With `level: "DEBUG"`: You should see all messages including DEBUG

The logging level is applied immediately when the pipeline starts and affects all subsequent log messages throughout the ETL process.
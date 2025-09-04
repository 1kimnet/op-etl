# Enhanced Staging Demonstration

This document demonstrates the improvements made to GeoJSON geometry type handling in the simplified staging module.

## Problem Solved

**Before**: GeoJSON files with mixed geometry types would cause unpredictable results in ArcPy's `JSONToFeatures` tool, leading to incorrect feature class geometry types in the staging geodatabase.

**After**: Enhanced staging detects geometry types, filters to dominant type, and uses explicit geometry type parameters for reliable feature class creation.

## Example GeoJSON Processing

### Input: Mixed Geometry GeoJSON

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [18.0686, 59.3293]},
      "properties": {"name": "Stockholm City Hall"}
    },
    {
      "type": "Feature", 
      "geometry": {"type": "Point", "coordinates": [18.0896, 59.3366]},
      "properties": {"name": "Stockholm Central"}
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon", 
        "coordinates": [[[18.06, 59.32], [18.09, 59.32], [18.09, 59.34], [18.06, 59.34], [18.06, 59.32]]]
      },
      "properties": {"name": "District Boundary"}
    },
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [18.0756, 59.3348]},
      "properties": {"name": "Stockholm Palace"}
    }
  ]
}
```

### Enhanced Staging Processing

#### Step 1: Geometry Type Analysis
```
[STAGE] Geometry types in mixed_data.geojson: {'Point': 3, 'Polygon': 1}
[STAGE] Dominant geometry type: Point
```

#### Step 2: Mixed Type Detection & Filtering  
```
[STAGE] Mixed geometry types detected, filtering to Point
[STAGE] Filtered 4 -> 3 features (keeping 'Point')
```

#### Step 3: Reliable Import
```
[STAGE] Converting mixed_data.geojson -> authority_mixed_data (geometry type: POINT)
[STAGE] Successfully imported 3 features to authority_mixed_data
```

### Result

- ✅ **Correct geometry type**: Point feature class created in staging GDB
- ✅ **Consistent data**: Only Point features included (3 features)
- ✅ **Proper projection**: All coordinates projected to SWEREF99 TM (EPSG:3006)
- ✅ **Predictable naming**: Safe feature class name with authority prefix

## Configuration

Enable enhanced staging in your `config.yaml`:

```yaml
# Use enhanced staging module
use_simplified_staging: true

workspaces:
  downloads: ./data/downloads
  staging_gdb: ./data/staging.gdb

# Optional: cleanup staging before each run
cleanup_staging_before_run: true
```

## Benefits

1. **Reliability**: No more unpredictable geometry types from mixed GeoJSON files
2. **Consistency**: Always uses dominant geometry type for reliable ArcPy processing  
3. **Transparency**: Clear logging shows exactly what filtering decisions were made
4. **Efficiency**: Filters data before ArcPy processing, reducing memory usage
5. **Maintainability**: Simpler codebase (427 LOC vs 725 LOC in full staging)

## Supported Formats

The enhanced staging handles all standard formats:

- **GeoJSON**: Mixed geometry filtering + explicit type specification
- **GPKG**: Layer discovery and projection to SWEREF99 TM
- **Shapefile**: Spatial reference detection and projection 
- **ZIP archives**: Automatic extraction and processing of contents
- **Esri JSON**: Direct import with spatial reference handling

## Error Handling

Enhanced staging includes robust error handling:

- Invalid GeoJSON format detection
- Coordinate magnitude validation (Swedish bounds checking)
- Spatial reference detection and fallback to WGS84
- Temporary file cleanup on success or failure
- Clear error messages for debugging

This ensures the staging process is both reliable and debuggable in production environments.
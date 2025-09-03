### **OP-ETL Refactoring: Actionable Task List**

This breaks down your 5-week plan into a series of manageable tasks.

### **Phase 1: Foundation & Planning (Week 1\)**

*Objective: Prepare for the refactoring by establishing a baseline, designing the new configuration, and setting up logging.*

* **Task 1.1: Establish Baseline**
  * **Objective:** Quantify the current performance and output to validate the new system against it.
  * **Key Steps:**
    1. Select 5 representative data sources from sources.yaml (1 of each type: REST, OGC, HTTP file, etc.).
    2. Run the *current* full pipeline for only these sources.
    3. Record the execution time for each source.
    4. For each source, count the features in the final staged feature class (arcpy.GetCount\_management).
    5. Note the final geometry type and spatial reference.
  * **Deliverable:** A baseline\_report.md file with the recorded metrics for the 5 test sources.
* **Task 1.2: Design & Implement New Configuration**
  * **Objective:** Create a single, simple, and explicit configuration schema.
  * **Key Steps:**
    1. Create a new etl/config\_v2.py module.
    2. Implement dataclasses for a new PipelineConfig that includes Workspace, Processing, and Source sections.
    3. The Source class must require name, type, url, and authority. For ogc and wfs types, it must also require a geometry field (POINT, POLYLINE, POLYGON).
    4. Add validation logic within the dataclasses to ensure paths exist and required fields are present.
    5. Create a single config/config\_v2.yaml file based on the new schema for the 5 test sources.
  * **Deliverable:** A new etl/config\_v2.py module and a config/config\_v2.yaml file.
* **Task 1.3: Implement Simple Logging**
  * **Objective:** Create a single, unified logging setup.
  * **Key Steps:**
    1. Create etl/logging.py.
    2. Write a setup\_logging() function that configures a root logger for console output.
    3. The format should be simple and consistent (e.g., TIME | LEVEL | MESSAGE).
    4. Remove all other logging.basicConfig calls from the entire project.
  * **Deliverable:** A single etl/logging.py module.

### **Phase 2: Core Infrastructure (Week 2\)**

*Objective: Build the new, simplified scaffolding for the pipeline orchestrator and HTTP client.*

* **Task 2.1: Implement Simple HTTP Client**
  * **Objective:** Replace the complex http\_utils.py with a simple, reliable requests-based client.
  * **Key Steps:**
    1. Create etl/http\_client.py.
    2. Create a class SimpleHttpClient that uses requests.Session.
    3. Implement basic retry logic using urllib3.util.retry.Retry.
    4. Provide three methods: get\_json(), get\_text(), and download\_file().
    5. Delete the old etl/http\_utils.py file.
  * **Deliverable:** A new etl/http\_client.py module.
* **Task 2.2: Implement Pipeline Orchestrator**
  * **Objective:** Create the main run.py replacement that manages the ETL flow.
  * **Key Steps:**
    1. Create etl/pipeline.py.
    2. Create an Orchestrator class that takes the new PipelineConfig as input.
    3. Implement a main run() method.
    4. Inside run(), create placeholder methods for the three phases: \_run\_download\_phase(), \_run\_staging\_phase(), \_run\_load\_phase().
    5. Ensure errors in one source are caught and logged, allowing the pipeline to continue with other sources.
  * **Deliverable:** A new etl/pipeline.py module.

### **Phase 3: Download Consolidation (Week 3\)**

*Objective: Replace all old download modules with a single, unified system.*

* **Task 3.1: Create Unified Downloader**
  * **Objective:** Consolidate all download logic into one module.
  * **Key Steps:**
    1. Create etl/download.py.
    2. Create a Downloader class that takes the PipelineConfig.
    3. Implement a main download(source) method that dispatches based on source.type.
    4. Create private methods for each type: \_download\_http, \_download\_rest, \_download\_ogc, etc.
    5. All methods should use the new SimpleHttpClient from Task 2.1.
    6. Logic should be direct: download file, query API with bbox, save as a single file. No complex pagination or recursion.
  * **Deliverable:** A new etl/download.py module.
* **Task 3.2: Integrate and Deprecate**
  * **Objective:** Wire the new downloader into the orchestrator and remove old code.
  * **Key Steps:**
    1. In etl/pipeline.py, implement \_run\_download\_phase() to use the new Downloader.
    2. Delete the old files: download\_atom.py, download\_http.py, download\_ogc.py, download\_rest.py, download\_wfs.py.
  * **Deliverable:** A slimmed-down etl directory.

### **Phase 4: Staging Simplification (Week 4\)**

*Objective: Replace the complex, auto-detecting staging module with a simple, explicit one.*

* **Task 4.1: Implement Explicit Staging**
  * **Objective:** Create a staging module that relies on configuration, not file sniffing.
  * **Key Steps:**
    1. Create etl/stage.py.
    2. Create a StagingProcessor class.
    3. Implement a stage\_file(source, file\_path) method.
    4. For GeoJSON files, use the source.geometry from the config to call arcpy.JSONToFeatures\_conversion with the correct, explicit geometry type.
    5. For Shapefiles and GPKGs, use arcpy.FeatureClassToFeatureClass\_conversion.
    6. After import, project the feature class to the target\_wkid from the config.
    7. Delete the old etl/stage\_files.py.
  * **Deliverable:** A new etl/stage.py module.

### **Phase 5: Final Integration & Documentation (Week 5\)**

*Objective: Complete the pipeline, run validation, and document the new system.*

* **Task 5.1: Implement Simple SDE Loader**
  * **Objective:** Create a simple, robust SDE loading module.
  * **Key Steps:**
    1. Create etl/load.py.
    2. Create an SdeLoader class.
    3. Implement a load\_fc(fc\_name) method that uses a truncate-and-append pattern. The target dataset should be determined by the source's authority (e.g., Underlag\_SGU).
    4. Delete the old etl/load\_sde.py.
  * **Deliverable:** A new etl/load.py module.
* **Task 5.2: Finalize run.py and Validate**
  * **Objective:** Create the final user-facing script and confirm parity with the baseline.
  * **Key Steps:**
    1. Create a new, simple run.py at the project root.
    2. This script should parse command-line arguments (--config, \--phase), set up logging, load the config, and call the Orchestrator.
    3. Run the full pipeline for the 5 test sources.
    4. Compare the output feature counts, geometry types, and spatial references against the baseline\_report.md. They must match.
  * **Deliverable:** A new run.py and a successful validation run.
* **Task 5.3: Update Documentation**
  * **Objective:** Document the new, simplified workflow.
  * **Key Steps:**
    1. Update README.md to reflect the new config\_v2.yaml structure and the simple python run.py command.
    2. Explain the new source types and the required geometry field.
    3. Create a MIGRATION.md guide explaining how to convert an old configuration to the new format.
  * **Deliverable:** Updated project documentation.

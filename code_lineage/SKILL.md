---
name: code-lineage
description: Extract end-to-end data lineage from code repositories. Use this skill whenever the user wants to scan a codebase, extract functions/methods/classes/API calls/DB queries, build a dependency graph, trace data flows, or produce nodes and edges for a graph database (Neo4j, Snowflake, etc.). Also trigger when the user mentions code scanning for compliance, risk analysis, data lineage, call graphs, cross-service dependencies, or wants to understand how data flows through a system by analyzing source code. Covers single repos and multi-repo batch processing.
---

# Code Lineage Extractor

Extract semantic code structure and data flow lineage from repositories, outputting nodes and edges suitable for graph databases (Neo4j, Snowflake, or any relational store).

## What This Skill Does

Given one or more code repositories, this skill:

1. **Scans** each repo to identify source files by language
2. **Extracts** structural elements: classes, methods/functions, API endpoints, DB queries, external service calls, config references
3. **Maps relationships**: function calls, data flows, inheritance, imports, API dependencies
4. **Outputs** a standardized nodes + edges JSON (or CSV) that can be loaded into Neo4j, Snowflake, or any graph/relational store
5. **Generates** a semantic summary for each method describing its business logic in plain English

## Architecture

```
Repo(s) → Language Detection → File Chunking → Claude Extraction → Node/Edge Assembly → Output (JSON/CSV)
```

Each repo is processed independently. Nodes and edges include a `project_id` and `repo_id` so multiple repos merge cleanly into the same database.

## Node Types

| Node Type | Key Properties |
|-----------|---------------|
| `PROJECT` | project_id, name |
| `REPOSITORY` | repo_id, project_id, name, language |
| `FILE` | file_id, repo_id, path, language |
| `CLASS` | class_id, file_id, name, docstring |
| `METHOD` | method_id, class_id or file_id, name, signature, parameters, return_type, code_snippet, semantic_summary |
| `API_ENDPOINT` | endpoint_id, method_id, http_method, path, request_schema, response_schema |
| `DB_TABLE` | table_id, name, schema |
| `EXTERNAL_SERVICE` | service_id, name, type (REST/gRPC/queue/gateway) |
| `CONFIG` | config_id, key, source_file |
| `DATA_ELEMENT` | element_id, name, parent_entity (table/class/API), data_type, classification (PII/PCI/PHI/GENERAL), description |
| `TRANSFORMATION` | transform_id, name, logic_summary, rule_type (CALCULATION/VALIDATION/ENRICHMENT/MAPPING/FILTER/AGGREGATION), expression |

## Edge Types

| Edge Type | From → To | Description |
|-----------|-----------|-------------|
| `CALLS` | METHOD → METHOD | Direct function/method invocation |
| `READS_FROM` | METHOD → DB_TABLE | SELECT/read operations |
| `WRITES_TO` | METHOD → DB_TABLE | INSERT/UPDATE/DELETE operations |
| `CALLS_API` | METHOD → EXTERNAL_SERVICE | HTTP/gRPC calls to external services |
| `EXPOSES` | METHOD → API_ENDPOINT | Method serves this endpoint |
| `IMPORTS` | FILE → FILE | Import/require dependency |
| `INHERITS` | CLASS → CLASS | Class inheritance |
| `CONTAINS` | FILE → CLASS, CLASS → METHOD | Containment hierarchy |
| `USES_CONFIG` | METHOD → CONFIG | Reads configuration value |
| `RECEIVES_FROM` | API_ENDPOINT → EXTERNAL_SERVICE | Incoming data from external |
| `SENDS_TO` | API_ENDPOINT → EXTERNAL_SERVICE | Outgoing data to external |
| `TRANSFORMS_INTO` | DATA_ELEMENT → DATA_ELEMENT | Field transformed into another field (via TRANSFORMATION) |
| `INPUT_TO` | DATA_ELEMENT → TRANSFORMATION | Field is input to a transformation |
| `OUTPUT_OF` | DATA_ELEMENT → TRANSFORMATION | Field is produced by a transformation |
| `BELONGS_TO` | DATA_ELEMENT → DB_TABLE / CLASS / API_ENDPOINT | Field belongs to this entity |
| `METHOD_TRANSFORMS` | METHOD → TRANSFORMATION | Method contains this transformation |

## Extraction Workflow

### Step 1: Scan the repository

Run `scripts/scan_repo.py` to inventory the repo:

```bash
python /path/to/code-lineage/scripts/scan_repo.py /path/to/repo --output /home/claude/scan_result.json
```

This produces a manifest of all source files grouped by language, with line counts. Use this to plan chunking.

### Step 2: Extract nodes and edges per file

For each source file (or group of small files), use the extraction prompt below to have Claude analyze the code and output structured JSON.

**Chunking strategy:**
- Files under 500 lines: process as-is
- Files 500-2000 lines: split by class/function boundaries
- Files over 2000 lines: split into logical sections, process each with surrounding context

**Extraction prompt template** (customize per language):

```
You are a code lineage extractor. Analyze the following source code and extract ALL of:

1. Classes with their docstrings
2. Methods/functions with: name, signature, parameters, return type, the full code body, and a 1-2 sentence plain English summary of what the method does (business logic, not implementation detail)
3. Database operations: which tables are read from or written to, and what type of operation (SELECT/INSERT/UPDATE/DELETE)
4. External service calls: any HTTP requests, gRPC calls, message queue publishes/consumes, payment gateway calls
5. API endpoints exposed: HTTP method, path, request/response schemas
6. Configuration references: environment variables, config file reads
7. Import dependencies: what other files/modules are imported
8. **Data elements**: For each method, identify the specific data fields/columns/attributes being read or written. Track individual field names, their data types, and classify them (PII, PCI, PHI, or GENERAL).
9. **Transformations**: For each method, identify every business rule, calculation, validation, mapping, filtering, or aggregation applied to data elements. For each transformation, capture: what input fields go in, what output fields come out, the rule type (CALCULATION/VALIDATION/ENRICHMENT/MAPPING/FILTER/AGGREGATION), and a plain English description of the logic. Also capture the code expression if it's a single line or formula.

Output ONLY valid JSON matching this schema:

{
  "file_path": "string",
  "language": "string",
  "classes": [
    {
      "name": "string",
      "docstring": "string",
      "methods": [
        {
          "name": "string",
          "signature": "string",
          "parameters": [{"name": "string", "type": "string"}],
          "return_type": "string",
          "code_snippet": "string (full method body)",
          "semantic_summary": "string (1-2 sentence business logic description)",
          "calls": ["fully.qualified.method.name"],
          "db_operations": [{"table": "string", "operation": "READ|WRITE|DELETE"}],
          "external_calls": [{"service": "string", "type": "REST|gRPC|QUEUE|GATEWAY", "endpoint": "string"}],
          "config_refs": ["CONFIG_KEY_NAME"],
          "api_endpoint": {"method": "GET|POST|PUT|DELETE", "path": "/api/..."},
          "data_elements": [
            {
              "name": "string (e.g. customer.credit_score)",
              "parent_entity": "string (table name, class name, or API path)",
              "data_type": "string",
              "classification": "PII|PCI|PHI|GENERAL",
              "direction": "INPUT|OUTPUT|BOTH",
              "description": "string"
            }
          ],
          "transformations": [
            {
              "name": "string (short label, e.g. 'FICO risk calculation')",
              "rule_type": "CALCULATION|VALIDATION|ENRICHMENT|MAPPING|FILTER|AGGREGATION",
              "logic_summary": "string (plain English: what this transformation does)",
              "expression": "string or null (code expression if single-line)",
              "input_fields": ["field.name"],
              "output_fields": ["field.name"]
            }
          ]
        }
      ]
    }
  ],
  "standalone_functions": [...same structure as methods...],
  "imports": [{"module": "string", "resolved_file": "string or null"}]
}

Source code:
<code>
{FILE_CONTENT}
</code>

Project context: {PROJECT_NAME}, Repository: {REPO_NAME}
```

### Step 3: Assemble the graph

Run `scripts/assemble_graph.py` to merge all per-file extractions into a unified nodes + edges output:

```bash
python /path/to/code-lineage/scripts/assemble_graph.py \
  --input-dir /home/claude/extractions/ \
  --project-id "my-project" \
  --repo-id "payment-service" \
  --output /home/claude/lineage_graph.json
```

### Step 4: Output for target database

**For Neo4j:**
```bash
python /path/to/code-lineage/scripts/export_neo4j.py --input /home/claude/lineage_graph.json --output-dir /home/claude/neo4j_import/
```
Produces CSV files ready for `neo4j-admin import`.

**For Snowflake:**
```bash
python /path/to/code-lineage/scripts/export_snowflake.py --input /home/claude/lineage_graph.json --output-dir /home/claude/snowflake_import/
```
Produces SQL DDL + CSV files for COPY INTO.

## Snowflake Schema

When targeting Snowflake, use this schema (organized by project):

```sql
-- Dimension tables
CREATE TABLE projects (project_id VARCHAR PRIMARY KEY, name VARCHAR);
CREATE TABLE repositories (repo_id VARCHAR PRIMARY KEY, project_id VARCHAR REFERENCES projects, name VARCHAR, primary_language VARCHAR);
CREATE TABLE files (file_id VARCHAR PRIMARY KEY, repo_id VARCHAR REFERENCES repositories, file_path VARCHAR, language VARCHAR, line_count INT);
CREATE TABLE classes (class_id VARCHAR PRIMARY KEY, file_id VARCHAR REFERENCES files, name VARCHAR, docstring TEXT);
CREATE TABLE methods (method_id VARCHAR PRIMARY KEY, parent_id VARCHAR, parent_type VARCHAR, name VARCHAR, signature TEXT, parameters VARIANT, return_type VARCHAR, code_snippet TEXT, semantic_summary TEXT);
CREATE TABLE api_endpoints (endpoint_id VARCHAR PRIMARY KEY, method_id VARCHAR REFERENCES methods, http_method VARCHAR, path VARCHAR, request_schema VARIANT, response_schema VARIANT);
CREATE TABLE db_tables (table_id VARCHAR PRIMARY KEY, name VARCHAR, schema_name VARCHAR);
CREATE TABLE external_services (service_id VARCHAR PRIMARY KEY, name VARCHAR, service_type VARCHAR);
CREATE TABLE configs (config_id VARCHAR PRIMARY KEY, key VARCHAR, source_file VARCHAR);
CREATE TABLE data_elements (element_id VARCHAR PRIMARY KEY, name VARCHAR, parent_entity VARCHAR, data_type VARCHAR, classification VARCHAR, description TEXT);
CREATE TABLE transformations (transform_id VARCHAR PRIMARY KEY, name VARCHAR, logic_summary TEXT, rule_type VARCHAR, expression TEXT);

-- Edge/relationship tables
CREATE TABLE edges (edge_id VARCHAR PRIMARY KEY, edge_type VARCHAR, from_id VARCHAR, from_type VARCHAR, to_id VARCHAR, to_type VARCHAR, metadata VARIANT);
```

## Querying for Compliance and Risk

Once loaded, example queries:

**"Which methods touch customer PII?"**
```sql
SELECT m.name, m.semantic_summary, m.code_snippet, f.file_path
FROM methods m
JOIN edges e ON m.method_id = e.from_id
JOIN db_tables t ON e.to_id = t.table_id
WHERE t.name ILIKE '%customer%' OR t.name ILIKE '%user%' OR t.name ILIKE '%pii%'
  AND e.edge_type IN ('READS_FROM', 'WRITES_TO');
```

**"Trace data flow from trade execution to settlement"**
```sql
-- Use recursive CTE to walk the CALLS edges
WITH RECURSIVE flow AS (
  SELECT m.method_id, m.name, m.semantic_summary, 1 as depth
  FROM methods m WHERE m.name ILIKE '%execute_trade%'
  UNION ALL
  SELECT m2.method_id, m2.name, m2.semantic_summary, f.depth + 1
  FROM flow f
  JOIN edges e ON f.method_id = e.from_id AND e.edge_type = 'CALLS'
  JOIN methods m2 ON e.to_id = m2.method_id
  WHERE f.depth < 10
)
SELECT * FROM flow ORDER BY depth;
```

**"Which services have write access to the transactions table?"**
```sql
SELECT DISTINCT r.name as repo, f.file_path, m.name as method, m.semantic_summary
FROM edges e
JOIN methods m ON e.from_id = m.method_id
JOIN db_tables t ON e.to_id = t.table_id
JOIN files f ON m.parent_id = f.file_id OR m.parent_id IN (SELECT class_id FROM classes WHERE file_id = f.file_id)
JOIN repositories r ON f.repo_id = r.repo_id
WHERE t.name = 'transactions' AND e.edge_type = 'WRITES_TO';
```

## Multi-Repo Batch Processing

### Field-Level Lineage Queries

**"What happens to credit_score across the entire system?"**
```sql
-- Trace a single data element through all transformations
WITH RECURSIVE field_trace AS (
  SELECT de.element_id, de.name, t.name as transform_name, t.logic_summary, t.rule_type, 1 as depth
  FROM data_elements de
  JOIN edges e ON de.element_id = e.from_id AND e.edge_type = 'INPUT_TO'
  JOIN transformations t ON e.to_id = t.transform_id
  WHERE de.name ILIKE '%credit_score%'
  UNION ALL
  SELECT de2.element_id, de2.name, t2.name, t2.logic_summary, t2.rule_type, ft.depth + 1
  FROM field_trace ft
  JOIN edges e1 ON ft.element_id = e1.from_id AND e1.edge_type = 'TRANSFORMS_INTO'
  JOIN data_elements de2 ON e1.to_id = de2.element_id
  JOIN edges e2 ON de2.element_id = e2.from_id AND e2.edge_type = 'INPUT_TO'
  JOIN transformations t2 ON e2.to_id = t2.transform_id
  WHERE ft.depth < 15
)
SELECT * FROM field_trace ORDER BY depth;
```

**"Show all PII fields and every transformation touching them"**
```sql
SELECT de.name as field, de.classification, de.parent_entity,
       t.name as transformation, t.rule_type, t.logic_summary,
       m.name as method, f.file_path, r.name as repo
FROM data_elements de
JOIN edges e1 ON de.element_id = e1.from_id AND e1.edge_type = 'INPUT_TO'
JOIN transformations t ON e1.to_id = t.transform_id
JOIN edges e2 ON t.transform_id = e2.to_id AND e2.edge_type = 'METHOD_TRANSFORMS'
JOIN methods m ON e2.from_id = m.method_id
LEFT JOIN files f ON m.parent_id = f.file_id
LEFT JOIN repositories r ON f.repo_id = r.repo_id
WHERE de.classification = 'PII'
ORDER BY de.name, r.name;
```

**"Which transformations produce account_balance and what are their inputs?"**
```sql
SELECT t.name as transformation, t.logic_summary, t.expression, t.rule_type,
       input_de.name as input_field, input_de.parent_entity as input_source,
       output_de.name as output_field, output_de.parent_entity as output_target
FROM transformations t
JOIN edges e_out ON t.transform_id = e_out.from_id AND e_out.edge_type = 'OUTPUT_OF'
JOIN data_elements output_de ON e_out.to_id = output_de.element_id
JOIN edges e_in ON t.transform_id = e_in.to_id AND e_in.edge_type = 'INPUT_TO'
JOIN data_elements input_de ON e_in.from_id = input_de.element_id
WHERE output_de.name ILIKE '%account_balance%';
```

**"Regulatory audit: full lineage of a data element from source to sink"**
```sql
-- Find all data elements, their transformations, and destination
SELECT
  src.name as source_field, src.parent_entity as source_entity, src.classification,
  t.name as business_rule, t.rule_type, t.logic_summary,
  dst.name as destination_field, dst.parent_entity as destination_entity,
  m.name as method_name, m.semantic_summary,
  f.file_path, r.name as repo
FROM edges e_in
JOIN data_elements src ON e_in.from_id = src.element_id AND e_in.edge_type = 'INPUT_TO'
JOIN transformations t ON e_in.to_id = t.transform_id
JOIN edges e_out ON t.transform_id = e_out.from_id AND e_out.edge_type = 'OUTPUT_OF'
JOIN data_elements dst ON e_out.to_id = dst.element_id
JOIN edges e_mt ON t.transform_id = e_mt.to_id AND e_mt.edge_type = 'METHOD_TRANSFORMS'
JOIN methods m ON e_mt.from_id = m.method_id
LEFT JOIN files f ON m.parent_id = f.file_id
LEFT JOIN repositories r ON f.repo_id = r.repo_id
ORDER BY r.name, f.file_path, m.name;
```

## Multi-Repo Batch Processing

For processing hundreds of repos:

1. Create a `repos.csv` with columns: `project_id, repo_id, repo_path`
2. Process each row independently through Steps 1-3
3. All outputs merge into the same target database via project_id/repo_id segregation
4. Cross-repo lineage emerges from matching external service calls to exposed API endpoints

## Tips

- **Large repos**: Start with the scan to understand scope. Prioritize files that contain business logic over utility/helper files.
- **Polyglot repos**: The extraction prompt adapts per language. Include language-specific hints (e.g., for Java mention annotations like `@RestController`, for Python mention decorators like `@app.route`).
- **Accuracy validation**: Spot-check 5-10 methods against the actual code to verify Claude's semantic summaries are correct before processing all 500 repos.
- **Incremental updates**: Track file hashes. On re-scan, only re-extract files that changed.
- **Context limits**: For very large files, include class-level context (class name, imports) even when processing individual methods, so Claude understands the surrounding scope.

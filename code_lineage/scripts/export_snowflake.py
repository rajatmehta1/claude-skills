#!/usr/bin/env python3
"""Export lineage graph to Snowflake-compatible DDL + CSV files."""

import argparse
import csv
import json
import os
from pathlib import Path


DDL = """
-- Code Lineage Schema
-- Run this first to create tables

CREATE SCHEMA IF NOT EXISTS code_lineage;
USE SCHEMA code_lineage;

CREATE TABLE IF NOT EXISTS projects (
    project_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS repositories (
    repo_id VARCHAR(64) PRIMARY KEY,
    project_id VARCHAR(64) REFERENCES projects(project_id),
    name VARCHAR(256),
    primary_language VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS files (
    file_id VARCHAR(64) PRIMARY KEY,
    repo_id VARCHAR(64) REFERENCES repositories(repo_id),
    file_path VARCHAR(1024),
    language VARCHAR(64),
    line_count INT
);

CREATE TABLE IF NOT EXISTS classes (
    class_id VARCHAR(64) PRIMARY KEY,
    file_id VARCHAR(64) REFERENCES files(file_id),
    name VARCHAR(256),
    docstring TEXT
);

CREATE TABLE IF NOT EXISTS methods (
    method_id VARCHAR(64) PRIMARY KEY,
    parent_id VARCHAR(64),
    parent_type VARCHAR(32),
    name VARCHAR(256),
    signature TEXT,
    parameters VARIANT,
    return_type VARCHAR(256),
    code_snippet TEXT,
    semantic_summary TEXT
);

CREATE TABLE IF NOT EXISTS api_endpoints (
    endpoint_id VARCHAR(64) PRIMARY KEY,
    method_id VARCHAR(64) REFERENCES methods(method_id),
    http_method VARCHAR(16),
    path VARCHAR(1024),
    request_schema VARIANT,
    response_schema VARIANT
);

CREATE TABLE IF NOT EXISTS db_tables (
    table_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256),
    schema_name VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS external_services (
    service_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256),
    service_type VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS configs (
    config_id VARCHAR(64) PRIMARY KEY,
    key VARCHAR(512),
    source_file VARCHAR(1024)
);

CREATE TABLE IF NOT EXISTS data_elements (
    element_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(512),
    parent_entity VARCHAR(256),
    data_type VARCHAR(128),
    classification VARCHAR(32),
    description TEXT
);

CREATE TABLE IF NOT EXISTS transformations (
    transform_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256),
    logic_summary TEXT,
    rule_type VARCHAR(64),
    expression TEXT
);

CREATE TABLE IF NOT EXISTS edges (
    edge_id VARCHAR(64) PRIMARY KEY,
    edge_type VARCHAR(64),
    from_id VARCHAR(64),
    from_type VARCHAR(32),
    to_id VARCHAR(64),
    to_type VARCHAR(32),
    metadata VARIANT
);

-- Useful views

CREATE OR REPLACE VIEW method_lineage AS
SELECT
    m.method_id, m.name as method_name, m.semantic_summary,
    f.file_path, r.name as repo_name, p.name as project_name,
    e.edge_type, e.to_id, e.to_type, e.metadata
FROM methods m
LEFT JOIN edges e ON m.method_id = e.from_id
LEFT JOIN files f ON m.parent_id = f.file_id
    OR m.parent_id IN (SELECT class_id FROM classes WHERE file_id = f.file_id)
LEFT JOIN repositories r ON f.repo_id = r.repo_id
LEFT JOIN projects p ON r.project_id = p.project_id;

CREATE OR REPLACE VIEW data_access_map AS
SELECT
    m.name as method_name, m.semantic_summary,
    dt.name as table_name,
    e.edge_type as access_type,
    e.metadata:operation::VARCHAR as operation,
    f.file_path, r.name as repo_name
FROM edges e
JOIN methods m ON e.from_id = m.method_id
JOIN db_tables dt ON e.to_id = dt.table_id
LEFT JOIN files f ON m.parent_id = f.file_id
    OR m.parent_id IN (SELECT class_id FROM classes WHERE file_id = f.file_id)
LEFT JOIN repositories r ON f.repo_id = r.repo_id
WHERE e.edge_type IN ('READS_FROM', 'WRITES_TO');

CREATE OR REPLACE VIEW field_lineage AS
SELECT
    src.name as source_field, src.classification as source_classification,
    src.parent_entity as source_entity,
    t.name as transformation, t.rule_type, t.logic_summary,
    dst.name as destination_field, dst.parent_entity as destination_entity
FROM edges e_in
JOIN data_elements src ON e_in.from_id = src.element_id AND e_in.edge_type = 'INPUT_TO'
JOIN transformations t ON e_in.to_id = t.transform_id
JOIN edges e_out ON t.transform_id = e_out.from_id AND e_out.edge_type = 'OUTPUT_OF'
JOIN data_elements dst ON e_out.to_id = dst.element_id;

CREATE OR REPLACE VIEW pii_exposure AS
SELECT
    de.name as field, de.parent_entity,
    t.name as transformation, t.rule_type, t.logic_summary,
    m.name as method_name, f.file_path, r.name as repo_name
FROM data_elements de
JOIN edges e1 ON de.element_id = e1.from_id AND e1.edge_type = 'INPUT_TO'
JOIN transformations t ON e1.to_id = t.transform_id
JOIN edges e2 ON t.transform_id = e2.to_id AND e2.edge_type = 'METHOD_TRANSFORMS'
JOIN methods m ON e2.from_id = m.method_id
LEFT JOIN files f ON m.parent_id = f.file_id
LEFT JOIN repositories r ON f.repo_id = r.repo_id
WHERE de.classification IN ('PII', 'PCI', 'PHI');
"""


NODE_TYPE_FIELDS = {
    "PROJECT": ["id", "name"],
    "REPOSITORY": ["id", "project_id", "name"],
    "FILE": ["id", "repo_id", "file_path", "language"],
    "CLASS": ["id", "file_id", "name", "docstring"],
    "METHOD": ["id", "parent_id", "parent_type", "name", "signature", "parameters", "return_type", "code_snippet", "semantic_summary"],
    "API_ENDPOINT": ["id", "method_id", "http_method", "path"],
    "DB_TABLE": ["id", "name"],
    "EXTERNAL_SERVICE": ["id", "name", "service_type"],
    "CONFIG": ["id", "key"],
    "DATA_ELEMENT": ["id", "name", "parent_entity", "data_type", "classification", "description"],
    "TRANSFORMATION": ["id", "name", "logic_summary", "rule_type", "expression"],
}

TABLE_NAME_MAP = {
    "PROJECT": "projects",
    "REPOSITORY": "repositories",
    "FILE": "files",
    "CLASS": "classes",
    "METHOD": "methods",
    "API_ENDPOINT": "api_endpoints",
    "DB_TABLE": "db_tables",
    "EXTERNAL_SERVICE": "external_services",
    "CONFIG": "configs",
    "DATA_ELEMENT": "data_elements",
    "TRANSFORMATION": "transformations",
}


def export_snowflake(graph: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    # Write DDL
    with open(os.path.join(output_dir, "001_ddl.sql"), "w") as f:
        f.write(DDL)

    # Group nodes by type
    by_type = {}
    for node in graph["nodes"]:
        ntype = node["type"]
        by_type.setdefault(ntype, []).append(node)

    # Write CSV per node type
    for ntype, nodes in by_type.items():
        fields = NODE_TYPE_FIELDS.get(ntype, ["id", "name"])
        table_name = TABLE_NAME_MAP.get(ntype, ntype.lower())
        csv_path = os.path.join(output_dir, f"{table_name}.csv")

        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for node in nodes:
                row = {}
                for field in fields:
                    val = node.get(field, "")
                    if isinstance(val, (dict, list)):
                        val = json.dumps(val)
                    row[field] = val
                writer.writerow(row)

    # Write edges CSV
    edge_fields = ["edge_id", "edge_type", "from_id", "from_type", "to_id", "to_type", "metadata"]
    with open(os.path.join(output_dir, "edges.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=edge_fields, extrasaction="ignore")
        writer.writeheader()
        for edge in graph["edges"]:
            row = dict(edge)
            if "metadata" in row and isinstance(row["metadata"], (dict, list)):
                row["metadata"] = json.dumps(row["metadata"])
            writer.writerow(row)

    # Write COPY INTO commands
    copy_sql = ["USE SCHEMA code_lineage;\n"]
    for ntype in by_type:
        table_name = TABLE_NAME_MAP.get(ntype, ntype.lower())
        copy_sql.append(f"""
COPY INTO {table_name}
FROM @my_stage/{table_name}.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
""")
    copy_sql.append("""
COPY INTO edges
FROM @my_stage/edges.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
""")

    with open(os.path.join(output_dir, "002_copy_into.sql"), "w") as f:
        f.write("\n".join(copy_sql))

    print(f"Snowflake export: {output_dir}/")
    print(f"  DDL: 001_ddl.sql")
    print(f"  Data: {len(by_type)} CSV files + edges.csv")
    print(f"  Load: 002_copy_into.sql")


def main():
    parser = argparse.ArgumentParser(description='Export lineage graph for Snowflake')
    parser.add_argument('--input', required=True, help='Path to lineage_graph.json')
    parser.add_argument('--output-dir', default=None)
    args = parser.parse_args()

    if args.output_dir is None:
        args.output_dir = os.path.join(os.path.dirname(args.input), "snowflake_export")

    with open(args.input) as f:
        graph = json.load(f)

    export_snowflake(graph, args.output_dir)


if __name__ == '__main__':
    main()

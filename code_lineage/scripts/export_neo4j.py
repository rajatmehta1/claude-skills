#!/usr/bin/env python3
"""Export lineage graph to Neo4j-compatible CSV files for neo4j-admin import."""

import argparse
import csv
import json
import os


def export_neo4j(graph: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    # Nodes CSV — all types in one file with :LABEL column
    node_fields = ["id:ID", "name", "type:LABEL", "file_path", "language", "signature",
                    "semantic_summary", "code_snippet", "docstring", "http_method", "path",
                    "service_type", "key", "parent_id", "parent_type", "project_id", "repo_id"]

    with open(os.path.join(output_dir, "nodes.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=node_fields, extrasaction="ignore")
        writer.writeheader()
        for node in graph["nodes"]:
            row = {
                "id:ID": node["id"],
                "name": node.get("name", ""),
                "type:LABEL": node["type"],
            }
            for k, v in node.items():
                if k in ("id", "type"):
                    continue
                if isinstance(v, (dict, list)):
                    v = json.dumps(v)
                if k in node_fields or f"{k}" in [f.split(":")[0] for f in node_fields]:
                    row[k] = v
            writer.writerow(row)

    # Edges CSV
    edge_fields = [":START_ID", ":END_ID", ":TYPE", "metadata"]
    with open(os.path.join(output_dir, "edges.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=edge_fields, extrasaction="ignore")
        writer.writeheader()
        for edge in graph["edges"]:
            writer.writerow({
                ":START_ID": edge["from_id"],
                ":END_ID": edge["to_id"],
                ":TYPE": edge["type"],
                "metadata": json.dumps(edge.get("metadata", {})),
            })

    # Cypher load script (alternative to neo4j-admin import)
    cypher = """// Load nodes
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row
CALL apoc.create.node([row.`type:LABEL`], {
  id: row.`id:ID`, name: row.name, file_path: row.file_path,
  language: row.language, signature: row.signature,
  semantic_summary: row.semantic_summary, code_snippet: row.code_snippet,
  docstring: row.docstring, http_method: row.http_method, path: row.path,
  service_type: row.service_type, key: row.key,
  parent_id: row.parent_id, project_id: row.project_id, repo_id: row.repo_id
}) YIELD node RETURN count(node);

// Create index on id for fast lookups
CREATE INDEX node_id IF NOT EXISTS FOR (n:METHOD) ON (n.id);
CREATE INDEX node_id_file IF NOT EXISTS FOR (n:FILE) ON (n.id);
CREATE INDEX node_id_class IF NOT EXISTS FOR (n:CLASS) ON (n.id);

// Load edges
LOAD CSV WITH HEADERS FROM 'file:///edges.csv' AS row
MATCH (a {id: row.`:START_ID`})
MATCH (b {id: row.`:END_ID`})
CALL apoc.create.relationship(a, row.`:TYPE`, {metadata: row.metadata}, b) YIELD rel
RETURN count(rel);
"""
    with open(os.path.join(output_dir, "load_cypher.cql"), "w") as f:
        f.write(cypher)

    print(f"Neo4j export: {output_dir}/")
    print(f"  Nodes: nodes.csv ({len(graph['nodes'])} nodes)")
    print(f"  Edges: edges.csv ({len(graph['edges'])} edges)")
    print(f"  Cypher: load_cypher.cql")


def main():
    parser = argparse.ArgumentParser(description='Export lineage graph for Neo4j')
    parser.add_argument('--input', required=True, help='Path to lineage_graph.json')
    parser.add_argument('--output-dir', default='neo4j_import')
    args = parser.parse_args()

    with open(args.input) as f:
        graph = json.load(f)

    export_neo4j(graph, args.output_dir)


if __name__ == '__main__':
    main()

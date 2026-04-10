#!/usr/bin/env python3
"""Assemble per-file extraction JSONs into a unified nodes + edges graph."""

import argparse
import json
import os
import uuid
from pathlib import Path


def make_id(prefix: str, *parts: str) -> str:
    """Deterministic ID from components."""
    raw = ":".join(str(p) for p in parts)
    return f"{prefix}_{uuid.uuid5(uuid.NAMESPACE_DNS, raw).hex[:12]}"


def assemble(input_dir: str, project_id: str, repo_id: str) -> dict:
    nodes = []
    edges = []

    # Project and repo nodes
    nodes.append({"id": project_id, "type": "PROJECT", "name": project_id})
    nodes.append({"id": repo_id, "type": "REPOSITORY", "project_id": project_id, "name": repo_id})

    extraction_files = sorted(Path(input_dir).glob("*.json"))
    
    known_methods = {}  # fully_qualified_name -> method_id
    known_tables = {}   # table_name -> table_id
    known_services = {} # service_name -> service_id
    known_configs = {}  # config_key -> config_id

    for ef in extraction_files:
        with open(ef) as f:
            data = json.load(f)

        file_path = data.get("file_path", ef.stem)
        language = data.get("language", "unknown")
        file_id = make_id("file", repo_id, file_path)

        nodes.append({
            "id": file_id, "type": "FILE",
            "repo_id": repo_id, "file_path": file_path, "language": language,
        })

        def process_methods(methods, parent_id, parent_type, class_name=None):
            for m in methods:
                fqn = f"{file_path}:{class_name}.{m['name']}" if class_name else f"{file_path}:{m['name']}"
                method_id = make_id("method", repo_id, fqn)
                known_methods[fqn] = method_id
                # Also register short name for cross-file matching
                short = f"{class_name}.{m['name']}" if class_name else m['name']
                known_methods[short] = method_id

                nodes.append({
                    "id": method_id, "type": "METHOD",
                    "parent_id": parent_id, "parent_type": parent_type,
                    "name": m.get("name"), "signature": m.get("signature"),
                    "parameters": m.get("parameters", []),
                    "return_type": m.get("return_type"),
                    "code_snippet": m.get("code_snippet", ""),
                    "semantic_summary": m.get("semantic_summary", ""),
                })
                edges.append({
                    "type": "CONTAINS", "from_id": parent_id,
                    "from_type": parent_type, "to_id": method_id, "to_type": "METHOD",
                })

                # DB operations
                for db_op in m.get("db_operations", []):
                    table_name = db_op["table"]
                    if table_name not in known_tables:
                        tid = make_id("table", table_name)
                        known_tables[table_name] = tid
                        nodes.append({"id": tid, "type": "DB_TABLE", "name": table_name})
                    edge_type = "WRITES_TO" if db_op["operation"] in ("WRITE", "DELETE") else "READS_FROM"
                    edges.append({
                        "type": edge_type, "from_id": method_id,
                        "from_type": "METHOD", "to_id": known_tables[table_name], "to_type": "DB_TABLE",
                        "metadata": {"operation": db_op["operation"]},
                    })

                # External calls
                for ext in m.get("external_calls", []):
                    svc_name = ext["service"]
                    if svc_name not in known_services:
                        sid = make_id("service", svc_name)
                        known_services[svc_name] = sid
                        nodes.append({"id": sid, "type": "EXTERNAL_SERVICE", "name": svc_name, "service_type": ext.get("type", "REST")})
                    edges.append({
                        "type": "CALLS_API", "from_id": method_id,
                        "from_type": "METHOD", "to_id": known_services[svc_name], "to_type": "EXTERNAL_SERVICE",
                        "metadata": {"endpoint": ext.get("endpoint", "")},
                    })

                # API endpoint
                ep = m.get("api_endpoint")
                if ep and ep.get("path"):
                    ep_id = make_id("endpoint", repo_id, ep["method"], ep["path"])
                    nodes.append({
                        "id": ep_id, "type": "API_ENDPOINT",
                        "method_id": method_id, "http_method": ep["method"], "path": ep["path"],
                    })
                    edges.append({
                        "type": "EXPOSES", "from_id": method_id,
                        "from_type": "METHOD", "to_id": ep_id, "to_type": "API_ENDPOINT",
                    })

                # Config refs
                for cfg_key in m.get("config_refs", []):
                    if cfg_key not in known_configs:
                        cid = make_id("config", cfg_key)
                        known_configs[cfg_key] = cid
                        nodes.append({"id": cid, "type": "CONFIG", "key": cfg_key})
                    edges.append({
                        "type": "USES_CONFIG", "from_id": method_id,
                        "from_type": "METHOD", "to_id": known_configs[cfg_key], "to_type": "CONFIG",
                    })

                # Method calls (deferred resolution)
                for call_target in m.get("calls", []):
                    edges.append({
                        "type": "CALLS", "from_id": method_id,
                        "from_type": "METHOD", "to_id": f"__unresolved__{call_target}", "to_type": "METHOD",
                    })

                # Data elements
                known_elements_local = {}
                for de in m.get("data_elements", []):
                    el_name = de["name"]
                    el_id = make_id("element", repo_id, el_name, de.get("parent_entity", ""))
                    if el_id not in known_elements_local:
                        known_elements_local[el_id] = True
                        nodes.append({
                            "id": el_id, "type": "DATA_ELEMENT",
                            "name": el_name,
                            "parent_entity": de.get("parent_entity", ""),
                            "data_type": de.get("data_type", ""),
                            "classification": de.get("classification", "GENERAL"),
                            "description": de.get("description", ""),
                        })
                        # BELONGS_TO edge to parent entity (table or class)
                        parent_ent = de.get("parent_entity", "")
                        if parent_ent in known_tables:
                            edges.append({
                                "type": "BELONGS_TO", "from_id": el_id,
                                "from_type": "DATA_ELEMENT", "to_id": known_tables[parent_ent], "to_type": "DB_TABLE",
                            })

                # Transformations
                for tx in m.get("transformations", []):
                    tx_id = make_id("transform", repo_id, fqn, tx["name"])
                    nodes.append({
                        "id": tx_id, "type": "TRANSFORMATION",
                        "name": tx.get("name", ""),
                        "logic_summary": tx.get("logic_summary", ""),
                        "rule_type": tx.get("rule_type", ""),
                        "expression": tx.get("expression", ""),
                    })
                    edges.append({
                        "type": "METHOD_TRANSFORMS", "from_id": method_id,
                        "from_type": "METHOD", "to_id": tx_id, "to_type": "TRANSFORMATION",
                    })
                    # Input fields
                    for inp_name in tx.get("input_fields", []):
                        inp_id = make_id("element", repo_id, inp_name, "")
                        # Ensure node exists
                        if not any(n["id"] == inp_id for n in nodes):
                            nodes.append({"id": inp_id, "type": "DATA_ELEMENT", "name": inp_name, "classification": "GENERAL"})
                        edges.append({
                            "type": "INPUT_TO", "from_id": inp_id,
                            "from_type": "DATA_ELEMENT", "to_id": tx_id, "to_type": "TRANSFORMATION",
                        })
                    # Output fields
                    for out_name in tx.get("output_fields", []):
                        out_id = make_id("element", repo_id, out_name, "")
                        if not any(n["id"] == out_id for n in nodes):
                            nodes.append({"id": out_id, "type": "DATA_ELEMENT", "name": out_name, "classification": "GENERAL"})
                        edges.append({
                            "type": "OUTPUT_OF", "from_id": tx_id,
                            "from_type": "TRANSFORMATION", "to_id": out_id, "to_type": "DATA_ELEMENT",
                        })
                    # TRANSFORMS_INTO edges between input and output fields
                    for inp_name in tx.get("input_fields", []):
                        inp_id = make_id("element", repo_id, inp_name, "")
                        for out_name in tx.get("output_fields", []):
                            out_id = make_id("element", repo_id, out_name, "")
                            edges.append({
                                "type": "TRANSFORMS_INTO", "from_id": inp_id,
                                "from_type": "DATA_ELEMENT", "to_id": out_id, "to_type": "DATA_ELEMENT",
                                "metadata": {"via_transform": tx.get("name", ""), "rule_type": tx.get("rule_type", "")},
                            })

        # Process classes
        for cls in data.get("classes", []):
            class_id = make_id("class", repo_id, file_path, cls["name"])
            nodes.append({
                "id": class_id, "type": "CLASS",
                "file_id": file_id, "name": cls["name"],
                "docstring": cls.get("docstring", ""),
            })
            edges.append({
                "type": "CONTAINS", "from_id": file_id,
                "from_type": "FILE", "to_id": class_id, "to_type": "CLASS",
            })
            process_methods(cls.get("methods", []), class_id, "CLASS", cls["name"])

        # Process standalone functions
        process_methods(data.get("standalone_functions", []), file_id, "FILE")

        # Imports
        for imp in data.get("imports", []):
            edges.append({
                "type": "IMPORTS", "from_id": file_id, "from_type": "FILE",
                "to_id": f"__unresolved_file__{imp.get('resolved_file', imp['module'])}",
                "to_type": "FILE",
                "metadata": {"module": imp["module"]},
            })

    # Resolve method call edges
    for edge in edges:
        if edge["to_id"].startswith("__unresolved__"):
            target_name = edge["to_id"].replace("__unresolved__", "")
            if target_name in known_methods:
                edge["to_id"] = known_methods[target_name]
            else:
                edge["to_id"] = make_id("method", "external", target_name)
                # Add as external/unresolved node if not exists
                if not any(n["id"] == edge["to_id"] for n in nodes):
                    nodes.append({"id": edge["to_id"], "type": "METHOD", "name": target_name, "semantic_summary": "External/unresolved method"})

    # Resolve file import edges
    for edge in edges:
        if edge["to_id"].startswith("__unresolved_file__"):
            target = edge["to_id"].replace("__unresolved_file__", "")
            resolved = make_id("file", repo_id, target)
            if any(n["id"] == resolved for n in nodes):
                edge["to_id"] = resolved
            # else leave as-is (external dependency)

    # Add edge IDs
    for i, edge in enumerate(edges):
        edge["edge_id"] = f"edge_{i:06d}"

    return {
        "project_id": project_id,
        "repo_id": repo_id,
        "summary": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "node_types": {t: sum(1 for n in nodes if n["type"] == t) for t in set(n["type"] for n in nodes)},
            "edge_types": {t: sum(1 for e in edges if e["type"] == t) for t in set(e["type"] for e in edges)},
        },
        "nodes": nodes,
        "edges": edges,
    }


def main():
    parser = argparse.ArgumentParser(description='Assemble extraction JSONs into unified graph')
    parser.add_argument('--input-dir', required=True, help='Directory with per-file extraction JSONs')
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--repo-id', required=True)
    parser.add_argument('--output', '-o', default='lineage_graph.json')
    args = parser.parse_args()

    result = assemble(args.input_dir, args.project_id, args.repo_id)

    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2)

    s = result["summary"]
    print(f"Assembled: {s['total_nodes']} nodes, {s['total_edges']} edges")
    for ntype, count in sorted(s["node_types"].items()):
        print(f"  {ntype}: {count}")
    print(f"Output: {args.output}")


if __name__ == '__main__':
    main()

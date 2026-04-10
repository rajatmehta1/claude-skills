#!/usr/bin/env python3
"""Scan a repository and produce a manifest of source files grouped by language."""

import argparse
import json
import os
import hashlib
from pathlib import Path
from collections import defaultdict

LANGUAGE_EXTENSIONS = {
    '.py': 'Python', '.java': 'Java', '.kt': 'Kotlin', '.scala': 'Scala',
    '.js': 'JavaScript', '.ts': 'TypeScript', '.tsx': 'TypeScript',
    '.jsx': 'JavaScript', '.go': 'Go', '.rs': 'Rust', '.rb': 'Ruby',
    '.cs': 'C#', '.cpp': 'C++', '.c': 'C', '.h': 'C/C++ Header',
    '.php': 'PHP', '.swift': 'Swift', '.m': 'Objective-C',
    '.sql': 'SQL', '.graphql': 'GraphQL', '.proto': 'Protobuf',
}

SKIP_DIRS = {
    'node_modules', '.git', '__pycache__', '.venv', 'venv', 'env',
    'dist', 'build', '.next', '.nuxt', 'target', 'bin', 'obj',
    '.idea', '.vscode', '.gradle', '.mvn', 'vendor', 'coverage',
    '.tox', '.mypy_cache', '.pytest_cache', '.eggs',
}

SKIP_PATTERNS = {
    'package-lock.json', 'yarn.lock', 'pnpm-lock.yaml',
    '.min.js', '.min.css', '.bundle.js', '.map',
}


def file_hash(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()[:16]


def count_lines(filepath: str) -> int:
    try:
        with open(filepath, 'r', errors='replace') as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def should_skip_file(filename: str) -> bool:
    return any(pattern in filename for pattern in SKIP_PATTERNS)


def scan_repo(repo_path: str) -> dict:
    repo_path = Path(repo_path).resolve()
    files_by_language = defaultdict(list)
    total_files = 0
    total_lines = 0

    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        
        for fname in files:
            if should_skip_file(fname):
                continue
            
            ext = Path(fname).suffix.lower()
            language = LANGUAGE_EXTENSIONS.get(ext)
            if not language:
                continue
            
            filepath = os.path.join(root, fname)
            rel_path = os.path.relpath(filepath, repo_path)
            lines = count_lines(filepath)
            fhash = file_hash(filepath)
            
            files_by_language[language].append({
                'path': rel_path,
                'lines': lines,
                'hash': fhash,
                'size_category': 'small' if lines < 500 else 'medium' if lines < 2000 else 'large',
            })
            total_files += 1
            total_lines += lines

    return {
        'repo_path': str(repo_path),
        'repo_name': repo_path.name,
        'total_files': total_files,
        'total_lines': total_lines,
        'languages': {
            lang: {
                'file_count': len(files),
                'total_lines': sum(f['lines'] for f in files),
                'files': sorted(files, key=lambda x: -x['lines']),
            }
            for lang, files in sorted(files_by_language.items(), key=lambda x: -len(x[1]))
        }
    }


def main():
    parser = argparse.ArgumentParser(description='Scan repository for source files')
    parser.add_argument('repo_path', help='Path to the repository')
    parser.add_argument('--project-id', default='default', help='Project identifier')
    parser.add_argument('--repo-id', default=None, help='Repository identifier (defaults to folder name)')
    parser.add_argument('--output', '-o', default=None, help='Output JSON path')
    args = parser.parse_args()

    if args.repo_id is None:
        args.repo_id = Path(args.repo_path).resolve().name
    if args.output is None:
        out_dir = os.path.join("lineage-output", args.project_id, args.repo_id)
        os.makedirs(out_dir, exist_ok=True)
        args.output = os.path.join(out_dir, "scan_result.json")

    result = scan_repo(args.repo_path)
    
    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2)
    
    print(f"Scanned: {result['total_files']} files, {result['total_lines']} lines")
    for lang, info in result['languages'].items():
        print(f"  {lang}: {info['file_count']} files ({info['total_lines']} lines)")
    print(f"Output: {args.output}")


if __name__ == '__main__':
    main()

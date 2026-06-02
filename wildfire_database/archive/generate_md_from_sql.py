#!/usr/bin/env python3
"""
Generate Markdown documentation from SQL function files.

This script treats SQL as canonical and emits a GitHub-friendly .md file with:
- optional source header comments
- one section per CREATE OR REPLACE FUNCTION block
- SQL fenced code blocks for each function definition
"""

import argparse
import re
import sys
from pathlib import Path


FUNC_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+FUNCTION\s+"
    r"(?P<name>[a-zA-Z0-9_\.]+)"
    r"(?P<body>[\s\S]*?)\$\$;",
    re.IGNORECASE,
)


def _read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as f:
        return f.read()


def _extract_header_comments(sql_text: str) -> list[str]:
    header = []
    for line in sql_text.splitlines():
        if line.startswith("--"):
            header.append(line[2:].strip())
            continue
        if line.strip() == "":
            if header:
                continue
            continue
        break
    return [h for h in header if h]


def _extract_sections(sql_text: str) -> list[dict]:
    sections = []
    for m in FUNC_PATTERN.finditer(sql_text):
        fn_name = m.group("name")
        fn_sql = (m.group(0) + "\n").strip("\n")

        # Look backward for an immediate chunk label comment such as: -- chunk_6
        prior = sql_text[: m.start()]
        chunk_label = None
        prior_lines = prior.splitlines()
        i = len(prior_lines) - 1
        while i >= 0 and prior_lines[i].strip() == "":
            i -= 1
        if i >= 0 and prior_lines[i].strip().startswith("--"):
            maybe = prior_lines[i].strip()[2:].strip()
            if maybe.lower().startswith("chunk_"):
                chunk_label = maybe

        sections.append(
            {
                "name": fn_name,
                "sql": fn_sql,
                "chunk": chunk_label,
            }
        )
    return sections


def generate_markdown(sql_file: Path, include_toc: bool = False) -> str:
    sql_text = _read_text(sql_file)
    header_comments = _extract_header_comments(sql_text)
    sections = _extract_sections(sql_text)

    if not sections:
        raise ValueError("No CREATE OR REPLACE FUNCTION blocks were found in the SQL file.")

    lines: list[str] = []
    lines.append(f"# {sql_file.stem}")
    lines.append("")
    lines.append("Generated from SQL source. SQL is the canonical source for these docs.")
    lines.append("")

    if header_comments:
        lines.append("## Source Notes")
        lines.append("")
        for c in header_comments:
            lines.append(f"- {c}")
        lines.append("")

    if include_toc:
        lines.append("## Functions")
        lines.append("")
        for s in sections:
            anchor = s["name"].lower().replace(".", "")
            lines.append(f"- [{s['name']}](#{anchor})")
        lines.append("")

    for s in sections:
        lines.append(f"## {s['name']}")
        lines.append("")
        if s["chunk"]:
            lines.append(f"- Source chunk: {s['chunk']}")
            lines.append("")
        lines.append("```sql")
        lines.append(s["sql"])
        lines.append("```")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Markdown documentation from a SQL function file."
    )
    parser.add_argument("sql_file", help="Path to input SQL file")
    parser.add_argument(
        "-o",
        "--output",
        help="Path to output Markdown file (default: input with .md suffix)",
    )
    parser.add_argument(
        "--toc",
        action="store_true",
        help="Include a simple table of contents",
    )
    args = parser.parse_args()

    sql_path = Path(args.sql_file)
    if not sql_path.exists():
        print(f"ERROR: File not found: {sql_path}")
        sys.exit(1)

    out_path = Path(args.output) if args.output else sql_path.with_suffix(".md")

    try:
        md = generate_markdown(sql_path, include_toc=args.toc)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    out_path.write_text(md, encoding="utf-8")
    print(f"Generated Markdown: {out_path}")


if __name__ == "__main__":
    main()

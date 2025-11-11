#!/usr/bin/env python3
"""
Extract SQL chunks from Quarto .qmd files and compile into a single .sql file
"""
import re
import sys
import argparse
from pathlib import Path

def extract_sql_chunks(qmd_file):
    """Extract SQL code blocks from Quarto file"""
    with open(qmd_file, 'r') as f:
        content = f.read()
    
    # More robust pattern to match SQL chunks
    pattern = r'```\{sql\}([^`]*?)\n((?:#\|[^\n]*\n)*)(.*?)```'
    
    sql_blocks = []
    chunks_found = 0
    
    for match in re.finditer(pattern, content, re.DOTALL):
        chunks_found += 1
        options = match.group(2) if match.group(2) else ""
        sql_code = match.group(3).strip()
        
        print(f"🔍 Chunk {chunks_found}:")
        print(f"   Options: {repr(options[:100])}")
        print(f"   SQL preview: {sql_code[:100]}...")
        
        # Extract label if present
        label_match = re.search(r'#\|\s*label:\s*([^\n]+)', options)
        label = label_match.group(1).strip() if label_match else f"chunk_{len(sql_blocks)//3 + 1}"
        
        print(f"   ✅ Including as: {label}")
        
        # Add label as comment and SQL code
        sql_blocks.append(f"-- {label}")
        sql_blocks.append(sql_code)
        sql_blocks.append("")  # Empty line between chunks
    
    print(f"\n📊 Total chunks found: {chunks_found}")
    print(f"📊 Chunks included: {len([b for b in sql_blocks if not b.startswith('--') and b.strip()])}") 
    
    return "\n".join(sql_blocks)

def main():
    parser = argparse.ArgumentParser(
        description='Extract SQL chunks from Quarto .qmd files and compile into a single .sql file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - auto-generates output filename
  python3 extract_sql.py water_chem_functions.qmd
  
  # Specify custom output file
  python3 extract_sql.py water_chem_functions.qmd -o custom_output.sql
  
  # Work with different qmd files
  python3 extract_sql.py wildfire_database_query_nitrate.qmd
  python3 extract_sql.py wildfire_database_query_orthop.qmd -o orthop_queries.sql
        """
    )
    
    parser.add_argument('qmd_file', help='Path to the .qmd file')
    parser.add_argument('-o', '--output', help='Output .sql file path (default: auto-generated from input filename)')
    parser.add_argument('--no-transaction', action='store_true', help='Do not wrap SQL in transaction')
    
    args = parser.parse_args()
    
    qmd_file = Path(args.qmd_file)
    if not qmd_file.exists():
        print(f"❌ File not found: {qmd_file}")
        sys.exit(1)
    
    # Generate output filename if not specified
    if args.output:
        sql_file = Path(args.output)
    else:
        sql_file = qmd_file.with_suffix('.sql')
    
    print(f"📖 Extracting SQL chunks from: {qmd_file}")
    print(f"📝 Output file: {sql_file}")
    print("=" * 50)
    
    # Extract SQL content
    sql_content = extract_sql_chunks(qmd_file)
    
    if not sql_content.strip():
        print("❌ No SQL chunks were included!")
        sys.exit(1)
    
    # Wrap in transaction unless disabled
    if args.no_transaction:
        final_sql = f"""-- Generated from {qmd_file.name}

{sql_content}
"""
    else:
        final_sql = f"""-- Generated from {qmd_file.name}
-- Execute all functions in a single transaction

BEGIN;

{sql_content}

COMMIT;

-- If any errors occurred, the transaction will be rolled back automatically
"""
    
    with open(sql_file, 'w') as f:
        f.write(final_sql)
    
    print("=" * 50)
    print(f"📝 Generated: {sql_file}")
    print(f"🚀 To execute: psql -d wildfire_dev -U srearl -f {sql_file}")
    if not args.no_transaction:
        print(f"💡 All SQL will execute in a single transaction (auto-rollback on any error)")

if __name__ == "__main__":
    main()
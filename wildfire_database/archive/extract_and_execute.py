#!/usr/bin/env python3
"""
Extract SQL chunks from Quarto .qmd files and execute them with psql
"""
import re
import subprocess
import sys
import tempfile
import os
import argparse
from pathlib import Path

def extract_sql_chunks(qmd_file):
    """Extract SQL code blocks from Quarto file"""
    with open(qmd_file, 'r') as f:
        content = f.read()
    
    # Pattern to match SQL chunks with their labels
    pattern = r'```{sql}\s*\n(?:#\|\s*.*\n)*(?:#\|\s*label:\s*([^\n]+)\n)?.*?\n(.*?)\n```'
    
    chunks = []
    for match in re.finditer(pattern, content, re.DOTALL):
        label = match.group(1) if match.group(1) else "unlabeled"
        sql_code = match.group(2).strip()
        
        # Skip chunks that are commented out or marked as eval: false
        chunk_header = match.group(0).split('\n')[1:5]  # Get first few lines
        if any('eval: false' in line.lower() for line in chunk_header):
            print(f"⏭️  Skipping chunk '{label}' (eval: false)")
            continue
            
        chunks.append({
            'label': label.strip() if label else f"chunk_{len(chunks)+1}",
            'sql': sql_code
        })
    
    return chunks

def execute_sql_chunk(sql_code, chunk_label, host=None, port=None, db_name=None, user=None, password=None):
    """Execute a single SQL chunk with psql"""
    
    # Create temporary file for SQL
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(sql_code)
        temp_file = f.name
    
    try:
        # Build psql command with connection parameters
        cmd = ['psql']
        
        # Add connection parameters if provided
        if host:
            cmd.extend(['-h', host])
        if port:
            cmd.extend(['-p', str(port)])
        if db_name:
            cmd.extend(['-d', db_name])
        if user:
            cmd.extend(['-U', user])
        
        # Add the SQL file
        cmd.extend(['-f', temp_file])
        
        # Set environment variables
        env = os.environ.copy()
        if password:
            env['PGPASSWORD'] = password
        
        print(f"🔄 Executing chunk: {chunk_label}")
        
        # Execute with psql
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True,
            timeout=300,  # 5 minute timeout
            env=env
        )
        
        if result.returncode == 0:
            print(f"✅ SUCCESS: {chunk_label}")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
        else:
            print(f"❌ ERROR in {chunk_label}:")
            print(f"   {result.stderr.strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ TIMEOUT: {chunk_label} took too long")
        return False
    except Exception as e:
        print(f"💥 EXCEPTION in {chunk_label}: {e}")
        return False
    finally:
        # Clean up temp file
        os.unlink(temp_file)
    
    return True

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Extract SQL chunks from Quarto .qmd files and execute them with psql',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with defaults
  python extract_and_execute.py water_chem_functions.qmd
  
  # Specify database and user
  python extract_and_execute.py water_chem_functions.qmd -d wildfire_dev -U srearl
  
  # Full connection details
  python extract_and_execute.py water_chem_functions.qmd -h localhost -p 5432 -d wildfire_dev -U srearl
  
  # With password (will prompt if not provided via environment)
  PGPASSWORD=mypass python extract_and_execute.py water_chem_functions.qmd -d wildfire_dev -U srearl
        """
    )
    
    parser.add_argument('qmd_file', help='Path to the .qmd file')
    parser.add_argument('-h', '--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=5432, help='Database port (default: 5432)')
    parser.add_argument('-d', '--dbname', default='wildfire_dev', help='Database name (default: wildfire_dev)')
    parser.add_argument('-U', '--username', help='Database username (default: current user)')
    parser.add_argument('-W', '--password', help='Database password (or set PGPASSWORD env var)')
    parser.add_argument('--dry-run', action='store_true', help='Extract chunks but do not execute')
    
    args = parser.parse_args()
    
    if not Path(args.qmd_file).exists():
        print(f"❌ File not found: {args.qmd_file}")
        sys.exit(1)
    
    # Use current user if username not specified
    username = args.username or os.getenv('USER')
    
    print(f"📖 Extracting SQL chunks from: {args.qmd_file}")
    print(f"🎯 Target: {username}@{args.host}:{args.port}/{args.dbname}")
    print("=" * 50)
    
    # Extract chunks
    chunks = extract_sql_chunks(args.qmd_file)
    print(f"📝 Found {len(chunks)} SQL chunks")
    
    if args.dry_run:
        print("\n🔍 DRY RUN - SQL chunks found:")
        for i, chunk in enumerate(chunks, 1):
            print(f"\n--- Chunk {i}: {chunk['label']} ---")
            print(chunk['sql'][:200] + "..." if len(chunk['sql']) > 200 else chunk['sql'])
        sys.exit(0)
    
    # Execute each chunk
    success_count = 0
    for chunk in chunks:
        if execute_sql_chunk(
            chunk['sql'], 
            chunk['label'], 
            host=args.host,
            port=args.port,
            db_name=args.dbname,
            user=username,
            password=args.password
        ):
            success_count += 1
        print("-" * 30)
    
    print(f"🏁 SUMMARY: {success_count}/{len(chunks)} chunks executed successfully")
    
    if success_count == len(chunks):
        print("🎉 All chunks executed successfully!")
        sys.exit(0)
    else:
        print("⚠️  Some chunks failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
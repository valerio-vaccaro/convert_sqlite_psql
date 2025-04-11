#!/usr/bin/env python3

import sys
import sqlparse
import re

def process_sql_dump(sql_content):
    # Parse the SQL content
    parsed = sqlparse.parse(sql_content)
    output_statements = []
    foreign_key_statements = []

    for statement in parsed:
        # Convert statement to string and clean it
        stmt_str = str(statement).strip()
        tokens = statement.tokens

        # Check if it's a CREATE TABLE statement
        if statement.get_type() == 'CREATE' and 'TABLE' in stmt_str.upper():
            table_name = None
            fk_constraints = []
            other_lines = []
            inside_table_def = False

            # Split the statement into lines for processing
            lines = stmt_str.split('\n')
            for line in lines:
                line = line.strip()
                if line.upper().startswith('CREATE TABLE'):
                    # Extract table name
                    parts = line.split()
                    table_name = parts[2].strip('(').strip()
                    other_lines.append(line)
                    inside_table_def = True
                elif inside_table_def and 'FOREIGN KEY' in line.upper():
                    # Collect foreign key constraints
                    fk_constraints.append(line.strip(',').strip())
                elif inside_table_def and line.strip() == ');':
                    # Skip the closing parenthesis for now
                    continue
                else:
                    other_lines.append(line)
                    if line.strip().endswith(');'):
                        inside_table_def = False

            # Rebuild CREATE TABLE without foreign keys
            if fk_constraints:
                # Remove trailing comma from the last column definition
                last_line = other_lines[-1].strip()
                if last_line.endswith(','):
                    other_lines[-1] = last_line[:-1]
                # Ensure the statement ends with );
                if not other_lines[-1].strip().endswith(');'):
                    other_lines[-1] = other_lines[-1].rstrip() + ');'
                # Create the modified CREATE TABLE statement
                output_statements.append('\n'.join(other_lines))
                # Convert foreign keys to ALTER TABLE statements
                for fk in fk_constraints:
                    # Extract constraint details using regex for robustness
                    fk_clean = fk.replace('FOREIGN KEY', '').strip()
                    match = re.match(r'\((.*?)\) REFERENCES (\w+)\((.*?)\)', fk_clean)
                    if match:
                        column = match.group(1).strip()
                        ref_table = match.group(2).strip()
                        ref_column = match.group(3).strip()
                        # Create ALTER TABLE statement
                        fk_stmt = (
                            f'ALTER TABLE ONLY {table_name} '
                            f'ADD CONSTRAINT fk_{table_name}_{column} '
                            f'FOREIGN KEY ({column}) '
                            f'REFERENCES {ref_table}({ref_column});'
                        )
                        foreign_key_statements.append(fk_stmt)
            else:
                # Ensure statement ends with ; for non-FK tables
                if not stmt_str.endswith(';'):
                    stmt_str += ';'
                output_statements.append(stmt_str)
        else:
            # Keep non-CREATE TABLE statements as is
            output_statements.append(stmt_str)

    # Combine all statements, with foreign keys at the end
    final_output = '\n\n'.join(output_statements + foreign_key_statements)
    return final_output

def main():
    # Read SQL dump from stdin
    sql_content = sys.stdin.read()
    
    # Process the SQL dump
    result = process_sql_dump(sql_content)
    
    # Write to stdout
    sys.stdout.write(result)
    sys.stdout.flush()

if __name__ == '__main__':
    main()

# scripts/column_search.py

import json
from pathlib import Path
import re

def load_column_info():
    """Load column information from JSON file."""
    json_path = Path("column_info.json")
    with open(json_path, "r") as f:
        return json.load(f)

def search_columns(term: str, column_data: list) -> list:
    """
    Search for columns matching the given term.
    Matches against column name, display name, or description.
    """
    term = term.lower()
    matches = []
    
    for col in column_data:
        # Check if term matches any part of the column information
        if (term in col["column"].lower() or 
            term in col["name"].lower() or 
            term in col["description"].lower()):
            matches.append(col)
    
    return matches

def display_column_info(column):
    """Display formatted column information."""
    print("\n" + "="*50)
    print(f"Column Name: {column['column']}")
    print(f"Display Name: {column['name']}")
    print(f"Description: {column['description']}")
    print("="*50)

def main():
    # Load column information
    try:
        column_data = load_column_info()
    except FileNotFoundError:
        print("❌ Error: column_info.json not found!")
        return
    except json.JSONDecodeError:
        print("❌ Error: Invalid JSON in column_info.json!")
        return

    print("\n🔍 Compustat Column Search")
    print("========================")
    print("Search for columns by name, display name, or description.")
    print("Type 'q' to quit, 'list' to see all columns.")
    
    while True:
        # Get search term
        term = input("\nEnter search term: ").strip().lower()
        
        if term == 'q':
            print("Goodbye!")
            break
            
        if term == 'list':
            print("\nAll Available Columns:")
            print("====================")
            for col in column_data:
                display_column_info(col)
            continue
        
        # Search for matches
        matches = search_columns(term, column_data)
        
        if not matches:
            print(f"\n❌ No columns found matching '{term}'")
            continue
            
        print(f"\n✅ Found {len(matches)} matching column(s):")
        
        # Display matches
        for col in matches:
            display_column_info(col)
        
        # Show search tips
        if len(matches) > 1:
            print("\n💡 Tip: Try a more specific search term to narrow results")

if __name__ == "__main__":
    main() 
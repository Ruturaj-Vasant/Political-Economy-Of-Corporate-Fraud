import json
from pathlib import Path

# Load column info
json_path = Path("column_info.json")
with open(json_path, "r") as f:
    column_data = json.load(f)

# Convert to lookup dictionary for fast access
column_lookup = {col["column"]: col for col in column_data}

def main():
    print("Compustat Column Explorer")
    
    while True:
        col = input("\nEnter the column name you want to look up (e.g., 'roa', 'sales'): ").strip().lower()
        
        if col in column_lookup:
            entry = column_lookup[col]
            print(f"\nColumn: {entry['column']}")
            print(f"Name: {entry['name']}")
            print(f"Description: {entry['description']}")
        else:
            print("Column not found in documentation.")
        
        again = input("\nDo you want to look up another column? (y/n): ").strip().lower()
        if again != 'y':
            print("Exiting.")
            break

if __name__ == "__main__":
    main()
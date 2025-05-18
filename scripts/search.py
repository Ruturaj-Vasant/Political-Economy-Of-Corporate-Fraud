import json
from pathlib import Path

# Load column info
json_path = Path("column_info.json")
with open(json_path, "r") as f:
    column_data = json.load(f)

# Exact match lookup
column_lookup = {col["column"]: col for col in column_data}

def display_column_info(entry):
    print(f"\nColumn: {entry['column']}")
    print(f"Name: {entry['name']}")
    print(f"Description: {entry['description']}")

def search_column_info(term):
    term = term.lower()
    matches = [
        col for col in column_data
        if term in col["column"].lower() or term in col["name"].lower() or term in col["description"].lower()
    ]
    return matches

def main():
    print("Compustat Column Explorer")

    while True:
        mode = input("\nChoose mode: (1) Exact Lookup  (2) Search  (q) Quit: ").strip().lower()

        if mode == "1":
            col = input("Enter exact column name (e.g., 'roa', 'sales'): ").strip().lower()
            if col in column_lookup:
                display_column_info(column_lookup[col])
            else:
                print("Column not found.")
        
        elif mode == "2":
            term = input("Enter search term (partial or full): ").strip()
            results = search_column_info(term)
            if results:
                print(f"\nFound {len(results)} match(es):")
                for r in results:
                    display_column_info(r)
            else:
                print("No matches found.")

        elif mode == "q":
            print("Exiting.")
            break
        else:
            print("Invalid option. Please choose 1, 2, or q.")

if __name__ == "__main__":
    main()
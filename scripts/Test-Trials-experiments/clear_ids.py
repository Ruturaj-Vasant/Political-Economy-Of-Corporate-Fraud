import json
import shutil

def clear_company_ids(filepath):
    # Backup the original file
    shutil.copy(filepath, filepath + '.bak')
    print(f"Backup created at {filepath + '.bak'}")

    # Load the data
    with open(filepath, 'r') as f:
        data = json.load(f)

    count = 0
    for key, company in data.items():
        if isinstance(company, dict):
            company['permno'] = None
            company['gvkey'] = None
            if 'cik' in company:
                company['cik'] = None
            if 'cik_str' in company:
                company['cik_str'] = None
            count += 1
        else:
            print(f"Skipped key {key}: not a dict")

    # Write the updated data back
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Cleared fields for {count} companies.")

# Usage
clear_company_ids('metadata/sec_company_tickers.json')
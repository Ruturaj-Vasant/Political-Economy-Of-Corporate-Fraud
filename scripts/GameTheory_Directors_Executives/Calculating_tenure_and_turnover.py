import pandas as pd
import os
from collections import defaultdict
from datetime import datetime
from glob import glob
import duckdb

def extract_director_metrics(director_file):
    """Analyze director compensation data."""
    query = f"""
        SELECT 
            gvkey,
            year::INT AS year,
            COUNT(DISTINCT dirname) AS max_directors
        FROM '{director_file}'
        GROUP BY gvkey, year
    """
    director_counts = duckdb.query(query).to_df()

    df = pd.read_csv(director_file)
    df['year'] = df['year'].astype(int)
    df['dirname'] = df['dirname'].astype(str)

    director_dict = defaultdict(dict)
    for _, row in df.groupby(['gvkey', 'year'])['dirname'].apply(set).reset_index().iterrows():
        director_dict[row['gvkey']][row['year']] = row['dirname']

    max_directors = []
    turnover = []

    for _, row in director_counts.iterrows():
        max_directors.append({'year': int(row['year']), 'count': int(row['max_directors'])})

    for gvkey in director_dict:
        years = sorted(director_dict[gvkey].keys())
        for i in range(1, len(years)):
            y0, y1 = years[i - 1], years[i]
            prev_set, curr_set = director_dict[gvkey][y0], director_dict[gvkey][y1]
            new_count = len(curr_set - prev_set)
            departed_count = len(prev_set - curr_set)
            turnover.append({'year': y1, 'new_count': new_count, 'departed_count': departed_count})

    return {'max_directors': max_directors, 'turnover': turnover}
def extract_ceo_metrics(execucomp_file):
    """Analyze executive compensation data using ceoann only."""
    df = pd.read_csv(execucomp_file)
    ceo_df = df[df['ceoann'] == 'CEO'].copy()
    ceo_df = ceo_df[['gvkey', 'year', 'exec_fullname']].drop_duplicates()
    ceo_df = ceo_df.sort_values(by=['gvkey', 'exec_fullname', 'year'])

    ceo_info = []
    for (gvkey, name), group in ceo_df.groupby(['gvkey', 'exec_fullname']):
        group = group.sort_values('year')
        years = group['year'].tolist()

        # Detect continuous stretches of CEO years
        start_year = years[0]
        tenure = 0
        for i, year in enumerate(years):
            if i == 0 or year == years[i - 1] + 1:
                tenure += 1
            else:
                # Save previous stretch
                for j in range(i - tenure, i):
                    ceo_info.append({
                        'year': years[j],
                        'name': name,
                        'became_ceo': start_year,
                        'tenure': years[j] - start_year
                    })
                # Reset
                start_year = year
                tenure = 1

        # Final stretch
        for j in range(len(years) - tenure, len(years)):
            ceo_info.append({
                'year': years[j],
                'name': name,
                'became_ceo': start_year,
                'tenure': years[j] - start_year
            })

    return {'ceo_info': ceo_info}

def create_director_csv(results, output_path):
    """Create CSV file for director information."""
    data = []
    
    # Process max directors
    for entry in results['max_directors']:
        year = entry['year']
        max_directors = entry['count']
        
        # Find turnover info for this year
        turnover_info = next((t for t in results['turnover'] if t['year'] == year), None)
        if turnover_info:
            new_count = turnover_info['new_count']
            departed_count = turnover_info['departed_count']
        else:
            new_count = None
            departed_count = None
        
        data.append({
            'year': year,
            'max_directors': max_directors,
            'new_directors': new_count,
            'departed_directors': departed_count
        })
    
    if data:  # Only create CSV if we have data
        df = pd.DataFrame(data)
        df = df.sort_values('year')  # Ensure sorting by year
        df.to_csv(output_path, index=False)
    else:
        print(f"Warning: No director data to write to {output_path}")

def create_ceo_csv(results, output_path):
    """Create CSV file for CEO information."""
    data = []
    
    for entry in results['ceo_info']:
        data.append({
            'year': entry['year'],
            'ceo_name': entry['name'],
            'became_ceo': entry['became_ceo'],
            'tenure': entry['tenure']
        })
    
    if data:  # Only create CSV if we have data
        df = pd.DataFrame(data)
        df = df.sort_values('year')  # Ensure sorting by year
        df.to_csv(output_path, index=False)
    else:
        print(f"Warning: No CEO data to write to {output_path}")

def create_combined_csv(director_results, ceo_results, output_path):
    """Create combined CSV file with both director and CEO information."""
    data = []
    
    # Get all unique years
    years = set()
    for entry in director_results['max_directors']:
        years.add(entry['year'])
    for entry in ceo_results['ceo_info']:
        years.add(entry['year'])
    
    years = sorted(list(years))
    
    for year in years:
        # Get director info
        director_info = next((d for d in director_results['max_directors'] if d['year'] == year), None)
        turnover_info = next((t for t in director_results['turnover'] if t['year'] == year), None)
        
        # Get CEO info
        ceo_info = next((c for c in ceo_results['ceo_info'] if c['year'] == year), None)
        
        data.append({
            'year': year,
            'max_directors': director_info['count'] if director_info else None,
            'new_directors': turnover_info['new_count'] if turnover_info else None,
            'departed_directors': turnover_info['departed_count'] if turnover_info else None,
            'ceo_name': ceo_info['name'] if ceo_info else None,
            'became_ceo': ceo_info['became_ceo'] if ceo_info else None,
            'ceo_tenure': ceo_info['tenure'] if ceo_info else None
        })
    
    if data:  # Only create CSV if we have data
        df = pd.DataFrame(data)
        df = df.sort_values('year')  # Ensure sorting by year
        df.to_csv(output_path, index=False)
    else:
        print(f"Warning: No combined data to write to {output_path}")

def process_company_data(company_dir):
    """Process all data for a company and create CSV files."""
    company_code = os.path.basename(company_dir)
    
    # Find the relevant files
    director_file = None
    exec_file = None
    
    for file in os.listdir(company_dir):
        if 'director_compensation' in file:
            director_file = os.path.join(company_dir, file)
        elif 'execucomp_anncomp' in file:
            exec_file = os.path.join(company_dir, file)
    
    if director_file and exec_file:
        try:
            # Analyze data
            director_results = extract_director_metrics(director_file)
            ceo_results = extract_ceo_metrics(exec_file)
            
            # Create output files
            director_output = os.path.join(company_dir, f'{company_code}_director_analysis.csv')
            ceo_output = os.path.join(company_dir, f'{company_code}_ceo_analysis.csv')
            combined_output = os.path.join(company_dir, f'{company_code}_combined_analysis.csv')
            
            create_director_csv(director_results, director_output)
            create_ceo_csv(ceo_results, ceo_output)
            create_combined_csv(director_results, ceo_results, combined_output)
            
            print(f"Created analysis files for {company_code}")
        except Exception as e:
            print(f"Error processing {company_code}: {str(e)}")
    else:
        print(f"Skipping {company_code}: Missing required files")

def main():
    # Process all companies in the data directory
    data_dir = 'data'
    
    for company_dir in os.listdir(data_dir):
        company_path = os.path.join(data_dir, company_dir)
        if os.path.isdir(company_path):
            process_company_data(company_path)

if __name__ == "__main__":
    main()
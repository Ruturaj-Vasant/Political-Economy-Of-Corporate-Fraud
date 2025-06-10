import pandas as pd
import os
from collections import defaultdict
from datetime import datetime
import os
from glob import glob
import duckdb

def analyze_director_compensation(file_path):
    """Analyze director compensation data."""
    df = pd.read_csv(file_path)
    
    # Sort by year and dirnbr
    df = df.sort_values(['year', 'dirnbr'])
    
    # Get unique years
    years = sorted(df['year'].unique())
    
    # Initialize results
    results = {
        'max_directors': [],
        'turnover': [],
        'new_directors': [],
        'departed_directors': []
    }
    
    # Track directors by year
    directors_by_year = {}
    
    # First pass: get max directors per year
    for year in years:
        year_data = df[df['year'] == year]
        # Get the maximum dirnbr for this year
        max_dirnbr = year_data['dirnbr'].max()
        directors = set(year_data['dirname'])
        directors_by_year[year] = directors
        
        results['max_directors'].append({
            'year': year,
            'count': int(max_dirnbr)  # Convert to int to avoid decimal points
        })
    
    # Second pass: calculate turnover between years
    for i in range(1, len(years)):
        prev_year = years[i-1]
        curr_year = years[i]
        
        prev_directors = directors_by_year[prev_year]
        curr_directors = directors_by_year[curr_year]
        
        # Calculate turnover
        departed = prev_directors - curr_directors
        new = curr_directors - prev_directors
        
        results['turnover'].append({
            'year': curr_year,
            'prev_year': prev_year,
            'departed_count': len(departed),
            'new_count': len(new)
        })
        
        results['new_directors'].append({
            'year': curr_year,
            'directors': list(new)
        })
        
        results['departed_directors'].append({
            'year': curr_year,
            'directors': list(departed)
        })
    
    return results

def analyze_executive_compensation(file_path):
    """Analyze executive compensation data."""
    df = pd.read_csv(file_path)
    
    # Get CEO information
    ceo_data = df[df['title'].str.contains('CEO', na=False)]
    
    # Get unique years
    years = sorted(df['year'].unique())
    
    # Initialize results
    results = {
        'ceo_info': [],
        'annual_compensation': []
    }
    
    # Process CEO information
    for year in years:
        year_ceo = ceo_data[ceo_data['year'] == year]
        if not year_ceo.empty:
            ceo = year_ceo.iloc[0]
            
            # Calculate tenure properly
            tenure = None
            if pd.notna(ceo['becameceo']):
                try:
                    became_ceo_date = pd.to_datetime(ceo['becameceo'])
                    tenure = year - became_ceo_date.year
                    # If the date is in the future, set tenure to 0
                    if tenure < 0:
                        tenure = 0
                except:
                    tenure = None
            
            results['ceo_info'].append({
                'year': year,
                'name': ceo['exec_fullname'],
                'became_ceo': ceo['becameceo'],
                'tenure': tenure,
                'total_compensation': ceo['total_sec']
            })
    
    return results

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
            new_count = 0
            departed_count = 0
        
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
            'new_directors': turnover_info['new_count'] if turnover_info else 0,
            'departed_directors': turnover_info['departed_count'] if turnover_info else 0,
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
            director_results = analyze_director_compensation(director_file)
            ceo_results = analyze_executive_compensation(exec_file)
            
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
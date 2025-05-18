#!/usr/bin/env python3

import sys
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from modules.csv_verifier import CSVVerifier
import logging
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Compare two CSV files and generate a verification report.')
    
    # Optional arguments
    parser.add_argument('--code-file', required=False,
                      help='Path to the CSV file downloaded through code (e.g., data/AAPL/AAPL_2019_2023_comp_execucomp_anncomp.csv)')
    parser.add_argument('--manual-file', required=False,
                      help='Path to the manually downloaded CSV file (e.g., manual_downloads/AAPL_manual.csv)')
    parser.add_argument('--output-dir', default=None,
                      help='Directory to save the verification report (defaults to the directory of the code file)')
    parser.add_argument('--report-name', default=None,
                      help='Name for the verification report (defaults to verification_report.txt)')
    
    return parser.parse_args()

def get_file_paths_interactive():
    print("\n=== CSV File Verification Tool ===")
    print("\nEnter the paths to the files you want to compare:")
    print("Example: data/XRX/XRX_2019_2024_comp_director_compensation.csv")
    
    code_file = input("\nEnter path to the first file: ").strip()
    manual_file = input("Enter path to the second file: ").strip()
    
    return code_file, manual_file

def main():
    args = parse_arguments()
    
    # Get file paths either from arguments or interactively
    if args.code_file and args.manual_file:
        code_file = args.code_file
        manual_file = args.manual_file
    else:
        code_file, manual_file = get_file_paths_interactive()
    
    # Verify files exist
    if not Path(code_file).exists():
        logger.error(f"Code-downloaded file not found: {code_file}")
        sys.exit(1)
    if not Path(manual_file).exists():
        logger.error(f"Manual file not found: {manual_file}")
        sys.exit(1)

    try:
        # Perform verification
        print("\nRunning verification...")
        comparison = CSVVerifier.verify_files(code_file, manual_file)
        
        # Determine output path for report
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = Path(code_file).parent
            
        report_name = args.report_name or "verification_report.txt"
        report_path = output_dir / report_name
        
        # Generate and save report
        report = CSVVerifier.generate_report(comparison, str(report_path))
        
        # Print summary
        print("\nVerification Summary:")
        print("=" * 50)
        print(f"Files compared:")
        print(f"  1. Code-downloaded file: {code_file}")
        print(f"  2. Manual file: {manual_file}")
        print(f"\nResults saved to: {report_path}")
        
        # Print key findings
        if comparison['shape_match'] and comparison['columns_match'] and not comparison['column_differences']:
            print("\n✅ Files match perfectly!")
        else:
            print("\n⚠️  Differences found:")
            if not comparison['shape_match']:
                print(f"  - Shape mismatch")
            if not comparison['columns_match']:
                print(f"  - Column mismatch")
            if comparison['column_differences']:
                print(f"  - {len(comparison['column_differences'])} columns have differences")
        
        print("\nSee the full report for detailed information.")

    except Exception as e:
        logger.error(f"Error during verification: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
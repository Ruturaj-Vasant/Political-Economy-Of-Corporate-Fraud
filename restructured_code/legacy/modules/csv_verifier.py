import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Dict, List
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVVerifier:
    @staticmethod
    def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names by converting to lowercase and removing special characters.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            DataFrame with standardized column names
        """
        # Create a mapping of original to standardized names
        name_mapping = {col: col.lower().strip().replace(' ', '_') for col in df.columns}
        return df.rename(columns=name_mapping)

    @staticmethod
    def standardize_data_types(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize data types across the DataFrame.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            DataFrame with standardized data types
        """
        df = df.copy()
        
        # Define type conversion rules
        type_rules = {
            # Numeric columns that should be float
            'float_columns': [
                'age', 'allothpd', 'allothtot', 'bonus', 'defer_balance_tot',
                'defer_contrib_co_tot', 'defer_contrib_exec_tot', 'defer_earnings_tot',
                'defer_rpt_as_comp_tot', 'defer_withdr_tot', 'eip_unearn_val',
                'execdir', 'execrankann', 'interlock', 'ltip', 'noneq_incent',
                'old_datafmt_flag', 'opt_exer_num', 'opt_exer_val',
                'opt_unex_exer_est_val', 'opt_unex_exer_num', 'opt_unex_unexer_est_val',
                'opt_unex_unexer_num', 'option_awards', 'option_awards_blk_value',
                'option_awards_fv', 'option_awards_num', 'option_awards_rpt_value',
                'othann', 'othcomp', 'page', 'pension_pymts_tot', 'pension_value_tot',
                'reprice', 'ret_yrs', 'rstkgrnt', 'rstkvyrs', 'sal_pct', 'salary',
                'shrown_excl_opts', 'shrown_excl_opts_pct', 'shrown_tot',
                'shrown_tot_pct', 'shrs_vest_num', 'shrs_vest_val', 'sic',
                'spindex', 'stock_awards', 'stock_awards_fv', 'stock_unvest_num',
                'stock_unvest_val', 'sub_tele', 'tdc1', 'tdc1_pct', 'tdc2',
                'tdc2_pct', 'term_pymt', 'total_alt1', 'total_alt1_pct',
                'total_alt2', 'total_alt2_pct', 'total_curr', 'total_curr_pct',
                'total_sec', 'total_sec_pct', 'year'
            ],
            # Date columns
            'date_columns': [
                'becameceo', 'joined_co', 'leftco', 'leftofc'
            ],
            # String columns that should be object
            'string_columns': [
                'address', 'city', 'comment', 'coname', 'cusip', 'exchange',
                'exec_fname', 'exec_fullname', 'exec_lname', 'exec_mname',
                'gender', 'gvkey', 'inddesc', 'naicsdesc', 'nameprefix',
                'pceo', 'pcfo', 'reason', 'rejoin', 'releft', 'sicdesc',
                'spcode', 'state', 'tele', 'ticker', 'title', 'titleann', 'zip'
            ]
        }
        
        # Convert numeric columns to float
        for col in type_rules['float_columns']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert date columns
        for col in type_rules['date_columns']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert string columns
        for col in type_rules['string_columns']:
            if col in df.columns:
                df[col] = df[col].astype(str).replace('nan', np.nan)
        
        return df

    @staticmethod
    def load_csv(file_path: str) -> pd.DataFrame:
        """
        Load a CSV file and return a DataFrame.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            DataFrame containing the CSV data
        """
        try:
            df = pd.read_csv(file_path)
            # Standardize column names
            df = CSVVerifier.standardize_column_names(df)
            # Standardize data types
            df = CSVVerifier.standardize_data_types(df)
            return df
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {str(e)}")
            raise

    @staticmethod
    def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """
        Compare two DataFrames and return a dictionary of differences.
        
        Args:
            df1: First DataFrame (downloaded from code)
            df2: Second DataFrame (manually downloaded)
            
        Returns:
            Dictionary containing comparison results
        """
        # Ensure both DataFrames have the same columns
        common_columns = sorted(list(set(df1.columns) & set(df2.columns)))
        df1 = df1[common_columns]
        df2 = df2[common_columns]

        comparison = {
            'shape_match': df1.shape == df2.shape,
            'columns_match': list(df1.columns) == list(df2.columns),
            'missing_columns': list(set(df2.columns) - set(df1.columns)),
            'extra_columns': list(set(df1.columns) - set(df2.columns)),
            'row_count_diff': abs(len(df1) - len(df2)),
            'column_differences': {},
            'sample_mismatches': {}
        }

        # Compare common columns
        for col in common_columns:
            try:
                if df1[col].dtype != df2[col].dtype:
                    comparison['column_differences'][col] = {
                        'type_mismatch': True,
                        'df1_type': str(df1[col].dtype),
                        'df2_type': str(df2[col].dtype)
                    }
                else:
                    # For numeric columns, compare with tolerance
                    if pd.api.types.is_numeric_dtype(df1[col]):
                        tolerance = 1e-10
                        matches = np.isclose(df1[col], df2[col], equal_nan=True, rtol=tolerance)
                        if not matches.all():
                            mismatch_indices = np.where(~matches)[0]
                            comparison['column_differences'][col] = {
                                'mismatch_count': len(mismatch_indices),
                                'sample_mismatches': {
                                    'df1_values': df1[col].iloc[mismatch_indices[:5]].tolist(),
                                    'df2_values': df2[col].iloc[mismatch_indices[:5]].tolist()
                                }
                            }
                    # For date columns
                    elif pd.api.types.is_datetime64_any_dtype(df1[col]):
                        matches = df1[col] == df2[col]
                        if not matches.all():
                            mismatch_indices = np.where(~matches)[0]
                            comparison['column_differences'][col] = {
                                'mismatch_count': len(mismatch_indices),
                                'sample_mismatches': {
                                    'df1_values': df1[col].iloc[mismatch_indices[:5]].dt.strftime('%Y-%m-%d').tolist(),
                                    'df2_values': df2[col].iloc[mismatch_indices[:5]].dt.strftime('%Y-%m-%d').tolist()
                                }
                            }
                    # For non-numeric columns, direct comparison
                    else:
                        matches = df1[col] == df2[col]
                        if not matches.all():
                            mismatch_indices = np.where(~matches)[0]
                            comparison['column_differences'][col] = {
                                'mismatch_count': len(mismatch_indices),
                                'sample_mismatches': {
                                    'df1_values': df1[col].iloc[mismatch_indices[:5]].tolist(),
                                    'df2_values': df2[col].iloc[mismatch_indices[:5]].tolist()
                                }
                            }
            except Exception as e:
                logger.error(f"Error comparing column {col}: {str(e)}")
                comparison['column_differences'][col] = {
                    'error': str(e)
                }

        return comparison

    @staticmethod
    def verify_files(file1_path: str, file2_path: str) -> Dict:
        """
        Verify two CSV files and return detailed comparison results.
        
        Args:
            file1_path: Path to first CSV file (downloaded from code)
            file2_path: Path to second CSV file (manually downloaded)
            
        Returns:
            Dictionary containing verification results
        """
        try:
            df1 = CSVVerifier.load_csv(file1_path)
            df2 = CSVVerifier.load_csv(file2_path)
            
            # Log column information
            logger.info(f"Columns in first file: {sorted(df1.columns)}")
            logger.info(f"Columns in second file: {sorted(df2.columns)}")
            
            comparison = CSVVerifier.compare_dataframes(df1, df2)
            
            # Log results
            logger.info(f"Verification Results for {Path(file1_path).name} vs {Path(file2_path).name}:")
            logger.info(f"Shape match: {comparison['shape_match']}")
            logger.info(f"Columns match: {comparison['columns_match']}")
            logger.info(f"Row count difference: {comparison['row_count_diff']}")
            
            if comparison['missing_columns']:
                logger.warning(f"Missing columns in first file: {comparison['missing_columns']}")
            if comparison['extra_columns']:
                logger.warning(f"Extra columns in first file: {comparison['extra_columns']}")
            
            for col, diff in comparison['column_differences'].items():
                logger.warning(f"Differences in column '{col}':")
                if 'error' in diff:
                    logger.warning(f"  Error: {diff['error']}")
                else:
                    logger.warning(f"  Mismatch count: {diff.get('mismatch_count', 'N/A')}")
                    if 'sample_mismatches' in diff:
                        logger.warning("  Sample mismatches:")
                        logger.warning(f"    First file values: {diff['sample_mismatches']['df1_values']}")
                        logger.warning(f"    Second file values: {diff['sample_mismatches']['df2_values']}")
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error during verification: {str(e)}")
            raise

    @staticmethod
    def generate_report(comparison: Dict, output_path: str = None) -> str:
        """
        Generate a human-readable report from comparison results.
        Creates a new timestamped report file for each verification.
        
        Args:
            comparison: Dictionary containing comparison results
            output_path: Base path for the report (will be modified to include timestamp)
            
        Returns:
            String containing the report
        """
        # Generate timestamp for this verification
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        report = []
        report.append(f"Verification Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 50)
        
        # Basic information
        report.append(f"\nShape match: {'✓' if comparison['shape_match'] else '✗'}")
        report.append(f"Columns match: {'✓' if comparison['columns_match'] else '✗'}")
        report.append(f"Row count difference: {comparison['row_count_diff']}")
        
        # Missing/Extra columns
        if comparison['missing_columns']:
            report.append("\nMissing columns in first file:")
            for col in comparison['missing_columns']:
                report.append(f"  - {col}")
        
        if comparison['extra_columns']:
            report.append("\nExtra columns in first file:")
            for col in comparison['extra_columns']:
                report.append(f"  - {col}")
        
        # Column differences
        if comparison['column_differences']:
            report.append("\nColumn Differences:")
            for col, diff in comparison['column_differences'].items():
                report.append(f"\n{col}:")
                if 'error' in diff:
                    report.append(f"  Error: {diff['error']}")
                else:
                    if 'type_mismatch' in diff:
                        report.append(f"  Type mismatch:")
                        report.append(f"    First file: {diff['df1_type']}")
                        report.append(f"    Second file: {diff['df2_type']}")
                    if 'mismatch_count' in diff:
                        report.append(f"  Mismatch count: {diff['mismatch_count']}")
                        if 'sample_mismatches' in diff:
                            report.append("  Sample mismatches:")
                            report.append(f"    First file values: {diff['sample_mismatches']['df1_values']}")
                            report.append(f"    Second file values: {diff['sample_mismatches']['df2_values']}")
        
        report_str = "\n".join(report)
        
        if output_path:
            # Create directory if it doesn't exist
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create new filename with timestamp
            output_file = Path(output_path)
            timestamped_path = output_file.parent / f"{output_file.stem}_{timestamp}{output_file.suffix}"
            
            # Write to new timestamped file
            with open(timestamped_path, 'w') as f:
                f.write(report_str)
            
            logger.info(f"Verification report saved to: {timestamped_path}")
        
        return report_str 
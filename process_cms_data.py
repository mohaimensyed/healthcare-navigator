# process_cms_data_fixed.py - Process CMS data with encoding fix
import pandas as pd
import numpy as np

def process_cms_data_with_encoding(input_file):
    """
    Process the CMS healthcare data with proper encoding handling
    """
    print(f"Loading CMS data from {input_file}...")
    
    # Try different encodings common in CMS files
    encodings_to_try = [
        'utf-8',
        'latin-1', 
        'iso-8859-1',
        'cp1252',
        'windows-1252'
    ]
    
    df = None
    successful_encoding = None
    
    for encoding in encodings_to_try:
        try:
            print(f"Trying encoding: {encoding}")
            df = pd.read_csv(input_file, encoding=encoding)
            successful_encoding = encoding
            print(f"✅ Successfully loaded with {encoding} encoding")
            break
        except UnicodeDecodeError as e:
            print(f"❌ Failed with {encoding}: {e}")
            continue
        except Exception as e:
            print(f"❌ Error with {encoding}: {e}")
            continue
    
    if df is None:
        print("❌ Could not read file with any encoding. Trying with error handling...")
        try:
            df = pd.read_csv(input_file, encoding='utf-8', errors='ignore')
            print("✅ Loaded with error handling (some characters may be missing)")
        except Exception as e:
            print(f"❌ Final attempt failed: {e}")
            return None
    
    print(f"Loaded {len(df):,} rows")
    print(f"Columns: {list(df.columns)}")
    
    # Show first few column names to help identify the right ones
    print(f"\nFirst 10 columns: {list(df.columns)[:10]}")
    
    # Filter for NY state only - try multiple possible column names
    print("\nFiltering for NY state only...")
    
    # Common state column variations in CMS files
    possible_state_columns = [
        'Rndrng_Prvdr_State_Abrvtn', 
        'Rndrng_Prvdr_St', 
        'Provider_State',
        'State',
        'Prvdr_State_Abrvtn',
        'Prvdr_St'
    ]
    
    ny_df = None
    state_column_used = None
    
    for col in possible_state_columns:
        if col in df.columns:
            print(f"Trying state column: {col}")
            print(f"Unique values in {col}: {df[col].value_counts().head()}")
            
            ny_df = df[df[col] == 'NY'].copy()
            if len(ny_df) > 0:
                print(f"✅ Found {len(ny_df):,} NY rows using column '{col}'")
                state_column_used = col
                break
            else:
                print(f"No NY data found in {col}")
    
    if ny_df is None or len(ny_df) == 0:
        print("\n❌ No NY data found. Available columns:")
        print(df.columns.tolist())
        print("\nFirst few rows to help identify correct columns:")
        print(df.head(2))
        return None
    
    # Take sample of up to 15,000 rows
    if len(ny_df) > 15000:
        print(f"Taking random sample of 15,000 rows from {len(ny_df):,} available...")
        sample_df = ny_df.sample(n=15000, random_state=42).copy()
    else:
        print(f"Using all {len(ny_df):,} NY rows")
        sample_df = ny_df.copy()
    
    # Try to identify DRG columns
    drg_code_col = None
    drg_desc_col = None
    
    possible_drg_code_cols = ['DRG_Cd', 'DRG_Code', 'MS_DRG_Code', 'MSDRG_Code']
    possible_drg_desc_cols = ['DRG_Desc', 'DRG_Description', 'MS_DRG_Desc', 'MSDRG_Desc']
    
    for col in possible_drg_code_cols:
        if col in sample_df.columns:
            drg_code_col = col
            break
    
    for col in possible_drg_desc_cols:
        if col in sample_df.columns:
            drg_desc_col = col
            break
    
    if drg_code_col and drg_desc_col:
        print(f"Creating DRG definition from {drg_code_col} + {drg_desc_col}")
        sample_df['ms_drg_definition'] = (
            sample_df[drg_code_col].astype(str) + ' - ' + sample_df[drg_desc_col].astype(str)
        )
    else:
        print(f"Warning: Could not find DRG columns. Available columns: {list(sample_df.columns)}")
    
    # Map columns to required format - flexible mapping
    print("\nMapping columns to required format...")
    
    # Try to map columns intelligently
    column_mapping = {}
    
    # Provider ID columns
    for col in ['Rndrng_Prvdr_CCN', 'Provider_CCN', 'CCN', 'Provider_Id']:
        if col in sample_df.columns:
            column_mapping[col] = 'Provider Id'
            break
    
    # Provider Name
    for col in ['Rndrng_Prvdr_Org_Name', 'Provider_Name', 'Hospital_Name', 'Org_Name']:
        if col in sample_df.columns:
            column_mapping[col] = 'Provider Name'
            break
    
    # Provider City
    for col in ['Rndrng_Prvdr_City', 'Provider_City', 'City']:
        if col in sample_df.columns:
            column_mapping[col] = 'Provider City'
            break
    
    # Provider State
    if state_column_used:
        column_mapping[state_column_used] = 'Provider State'
    
    # Provider ZIP
    for col in ['Rndrng_Prvdr_Zip5', 'Provider_Zip', 'Zip_Code', 'ZIP']:
        if col in sample_df.columns:
            column_mapping[col] = 'Provider Zip Code'
            break
    
    # DRG Definition
    if 'ms_drg_definition' in sample_df.columns:
        column_mapping['ms_drg_definition'] = 'DRG Definition'
    
    # Total Discharges
    for col in ['Tot_Dschrgs', 'Total_Discharges', 'Discharges']:
        if col in sample_df.columns:
            column_mapping[col] = 'Total Discharges'
            break
    
    # Average Covered Charges
    for col in ['Avg_Submtd_Cvrd_Chrg', 'Avg_Covered_Charges', 'Covered_Charges']:
        if col in sample_df.columns:
            column_mapping[col] = 'Average Covered Charges'
            break
    
    # Average Total Payments
    for col in ['Avg_Tot_Pymt_Amt', 'Avg_Total_Payments', 'Total_Payments']:
        if col in sample_df.columns:
            column_mapping[col] = 'Average Total Payments'
            break
    
    # Average Medicare Payments
    for col in ['Avg_Mdcr_Pymt_Amt', 'Avg_Medicare_Payments', 'Medicare_Payments']:
        if col in sample_df.columns:
            column_mapping[col] = 'Average Medicare Payments'
            break
    
    print("Column mapping found:")
    for old_col, new_col in column_mapping.items():
        print(f"  {old_col} → {new_col}")
    
    if not column_mapping:
        print("❌ Could not map any columns. Here are all available columns:")
        for i, col in enumerate(sample_df.columns):
            print(f"  {i}: {col}")
        return None
    
    # Select and rename columns
    available_columns = [col for col in column_mapping.keys() if col in sample_df.columns]
    result_df = sample_df[available_columns].rename(columns=column_mapping)
    
    # Clean the data
    print("Cleaning data...")
    
    # Remove rows with missing critical data
    critical_cols = ['Provider Name', 'Provider City', 'Provider State', 'Provider Zip Code']
    available_critical = [col for col in critical_cols if col in result_df.columns]
    
    if available_critical:
        initial_count = len(result_df)
        result_df = result_df.dropna(subset=available_critical)
        print(f"Removed {initial_count - len(result_df)} rows with missing critical data")
    
    # Convert numeric columns
    numeric_columns = [
        'Total Discharges', 'Average Covered Charges', 
        'Average Total Payments', 'Average Medicare Payments'
    ]
    
    for col in numeric_columns:
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
    
    # Remove rows with invalid numeric data
    available_numeric = [col for col in numeric_columns if col in result_df.columns]
    if available_numeric:
        before_count = len(result_df)
        result_df = result_df.dropna(subset=available_numeric)
        print(f"Removed {before_count - len(result_df)} rows with invalid numeric data")
    
    print(f"\nFinal dataset: {len(result_df):,} rows")
    print(f"Columns in final dataset: {list(result_df.columns)}")
    
    if len(result_df) == 0:
        print("❌ No data remaining after cleaning")
        return None
    
    # Show sample statistics
    print("\nDataset Statistics:")
    if 'Provider Id' in result_df.columns:
        print(f"- Unique providers: {result_df['Provider Id'].nunique()}")
    if 'DRG Definition' in result_df.columns:
        print(f"- Unique procedures: {result_df['DRG Definition'].nunique()}")
    if 'Average Covered Charges' in result_df.columns:
        print(f"- Average charges range: ${result_df['Average Covered Charges'].min():,.0f} - ${result_df['Average Covered Charges'].max():,.0f}")
    if 'Provider City' in result_df.columns:
        print(f"- Cities: {result_df['Provider City'].nunique()} unique")
        print(f"- Top cities: {result_df['Provider City'].value_counts().head().to_dict()}")
    
    return result_df

def save_sample_data(df, output_file):
    """Save the processed data to CSV"""
    # Ensure data directory exists
    import os
    os.makedirs('data', exist_ok=True)
    
    df.to_csv(output_file, index=False)
    print(f"\n✅ Saved processed data to: {output_file}")
    
    # Show first few rows
    print("\nFirst 3 rows:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df.head(3))

if __name__ == "__main__":
    input_filename = input("Enter the name of your downloaded CMS CSV file: ").strip()
    
    try:
        processed_df = process_cms_data_with_encoding(input_filename)
        
        if processed_df is not None and len(processed_df) > 0:
            # Save to the expected location
            output_file = "data/sample_prices_ny.csv"
            save_sample_data(processed_df, output_file)
            
            print(f"\n🎉 Successfully created sample_prices_ny.csv!")
            print(f"📁 File location: {output_file}")
            print(f"📊 Rows: {len(processed_df):,}")
            if 'Provider Id' in processed_df.columns:
                print(f"🏥 Providers: {processed_df['Provider Id'].nunique()}")
            if 'DRG Definition' in processed_df.columns:
                print(f"🔬 Procedures: {processed_df['DRG Definition'].nunique()}")
            
            print("\n✅ Ready to run ETL process!")
            print("Next step: python etl.py")
        else:
            print("❌ Failed to process data - check the error messages above")
            
    except FileNotFoundError:
        print(f"❌ File '{input_filename}' not found.")
        print("Make sure the file is in the current directory or provide the full path.")
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        print("Please check the file format and try again.")
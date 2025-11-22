from pathlib import Path
import os
import pandas as pd
import json
import ast
import numpy as np
from datetime import datetime

city = "Miami"
state = "FL"

def load():
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    path = Path(os.path.join(f"{BASE_DIR}", "data", "raw", f"PROPERTY_RECORDS_{city}_{state}.csv"))
    return pd.read_csv(path)

def parse_json_field(value):
    if pd.isna(value) or value == '':
        return None
    try:
        # Try parsing as Python dict literal (using ast.literal_eval)
        return ast.literal_eval(str(value))
    except:
        try:
            # Try parsing as JSON
            return json.loads(str(value))
        except:
            return None

def extract_tax_assessment_value(tax_assessments):
    """Extract the latest tax assessment value from the JSON field."""
    if tax_assessments is None:
        return None
    
    if isinstance(tax_assessments, dict):
        # Get the latest year's value
        years = [int(k) for k in tax_assessments.keys() if k.isdigit()]
        if years:
            latest_year = max(years)
            year_data = tax_assessments.get(str(latest_year), {})
            return year_data.get('value', None)
    return None

def extract_features_from_json(features):
    """Extract useful features from the features JSON field."""
    if features is None or not isinstance(features, dict):
        return {
            'has_cooling': False,
            'has_garage': False,
            'has_pool': False,
            'unit_count': 1,
            'floor_count': None,
            'exterior_type': None,
            'roof_type': None
        }
    
    return {
        'has_cooling': features.get('cooling', False),
        'has_garage': features.get('garage', False),
        'has_pool': features.get('pool', False),
        'unit_count': features.get('unitCount', 1),
        'floor_count': features.get('floorCount', None),
        'exterior_type': features.get('exteriorType', None),
        'roof_type': features.get('roofType', None)
    }

def clean(data):
    """
    Clean the property records dataset.
    
    Steps:
    1. Drop constant/redundant columns
    2. Handle missing values
    3. Handle outliers
    4. Parse JSON fields
    5. Feature engineering
    """
    print(f"Starting with {len(data)} records and {len(data.columns)} columns")
    
    # Make a copy to avoid modifying original
    df = data.copy()
    
    # ===== STEP 1: Drop constant/redundant columns =====
    columns_to_drop = [
        'id',                    # Unique identifier
        'assessorID',            # Administrative ID
        'formattedAddress',      # Redundant with addressLine1
        'state',                 # All "FL"
        'stateFips',             # All "12"
        'county',                # All "Miami-Dade"
        'countyFips',            # All "086"
        'legalDescription',      # Long text, not predictive
        'subdivision',           # Sparse categorical
        'zoning',                # Text field, sparse
        'owner',                 # Not predictive for value
        'addressLine2',          # Mostly empty, redundant
    ]
    
    # Only drop columns that exist
    columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    df = df.drop(columns=columns_to_drop)
    print(f"Dropped {len(columns_to_drop)} redundant columns")
    
    # ===== STEP 2: Parse JSON fields =====
    print("Parsing JSON fields...")
    
    # Parse taxAssessments and extract latest value
    if 'taxAssessments' in df.columns:
        df['taxAssessments_parsed'] = df['taxAssessments'].apply(parse_json_field)
        df['latest_assessment_value'] = df['taxAssessments_parsed'].apply(extract_tax_assessment_value)
        df = df.drop(columns=['taxAssessments', 'taxAssessments_parsed'])
    
    # Parse features and extract useful fields
    if 'features' in df.columns:
        df['features_parsed'] = df['features'].apply(parse_json_field)
        features_df = pd.DataFrame(df['features_parsed'].apply(extract_features_from_json).tolist())
        df = pd.concat([df, features_df], axis=1)
        df = df.drop(columns=['features', 'features_parsed'])
    
    # Parse propertyTaxes (optional - can extract latest tax amount)
    if 'propertyTaxes' in df.columns:
        df['propertyTaxes_parsed'] = df['propertyTaxes'].apply(parse_json_field)
        # Extract latest tax amount if needed
        def extract_latest_tax(prop_taxes):
            if prop_taxes is None or not isinstance(prop_taxes, dict):
                return None
            years = [int(k) for k in prop_taxes.keys() if k.isdigit()]
            if years:
                latest_year = max(years)
                year_data = prop_taxes.get(str(latest_year), {})
                return year_data.get('total', None)
            return None
        df['latest_property_tax'] = df['propertyTaxes_parsed'].apply(extract_latest_tax)
        df = df.drop(columns=['propertyTaxes', 'propertyTaxes_parsed'])
    
    # ===== STEP 3: Handle target variable =====
    # Use latest_assessment_value as target if lastSalePrice is mostly empty
    if 'lastSalePrice' in df.columns:
        sale_price_non_null = df['lastSalePrice'].notna().sum()
        if sale_price_non_null < len(df) * 0.1:  # Less than 10% have sale prices
            print(f"Only {sale_price_non_null} records have lastSalePrice. Using latest_assessment_value as target.")
            df['target_price'] = df['latest_assessment_value']
            df = df.drop(columns=['lastSalePrice'])
        else:
            df['target_price'] = df['lastSalePrice']
            # Fill missing target with assessment value
            df['target_price'] = df['target_price'].fillna(df['latest_assessment_value'])
    else:
        df['target_price'] = df.get('latest_assessment_value', None)
    
    # ===== STEP 4: Handle missing values =====
    print("Handling missing values...")
    
    # Drop rows where critical features are all missing
    critical_features = ['bedrooms', 'bathrooms', 'squareFootage']
    critical_missing = df[critical_features].isna().all(axis=1)
    df = df[~critical_missing]
    print(f"Dropped {critical_missing.sum()} rows with all critical features missing")
    
    # Fill missing values
    # Numeric columns - fill with median
    numeric_cols = ['bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'yearBuilt', 
                    'latitude', 'longitude', 'unit_count', 'floor_count']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(df[col].median())
    
    # Boolean columns
    if 'ownerOccupied' in df.columns:
        df['ownerOccupied'] = df['ownerOccupied'].fillna(False).astype(bool)
    
    # Fill boolean feature columns
    bool_cols = ['has_cooling', 'has_garage', 'has_pool']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].fillna(False)
    
    # Fill categorical columns with mode
    categorical_cols = ['propertyType', 'exterior_type', 'roof_type']
    for col in categorical_cols:
        if col in df.columns:
            mode_value = df[col].mode()[0] if not df[col].mode().empty else None
            df[col] = df[col].fillna(mode_value)
    
    # ===== STEP 5: Handle outliers =====
    print("Handling outliers...")
    
    # Cap extreme values
    if 'bedrooms' in df.columns:
        # Cap bedrooms at 10 (very few properties have more)
        df.loc[df['bedrooms'] > 10, 'bedrooms'] = 10
        df.loc[df['bedrooms'] < 0, 'bedrooms'] = 0
    
    if 'bathrooms' in df.columns:
        # Cap bathrooms at 10
        df.loc[df['bathrooms'] > 10, 'bathrooms'] = 10
        df.loc[df['bathrooms'] < 0, 'bathrooms'] = 0
    
    if 'squareFootage' in df.columns:
        # Cap square footage at 10,000 (very large values likely errors or commercial)
        df.loc[df['squareFootage'] > 10000, 'squareFootage'] = 10000
        df.loc[df['squareFootage'] < 100, 'squareFootage'] = 100
    
    if 'lotSize' in df.columns:
        # Cap lot size at 50,000 sq ft (very large lots)
        df.loc[df['lotSize'] > 50000, 'lotSize'] = 50000
    
    if 'unit_count' in df.columns:
        # Cap unit count at 50 for single property predictions
        df.loc[df['unit_count'] > 50, 'unit_count'] = 50
    
    # ===== STEP 6: Feature engineering =====
    print("Creating derived features...")
    
    # Property age
    current_year = datetime.now().year
    if 'yearBuilt' in df.columns:
        df['property_age'] = current_year - df['yearBuilt']
        df['property_age'] = df['property_age'].clip(lower=0, upper=200)  # Cap at 200 years
    
    # Price per square foot (if target available)
    if 'target_price' in df.columns and 'squareFootage' in df.columns:
        df['price_per_sqft'] = df['target_price'] / df['squareFootage']
        df['price_per_sqft'] = df['price_per_sqft'].replace([np.inf, -np.inf], np.nan)
        # Remove extreme outliers (likely data errors)
        df.loc[df['price_per_sqft'] > 2000, 'price_per_sqft'] = np.nan
        df.loc[df['price_per_sqft'] < 10, 'price_per_sqft'] = np.nan
    
    # Lot size ratio
    if 'lotSize' in df.columns and 'squareFootage' in df.columns:
        df['lot_to_building_ratio'] = df['lotSize'] / df['squareFootage']
        df['lot_to_building_ratio'] = df['lot_to_building_ratio'].replace([np.inf, -np.inf], np.nan)
        df['lot_to_building_ratio'] = df['lot_to_building_ratio'].clip(upper=100)  # Cap extreme ratios
    
    # Bedroom to bathroom ratio
    if 'bedrooms' in df.columns and 'bathrooms' in df.columns:
        df['bed_bath_ratio'] = df['bedrooms'] / df['bathrooms']
        df['bed_bath_ratio'] = df['bed_bath_ratio'].replace([np.inf, -np.inf], np.nan)
    
    # ===== STEP 7: Final cleanup =====
    # Drop rows where target is missing
    if 'target_price' in df.columns:
        initial_len = len(df)
        df = df[df['target_price'].notna()]
        df = df[df['target_price'] > 0]  # Remove zero or negative prices
        print(f"Dropped {initial_len - len(df)} rows with missing or invalid target prices")
    
    # Drop hoa if mostly empty
    if 'hoa' in df.columns:
        if df['hoa'].notna().sum() < len(df) * 0.05:  # Less than 5% have HOA data
            df = df.drop(columns=['hoa'])
            print("Dropped 'hoa' column (mostly empty)")
    
    # Reset index
    df = df.reset_index(drop=True)
    
    print(f"\nCleaning complete!")
    print(f"Final dataset: {len(df)} records and {len(df.columns)} columns")
    print(f"\nColumn summary:")
    print(f"  - Missing values per column:")
    missing_counts = df.isnull().sum()
    for col, count in missing_counts[missing_counts > 0].items():
        print(f"    {col}: {count} ({count/len(df)*100:.1f}%)")
    
    return df

def save_cleaned_data(data, filename=None):
    """Save cleaned data to processed folder."""
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    if filename is None:
        filename = f"PROPERTY_RECORDS_{city}_{state}_cleaned.csv"
    output_path = Path(os.path.join(f"{BASE_DIR}", "data", "processed", filename))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(output_path, index=False)
    print(f"\nCleaned data saved to: {output_path}")
    return output_path

def main():
    data = load()
    print(f"Loaded {len(data)} records")
    data = clean(data)
    save_cleaned_data(data)

if __name__ == "__main__":
    main()
import pandas as pd
import json
from datetime import datetime
import pytz

def parse_date(date_value):
  
    try:
        if isinstance(date_value, dict):
            if '$date' in date_value:
                return pd.to_datetime(date_value['$date']).tz_localize(None)
        elif isinstance(date_value, str):
            return pd.to_datetime(date_value).tz_localize(None)
        elif pd.isna(date_value):
            return pd.NaT
    except Exception as e:
        print(f"Error parsing date value: {date_value}, Error: {str(e)}")
        return pd.NaT
    return pd.NaT

def preprocess_dataframe(df):
   
    date_columns = [
        'createDate', 'dateScanned', 'finishedDate', 
        'pointsAwardedDate', 'purchaseDate', 'modifyDate',
        'createdDate', 'lastLogin'
    ]
    
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_date)
    
    return df

def analyze_data_quality(receipts_df, users_df, brands_df):
    issues = []
    

    receipts_df = preprocess_dataframe(receipts_df)
    users_df = preprocess_dataframe(users_df)
    

    for df, name in [(receipts_df, 'receipts'), (users_df, 'users'), (brands_df, 'brands')]:
        null_counts = df.isnull().sum()
        null_fields = null_counts[null_counts > 0]
        if not null_fields.empty:
            issues.append(f"Found null values in {name} dataset:")
            for field, count in null_fields.items():
                issues.append(f"  - {field}: {count} null values")
    

    date_columns = ['createDate', 'dateScanned', 'finishedDate', 'pointsAwardedDate', 'purchaseDate']
    current_time = pd.Timestamp.now().tz_localize(None)
    
    for col in date_columns:
        if col in receipts_df.columns:
            valid_dates = receipts_df[receipts_df[col].notna()]
            if not valid_dates.empty:
                future_dates = valid_dates[valid_dates[col] > current_time]
                if not future_dates.empty:
                    issues.append(f"Found {len(future_dates)} future dates in {col}")
                    sample_dates = future_dates[col].head(3)
                    issues.append(f"  Sample dates: {', '.join(str(d) for d in sample_dates)}")
    

    if all(col in receipts_df.columns for col in ['dateScanned', 'finishedDate']):
        invalid_sequence = receipts_df[
            (receipts_df['dateScanned'].notna()) & 
            (receipts_df['finishedDate'].notna()) &
            (receipts_df['dateScanned'] > receipts_df['finishedDate'])
        ]
        if not invalid_sequence.empty:
            issues.append(f"Found {len(invalid_sequence)} receipts where scan date is after finish date")
    

    if 'userId' in receipts_df.columns and '_id' in users_df.columns:
        orphaned_receipts = receipts_df[~receipts_df['userId'].isin(users_df['_id'])]
        if not orphaned_receipts.empty:
            issues.append(f"Found {len(orphaned_receipts)} receipts with no matching user")
    
    numeric_columns = {
        'pointsEarned': 'points',
        'purchasedItemCount': 'items',
        'totalSpent': 'amount spent'
    }
    
    for col, description in numeric_columns.items():
        if col in receipts_df.columns:
            try:
               
                numeric_values = pd.to_numeric(receipts_df[col], errors='coerce')
                negative_values = receipts_df[numeric_values < 0]
                if not negative_values.empty:
                    issues.append(f"Found {len(negative_values)} negative values for {description}")
                   
                    sample_values = negative_values[col].head(3)
                    issues.append(f"  Sample values: {', '.join(str(v) for v in sample_values)}")
            except Exception as e:
                issues.append(f"Error processing {col}: {str(e)}")
    

    for df, name, key in [
        (receipts_df, 'receipts', '_id'),
        (users_df, 'users', '_id'),
        (brands_df, 'brands', '_id')
    ]:
        if key in df.columns:
            duplicates = df[df[key].duplicated()]
            if not duplicates.empty:
                issues.append(f"Found {len(duplicates)} duplicate {name} records")
    
    if 'rewardsReceiptStatus' in receipts_df.columns:
        status_counts = receipts_df['rewardsReceiptStatus'].value_counts()
        issues.append("Receipt status distribution:")
        for status, count in status_counts.items():
            issues.append(f"  - {status}: {count} receipts")
    
    
    if 'totalSpent' in receipts_df.columns and 'purchasedItemCount' in receipts_df.columns:
        zero_spent = receipts_df[
            (receipts_df['totalSpent'] == 0) & 
            (receipts_df['purchasedItemCount'] > 0)
        ]
        if not zero_spent.empty:
            issues.append(f"Found {len(zero_spent)} receipts with items but zero total spent")
    
    return issues

def main():
    try:
    
        print("Loading data files...")
        
        try:
            with open('receipts.json') as f:
                receipts_df = pd.DataFrame([json.loads(line) for line in f])
            print("Successfully loaded receipts.json")
        except Exception as e:
            print(f"Error loading receipts.json: {str(e)}")
            receipts_df = pd.DataFrame()
            
        try:
            with open('users.json') as f:
                users_df = pd.DataFrame([json.loads(line) for line in f])
            print("Successfully loaded users.json")
        except Exception as e:
            print(f"Error loading users.json: {str(e)}")
            users_df = pd.DataFrame()
            
        try:
            with open('brands.json') as f:
                brands_df = pd.DataFrame([json.loads(line) for line in f])
            print("Successfully loaded brands.json")
        except Exception as e:
            print(f"Error loading brands.json: {str(e)}")
            brands_df = pd.DataFrame()
        
      
        print("\nAnalyzing data quality...")
        issues = analyze_data_quality(receipts_df, users_df, brands_df)
        
  
        print("\nData Quality Analysis Results:")
        for issue in issues:
            print(f"- {issue}")
            
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        print("Stack trace:")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
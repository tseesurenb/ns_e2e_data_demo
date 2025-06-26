# export_data.py
import sqlite3
import pandas as pd
from datetime import datetime
import os

def export_loan_data():
    """Export loan applications data to CSV for Starburst Cloud upload"""
    
    # Check if database exists
    if not os.path.exists('loan_applications.db'):
        print("❌ Database file 'loan_applications.db' not found!")
        print("Please run loan_generator.py first to create some data.")
        return None, None
    
    # Connect to SQLite database
    conn = sqlite3.connect('loan_applications.db')
    
    # Export all data to CSV
    df = pd.read_sql_query("SELECT * FROM loan_applications", conn)
    
    if len(df) == 0:
        print("❌ No data found in database!")
        print("Please generate some data first using loan_generator.py")
        conn.close()
        return None, None
    
    # Add ingestion_date column for partitioning
    df['ingestion_date'] = pd.to_datetime(df['application_timestamp']).dt.date
    
    # Save to CSV
    csv_filename = f"loan_applications_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_filename, index=False)
    
    conn.close()
    
    print(f"✅ Exported {len(df)} records to {csv_filename}")
    print(f"📊 Fraud Rate: {(df['is_fraud'].sum() / len(df) * 100):.1f}%")
    print(f"📁 File size: {os.path.getsize(csv_filename) / 1024:.1f} KB")
    
    # Show sample of data
    print("\n📋 Sample data:")
    print(df[['loan_id', 'customer_id', 'loan_amount', 'credit_score', 'is_fraud']].head())
    
    # Show data distribution
    print(f"\n📈 Data distribution:")
    print(f"Normal applications: {len(df[df['is_fraud'] == 0])}")
    print(f"Fraud applications: {len(df[df['is_fraud'] == 1])}")
    print(f"Date range: {df['ingestion_date'].min()} to {df['ingestion_date'].max()}")
    
    return csv_filename, df

def generate_insert_statements(df, batch_size=10):
    """Generate SQL INSERT statements for small datasets"""
    if df is None or len(df) == 0:
        return
    
    print(f"\n📝 Generating INSERT statements (first {min(batch_size, len(df))} records):")
    print("Copy and paste these into Starburst Cloud query editor:\n")
    
    for i, row in df.head(batch_size).iterrows():
        # Format the INSERT statement
        insert_sql = f"""INSERT INTO loan_applications VALUES (
    '{row['loan_id']}', '{row['customer_id']}', 
    TIMESTAMP '{row['application_timestamp']}',
    {row['loan_amount']}, {row['customer_age']}, {row['credit_score']}, 
    {row['annual_income']}, {row['employment_length']}, {row['debt_to_income']}, 
    {row['num_previous_loans']}, '{row['device_fingerprint']}', 
    '{row['ip_address']}', '{row['application_channel']}', {row['is_fraud']},
    TIMESTAMP '{row['created_at']}', TIMESTAMP '{row['updated_at']}',
    DATE '{row['ingestion_date']}'
);"""
        print(insert_sql)

if __name__ == "__main__":
    print("🔄 Exporting loan applications data for Starburst Cloud...")
    csv_file, df = export_loan_data()
    
    if csv_file:
        print(f"\n📤 Next steps:")
        print(f"1. You have the CSV file: {csv_file}")
        print(f"2. Set up your Iceberg catalog in Starburst Cloud")
        print(f"3. Create tables using the SQL from Step 2 guide")
        print(f"4. Upload this CSV file or use the INSERT statements below")
        
        # Generate some INSERT statements for testing
        generate_insert_statements(df, 5)
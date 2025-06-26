# Step 1: Database Setup and Loan Application Generator
# This script creates SQLite database and generates realistic loan applications

import sqlite3
import random
import time
from datetime import datetime, timedelta
import json
import uuid
from faker import Faker
import threading
import pandas as pd

class LoanApplicationGenerator:
    def __init__(self, db_path="loan_applications.db"):
        self.db_path = db_path
        self.fake = Faker()
        self.setup_database()
        self.device_fingerprints = [f"DEV_{i:04d}" for i in range(1, 1000)]
        self.suspicious_ips = ["10.0.0." + str(i) for i in range(1, 50)]
        self.normal_ips = ["192.168.1." + str(i) for i in range(1, 255)]
        
    def setup_database(self):
        """Create the database and loan_applications table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Drop table if exists (for fresh start)
        cursor.execute("DROP TABLE IF EXISTS loan_applications")
        
        # Create loan applications table
        cursor.execute('''
            CREATE TABLE loan_applications (
                loan_id TEXT PRIMARY KEY,
                customer_id TEXT,
                application_timestamp TEXT,
                loan_amount REAL,
                customer_age INTEGER,
                credit_score INTEGER,
                annual_income REAL,
                employment_length REAL,
                debt_to_income REAL,
                num_previous_loans INTEGER,
                device_fingerprint TEXT,
                ip_address TEXT,
                application_channel TEXT,
                is_fraud INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        
        # Create index for faster queries
        cursor.execute("CREATE INDEX idx_timestamp ON loan_applications(application_timestamp)")
        cursor.execute("CREATE INDEX idx_customer ON loan_applications(customer_id)")
        
        conn.commit()
        conn.close()
        print(f"✅ Database created at: {self.db_path}")
    
    def generate_customer_profile(self):
        """Generate a customer profile that influences fraud likelihood"""
        # 20% chance of suspicious customer
        is_suspicious = random.random() < 0.2
        
        if is_suspicious:
            # Suspicious profile
            age = random.randint(18, 30)  # Younger applicants more risky
            credit_score = random.randint(300, 550)  # Poor credit
            income = random.randint(15000, 35000)  # Low income
            employment_length = random.uniform(0, 2)  # Short employment
            debt_to_income = random.uniform(0.6, 1.2)  # High debt ratio
            num_previous_loans = random.randint(3, 10)  # Many previous loans
            ip_address = random.choice(self.suspicious_ips)
            device_fingerprint = random.choice(self.device_fingerprints[:100])  # Reused devices
            
        else:
            # Normal profile
            age = random.randint(25, 65)
            credit_score = random.randint(600, 850)
            income = random.randint(40000, 120000)
            employment_length = random.uniform(1, 15)
            debt_to_income = random.uniform(0.1, 0.5)
            num_previous_loans = random.randint(0, 3)
            ip_address = random.choice(self.normal_ips)
            device_fingerprint = random.choice(self.device_fingerprints)
        
        # Loan amount based on income (suspicious customers ask for more)
        if is_suspicious:
            loan_amount = random.randint(int(income * 0.8), int(income * 2.5))
        else:
            loan_amount = random.randint(int(income * 0.1), int(income * 0.6))
        
        return {
            'customer_age': age,
            'credit_score': credit_score,
            'annual_income': income,
            'employment_length': round(employment_length, 1),
            'debt_to_income': round(debt_to_income, 3),
            'num_previous_loans': num_previous_loans,
            'loan_amount': loan_amount,
            'ip_address': ip_address,
            'device_fingerprint': device_fingerprint,
            'is_fraud': 1 if is_suspicious else 0
        }
    
    def generate_loan_application(self):
        """Generate a single loan application"""
        profile = self.generate_customer_profile()
        
        application = {
            'loan_id': f"LOAN_{uuid.uuid4().hex[:8].upper()}",
            'customer_id': f"CUST_{uuid.uuid4().hex[:8].upper()}",
            'application_timestamp': datetime.now().isoformat(),
            'loan_amount': profile['loan_amount'],
            'customer_age': profile['customer_age'],
            'credit_score': profile['credit_score'],
            'annual_income': profile['annual_income'],
            'employment_length': profile['employment_length'],
            'debt_to_income': profile['debt_to_income'],
            'num_previous_loans': profile['num_previous_loans'],
            'device_fingerprint': profile['device_fingerprint'],
            'ip_address': profile['ip_address'],
            'application_channel': random.choice(['mobile_app', 'web', 'call_center']),
            'is_fraud': profile['is_fraud'],
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        return application
    
    def insert_application(self, application):
        """Insert loan application into database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO loan_applications VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        ''', (
            application['loan_id'],
            application['customer_id'],
            application['application_timestamp'],
            application['loan_amount'],
            application['customer_age'],
            application['credit_score'],
            application['annual_income'],
            application['employment_length'],
            application['debt_to_income'],
            application['num_previous_loans'],
            application['device_fingerprint'],
            application['ip_address'],
            application['application_channel'],
            application['is_fraud'],
            application['created_at'],
            application['updated_at']
        ))
        
        conn.commit()
        conn.close()
        
        # Print application details
        fraud_status = "🚨 FRAUD" if application['is_fraud'] else "✅ NORMAL"
        print(f"{datetime.now().strftime('%H:%M:%S')} | {fraud_status} | "
              f"Loan: ${application['loan_amount']:,} | "
              f"Credit: {application['credit_score']} | "
              f"Customer: {application['customer_id']}")
    
    def generate_historical_data(self, num_records=1000):
        """Generate historical training data"""
        print(f"🔄 Generating {num_records} historical loan applications...")
        
        conn = sqlite3.connect(self.db_path)
        applications = []
        
        for i in range(num_records):
            app = self.generate_loan_application()
            # Make historical timestamps
            days_ago = random.randint(1, 90)
            historical_time = datetime.now() - timedelta(days=days_ago)
            app['application_timestamp'] = historical_time.isoformat()
            app['created_at'] = historical_time.isoformat()
            app['updated_at'] = historical_time.isoformat()
            
            applications.append(tuple(app.values()))
            
            if (i + 1) % 100 == 0:
                print(f"Generated {i + 1}/{num_records} records...")
        
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT INTO loan_applications VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        ''', applications)
        
        conn.commit()
        conn.close()
        
        print(f"✅ Generated {num_records} historical records")
        self.show_fraud_stats()
    
    def show_fraud_stats(self):
        """Show current fraud statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM loan_applications")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM loan_applications WHERE is_fraud = 1")
        fraud_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(loan_amount) FROM loan_applications WHERE is_fraud = 1")
        avg_fraud_amount = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT AVG(loan_amount) FROM loan_applications WHERE is_fraud = 0")
        avg_normal_amount = cursor.fetchone()[0] or 0
        
        conn.close()
        
        fraud_rate = (fraud_count / total * 100) if total > 0 else 0
        
        print("\n📊 DATABASE STATISTICS:")
        print(f"Total Applications: {total}")
        print(f"Fraud Applications: {fraud_count}")
        print(f"Fraud Rate: {fraud_rate:.1f}%")
        print(f"Avg Fraud Amount: ${avg_fraud_amount:,.0f}")
        print(f"Avg Normal Amount: ${avg_normal_amount:,.0f}")
        print("-" * 50)
    
    def start_real_time_generation(self, interval_seconds=5):
        """Start generating loan applications in real-time"""
        print(f"\n🚀 Starting real-time loan application generation (every {interval_seconds} seconds)")
        print("Press Ctrl+C to stop...\n")
        
        try:
            while True:
                application = self.generate_loan_application()
                self.insert_application(application)
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n🛑 Stopped real-time generation")
            self.show_fraud_stats()
    
    def query_recent_applications(self, hours=1):
        """Query recent applications for testing"""
        conn = sqlite3.connect(self.db_path)
        
        # Get applications from last N hours
        since_time = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        df = pd.read_sql_query('''
            SELECT * FROM loan_applications 
            WHERE application_timestamp >= ? 
            ORDER BY application_timestamp DESC
        ''', conn, params=(since_time,))
        
        conn.close()
        return df

def main():
    """Main function to setup and run the loan application generator"""
    generator = LoanApplicationGenerator()
    
    print("🏦 Loan Application Database Generator")
    print("=" * 50)
    
    # Ask user what they want to do
    while True:
        print("\nOptions:")
        print("1. Generate historical training data")
        print("2. Start real-time application generation")
        print("3. Show current statistics")
        print("4. Query recent applications")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            num_records = input("Number of historical records (default 1000): ").strip()
            num_records = int(num_records) if num_records else 1000
            generator.generate_historical_data(num_records)
            
        elif choice == "2":
            interval = input("Generation interval in seconds (default 5): ").strip()
            interval = int(interval) if interval else 5
            generator.start_real_time_generation(interval)
            
        elif choice == "3":
            generator.show_fraud_stats()
            
        elif choice == "4":
            hours = input("Show applications from last N hours (default 1): ").strip()
            hours = int(hours) if hours else 1
            df = generator.query_recent_applications(hours)
            print(f"\n📋 Applications from last {hours} hour(s):")
            if len(df) > 0:
                print(df[['loan_id', 'customer_id', 'loan_amount', 'credit_score', 'is_fraud']].to_string(index=False))
            else:
                print("No applications found")
                
        elif choice == "5":
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
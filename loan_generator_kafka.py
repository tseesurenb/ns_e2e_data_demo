# loan_generator_kafka.py
import sqlite3
import random
import time
import json
from datetime import datetime, timedelta
import uuid
from faker import Faker
from confluent_kafka import Producer
import threading

class KafkaLoanGenerator:
    def __init__(self, db_path="loan_applications.db"):
        self.db_path = db_path
        self.fake = Faker()
        self.setup_database()
        
        # Kafka configuration - UPDATE THESE WITH YOUR CONFLUENT CLOUD VALUES
        self.kafka_config = {
            'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',  # e.g., 'pkc-xxxxx.us-west-2.aws.confluent.cloud:9092'
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': 'UNQA65VNTX7PQQMB',
            'sasl.password': 't523NKLTQbrQmijZgdKqpXzou/jDyMSC7H3jKpDBuIQr//v04MHLRkr3Ucn/ow7k',
        }
        
        self.producer = None
        self.topic_name = 'loan-applications'
        
        # Device and IP pools for fraud patterns
        self.device_fingerprints = [f"DEV_{i:04d}" for i in range(1, 1000)]
        self.suspicious_ips = ["10.0.0." + str(i) for i in range(1, 50)]
        self.normal_ips = ["192.168.1." + str(i) for i in range(1, 255)]
    
    def setup_kafka_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = Producer(self.kafka_config)
            print("✅ Kafka producer initialized")
            return True
        except Exception as e:
            print(f"❌ Failed to initialize Kafka producer: {e}")
            print("Please check your Confluent Cloud configuration")
            return False
    
    def setup_database(self):
        """Create the local database for backup/reference"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS loan_applications")
        
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
                updated_at TEXT,
                kafka_sent BOOLEAN DEFAULT 0
            )
        ''')
        
        cursor.execute("CREATE INDEX idx_timestamp ON loan_applications(application_timestamp)")
        cursor.execute("CREATE INDEX idx_kafka_sent ON loan_applications(kafka_sent)")
        
        conn.commit()
        conn.close()
        print(f"✅ Database created at: {self.db_path}")
    
    def generate_customer_profile(self):
        """Generate a customer profile with fraud patterns"""
        is_suspicious = random.random() < 0.2  # 20% fraud rate
        
        if is_suspicious:
            # High-risk profile
            age = random.randint(18, 30)
            credit_score = random.randint(300, 550)
            income = random.randint(15000, 35000)
            employment_length = random.uniform(0, 2)
            debt_to_income = random.uniform(0.6, 1.2)
            num_previous_loans = random.randint(3, 10)
            ip_address = random.choice(self.suspicious_ips)
            device_fingerprint = random.choice(self.device_fingerprints[:100])  # Reused devices
            loan_amount = random.randint(int(income * 0.8), int(income * 2.5))  # Asking for too much
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
    
    def delivery_report(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            print(f'❌ Message delivery failed: {err}')
        else:
            print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def send_to_kafka(self, application):
        """Send application to Kafka topic"""
        try:
            # Create message with key (customer_id) for partitioning
            key = application['customer_id']
            value = json.dumps(application)
            
            # Send to Kafka
            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=value,
                callback=self.delivery_report
            )
            
            # Trigger delivery
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to send to Kafka: {e}")
            return False
    
    def store_locally(self, application, kafka_sent=False):
        """Store application in local SQLite for backup"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO loan_applications VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
            application['updated_at'],
            kafka_sent
        ))
        
        conn.commit()
        conn.close()
    
    def generate_and_stream(self, interval_seconds=5):
        """Generate and stream loan applications to Kafka"""
        
        if not self.setup_kafka_producer():
            print("❌ Cannot start streaming without Kafka connection")
            return
        
        print(f"\n🚀 Starting real-time loan application streaming to Kafka")
        print(f"📡 Topic: {self.topic_name}")
        print(f"⏰ Interval: {interval_seconds} seconds")
        print("Press Ctrl+C to stop...\n")
        
        applications_sent = 0
        fraud_detected = 0
        
        try:
            while True:
                # Generate application
                application = self.generate_loan_application()
                
                # Send to Kafka
                kafka_success = self.send_to_kafka(application)
                
                # Store locally regardless
                self.store_locally(application, kafka_success)
                
                # Update counters
                applications_sent += 1
                if application['is_fraud']:
                    fraud_detected += 1
                
                # Print status
                fraud_status = "🚨 FRAUD" if application['is_fraud'] else "✅ NORMAL"
                kafka_status = "📡 SENT" if kafka_success else "❌ FAILED"
                
                print(f"{datetime.now().strftime('%H:%M:%S')} | {fraud_status} | {kafka_status} | "
                      f"Loan: ${application['loan_amount']:,} | "
                      f"Credit: {application['credit_score']} | "
                      f"Customer: {application['customer_id']}")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print(f"\n🛑 Streaming stopped")
            print(f"📊 Total applications sent: {applications_sent}")
            print(f"🚨 Fraud applications: {fraud_detected}")
            print(f"📈 Fraud rate: {(fraud_detected/applications_sent*100):.1f}%")
            
            # Flush any remaining messages
            self.producer.flush()
    
    def batch_send_historical_data(self, num_records=1000):
        """Send historical data to Kafka in batches"""
        
        if not self.setup_kafka_producer():
            return
            
        print(f"📦 Sending {num_records} historical records to Kafka...")
        
        sent_count = 0
        fraud_count = 0
        
        for i in range(num_records):
            # Generate historical application
            app = self.generate_loan_application()
            
            # Make it historical
            days_ago = random.randint(1, 90)
            historical_time = datetime.now() - timedelta(days=days_ago)
            app['application_timestamp'] = historical_time.isoformat()
            app['created_at'] = historical_time.isoformat()
            app['updated_at'] = historical_time.isoformat()
            
            # Send to Kafka
            if self.send_to_kafka(app):
                sent_count += 1
                
            if app['is_fraud']:
                fraud_count += 1
            
            # Store locally
            self.store_locally(app, True)
            
            if (i + 1) % 100 == 0:
                print(f"Sent {i + 1}/{num_records} records...")
                self.producer.flush()  # Ensure delivery
        
        # Final flush
        self.producer.flush()
        
        print(f"✅ Batch complete: {sent_count}/{num_records} sent successfully")
        print(f"🚨 Fraud records: {fraud_count} ({fraud_count/num_records*100:.1f}%)")

def main():
    print("🏦 Kafka Loan Application Streaming Generator")
    print("=" * 60)
    print("⚠️  IMPORTANT: Update your Confluent Cloud credentials in the code!")
    print("=" * 60)
    
    generator = KafkaLoanGenerator()
    
    while True:
        print("\nOptions:")
        print("1. Send historical data batch to Kafka")
        print("2. Start real-time streaming to Kafka")
        print("3. Test Kafka connection")
        print("4. Show local database stats")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            num_records = input("Number of historical records (default 500): ").strip()
            num_records = int(num_records) if num_records else 500
            generator.batch_send_historical_data(num_records)
            
        elif choice == "2":
            interval = input("Streaming interval in seconds (default 5): ").strip()
            interval = int(interval) if interval else 5
            generator.generate_and_stream(interval)
            
        elif choice == "3":
            print("🔍 Testing Kafka connection...")
            if generator.setup_kafka_producer():
                print("✅ Kafka connection successful!")
            else:
                print("❌ Kafka connection failed!")
                
        elif choice == "4":
            conn = sqlite3.connect(generator.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM loan_applications")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM loan_applications WHERE kafka_sent = 1")
            sent = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM loan_applications WHERE is_fraud = 1")
            fraud = cursor.fetchone()[0]
            conn.close()
            
            print(f"\n📊 Local Database Stats:")
            print(f"Total records: {total}")
            print(f"Sent to Kafka: {sent}")
            print(f"Fraud records: {fraud}")
            if total > 0:
                print(f"Fraud rate: {fraud/total*100:.1f}%")
                print(f"Kafka success rate: {sent/total*100:.1f}%")
            
        elif choice == "5":
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
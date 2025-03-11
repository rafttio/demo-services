import os
import psycopg2
import redis
import boto3
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

class DataConnector:
    def __init__(self):
        # Initialize connections
        self.postgres_conn = None
        self.redis_client = None
        self.s3_client = None
        
    def connect_postgres(self):
        """Connect to PostgreSQL database"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                database=os.getenv("POSTGRES_DB", "postgres"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                connect_timeout=3
            )
            print("Successfully connected to PostgreSQL!")
            return True
        except Exception as e:
            print(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def connect_redis(self):
        """Connect to Redis cache"""
        try:
            self.redis_client = redis.Redis(
                host=os.getenv("CACHE_HOST", "localhost"),
                port=int(os.getenv("CACHE_PORT", "6379")),
                db=int(os.getenv("CACHE_DB_NAME", "0")),
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            print("Successfully connected to Redis!")
            return True
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            return False
    
    def connect_s3(self):
        """Connect to AWS S3"""
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=os.getenv("AWS_REGION", "eu-central-1"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            # Test connection by listing buckets
            response = self.s3_client.list_buckets()
            print("Successfully connected to S3!")
            print(f"Available buckets: {[bucket['Name'] for bucket in response['Buckets']]}")
            return True
        except Exception as e:
            print(f"Failed to connect to S3: {e}")
            return False
    
    def execute_postgres_query(self, query, params=None):
        """Execute a query on PostgreSQL"""
        if not self.postgres_conn:
            print("PostgreSQL connection not established")
            return None
        
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute(query, params or ())
            if query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                self.postgres_conn.commit()
                cursor.close()
                return True
        except Exception as e:
            print(f"Error executing PostgreSQL query: {e}")
            return None
    
    def set_redis_value(self, key, value, expiry=None):
        """Set a value in Redis cache"""
        if not self.redis_client:
            print("Redis connection not established")
            return False
        
        try:
            self.redis_client.set(key, value, ex=expiry)
            return True
        except Exception as e:
            print(f"Error setting Redis value: {e}")
            return False
    
    def get_redis_value(self, key):
        """Get a value from Redis cache"""
        if not self.redis_client:
            print("Redis connection not established")
            return None
        
        try:
            return self.redis_client.get(key)
        except Exception as e:
            print(f"Error getting Redis value: {e}")
            return None
    
    def upload_to_s3(self, file_path, bucket_name, object_name=None):
        """Upload a file to S3 bucket"""
        if not self.s3_client:
            print("S3 connection not established")
            return False
        
        # If object_name not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_path)
        
        try:
            self.s3_client.upload_file(file_path, bucket_name, object_name)
            print(f"File {file_path} uploaded to {bucket_name}/{object_name}")
            return True
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False
    
    def download_from_s3(self, bucket_name, object_name, file_path):
        """Download a file from S3 bucket"""
        if not self.s3_client:
            print("S3 connection not established")
            return False
        
        try:
            self.s3_client.download_file(bucket_name, object_name, file_path)
            print(f"File {bucket_name}/{object_name} downloaded to {file_path}")
            return True
        except Exception as e:
            print(f"Error downloading from S3: {e}")
            return False
    
    def close_connections(self):
        """Close all connections"""
        if self.postgres_conn:
            self.postgres_conn.close()
            print("PostgreSQL connection closed")
        
        if self.redis_client:
            self.redis_client.close()
            print("Redis connection closed")
        
        # S3 client uses boto3 sessions which don't need explicit closing
        print("All connections closed")


def main():
    while True:
        """Main function to demonstrate usage"""
        # Create .env file first or set environment variables
        connector = DataConnector()
        
        # Connect to all services
        pg_status = connector.connect_postgres()
        redis_status = connector.connect_redis()
        s3_status = connector.connect_s3()
        
        if pg_status:
            # Example: Create a table
            connector.execute_postgres_query("""
                CREATE TABLE IF NOT EXISTS examples (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Example: Insert data
            connector.execute_postgres_query(
                "INSERT INTO examples (name) VALUES (%s)", ("test_item",)
            )
            
            # Example: Query data
            results = connector.execute_postgres_query("SELECT * FROM examples")
            print(f"PostgreSQL query results: {results}")
        
        if redis_status:
            # Example: Set and get cache
            connector.set_redis_value("example_key", "example_value", expiry=3600)
            value = connector.get_redis_value("example_key")
            print(f"Redis cached value: {value}")
        
        if s3_status:
            # Example: Create a test file and upload to S3
            # Note: You need to create or specify an existing bucket
            bucket_name = os.getenv("S3_BUCKET_NAME", "example-bucket")
            
            with open("test_file.txt", "w") as f:
                f.write("This is a test file for S3 upload")
            
            connector.upload_to_s3("test_file.txt", bucket_name)
            
            # Example: Download from S3
            connector.download_from_s3(bucket_name, "test_file.txt", "downloaded_test_file.txt")
        
        # Close all connections
        connector.close_connections()
        print("---------------------------------------------------------------------------------------")
        time.sleep(3)

if __name__ == '__main__':
    main()
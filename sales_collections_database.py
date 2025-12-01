#!/usr/bin/env python3
"""
Enhanced Sales and Collections Database Manager
Handles both sales data and payment collection data with date-wise and dealer-wise organization
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date
import os

class SalesCollectionsDatabase:
    def __init__(self, db_path="sales_collections_data.db"):
        """Initialize database connection and create tables"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
    
    def create_tables(self):
        """Create necessary tables for sales and collections data"""
        cursor = self.conn.cursor()
        
        # Create sales table (existing structure)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_date DATE NOT NULL,
                dealer_code INTEGER NOT NULL,
                dealer_name TEXT NOT NULL,
                ppc_quantity REAL DEFAULT 0,
                premium_quantity REAL DEFAULT 0,
                opc_quantity REAL DEFAULT 0,
                total_quantity REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sale_date, dealer_code)
            )
        ''')
        
        # Create collections table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collections_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_date DATE NOT NULL,
                dealer_code INTEGER NOT NULL,
                dealer_name TEXT NOT NULL,
                amount REAL NOT NULL,
                district_name TEXT,
                collection_type TEXT,
                payment_reference TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create opening balances table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS opening_balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dealer_code INTEGER NOT NULL,
                dealer_name TEXT NOT NULL,
                opening_balance REAL NOT NULL DEFAULT 0,
                month_year TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dealer_code, month_year)
            )
        ''')
        
        # Create indexes for faster queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_sale_date ON sales_data(sale_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_posting_date ON collections_data(posting_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_collections_dealer ON collections_data(dealer_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_opening_balance_dealer ON opening_balances(dealer_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_opening_balance_month ON opening_balances(month_year)
        ''')
        
        self.conn.commit()
        print(f"Database initialized with sales, collections, and opening balance tables: {self.db_path}")
    
    def insert_collections_data(self, df):
        """Insert collections data from DataFrame into database"""
        cursor = self.conn.cursor()
        
        # Clear existing collections data (optional - remove if you want to append)
        cursor.execute("DELETE FROM collections_data")
        
        inserted_count = 0
        
        # Insert new data
        for _, row in df.iterrows():
            # Skip rows with missing customer codes
            if pd.isna(row['Customer']):
                continue
                
            cursor.execute('''
                INSERT INTO collections_data 
                (posting_date, dealer_code, dealer_name, amount, district_name, collection_type)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                row['Posting Date'].strftime('%Y-%m-%d'),
                int(row['Customer']),
                row['Name of Customer'],
                row['Amount'],
                row['District name'],
                row['Collection Type']
            ))
            inserted_count += 1
        
        self.conn.commit()
        print(f"Inserted {inserted_count} collection records into database")
        return inserted_count
    
    def get_collections_by_date(self, target_date):
        """Get collections for a specific date"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT dealer_code, dealer_name, SUM(amount) as total_amount,
                   COUNT(*) as transaction_count, district_name, collection_type
            FROM collections_data 
            WHERE posting_date = ?
            GROUP BY dealer_code, dealer_name, district_name, collection_type
            ORDER BY total_amount DESC
        ''', (target_date,))
        
        results = cursor.fetchall()
        
        if results:
            df = pd.DataFrame(results, columns=[
                'Dealer_Code', 'Dealer_Name', 'Collection_Amount', 
                'Transaction_Count', 'District', 'Collection_Type'
            ])
            # Add serial number
            df.insert(0, 'Serial_No', range(1, len(df) + 1))
            return df
        else:
            return pd.DataFrame()
    
    def get_collections_summary_by_dealer(self):
        """Get consolidated collections summary by dealer"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT dealer_code, dealer_name, 
                   SUM(amount) as total_collections,
                   COUNT(*) as total_transactions,
                   MIN(posting_date) as first_collection,
                   MAX(posting_date) as last_collection,
                   COUNT(DISTINCT posting_date) as collection_days
            FROM collections_data 
            GROUP BY dealer_code, dealer_name
            ORDER BY total_collections DESC
        ''')
        
        results = cursor.fetchall()
        
        if results:
            df = pd.DataFrame(results, columns=[
                'Dealer_Code', 'Dealer_Name', 'Total_Collections', 
                'Total_Transactions', 'First_Collection', 'Last_Collection', 'Collection_Days'
            ])
            # Add serial number
            df.insert(0, 'Serial_No', range(1, len(df) + 1))
            return df
        else:
            return pd.DataFrame()
    
    def get_collections_stats(self, target_date=None):
        """Get collections statistics for a specific date or all data"""
        cursor = self.conn.cursor()
        
        if target_date:
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT dealer_code) as unique_dealers,
                    COUNT(*) as total_transactions,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM collections_data 
                WHERE posting_date = ?
            ''', (target_date,))
        else:
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT dealer_code) as unique_dealers,
                    COUNT(*) as total_transactions,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM collections_data
            ''')
        
        result = cursor.fetchone()
        return {
            'unique_dealers': result[0] or 0,
            'total_transactions': result[1] or 0,
            'total_amount': result[2] or 0,
            'avg_amount': result[3] or 0,
            'min_amount': result[4] or 0,
            'max_amount': result[5] or 0
        }
    
    def get_available_collection_dates(self):
        """Get all available collection dates in the database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT posting_date FROM collections_data ORDER BY posting_date")
        dates = [row[0] for row in cursor.fetchall()]
        return dates
    
    def get_sales_vs_collections_summary(self):
        """Get combined sales vs collections summary by dealer"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                COALESCE(s.dealer_code, c.dealer_code) as dealer_code,
                COALESCE(s.dealer_name, c.dealer_name) as dealer_name,
                COALESCE(s.total_sales, 0) as total_sales,
                COALESCE(c.total_collections, 0) as total_collections,
                COALESCE(s.total_sales, 0) - COALESCE(c.total_collections, 0) as outstanding_balance,
                COALESCE(s.transaction_days, 0) as sales_days,
                COALESCE(c.collection_days, 0) as collection_days
            FROM 
                (SELECT dealer_code, dealer_name, 
                        SUM(total_quantity) as total_sales,
                        COUNT(DISTINCT sale_date) as transaction_days
                 FROM sales_data 
                 GROUP BY dealer_code, dealer_name) s
            FULL OUTER JOIN 
                (SELECT dealer_code, dealer_name, 
                        SUM(amount) as total_collections,
                        COUNT(DISTINCT posting_date) as collection_days
                 FROM collections_data 
                 GROUP BY dealer_code, dealer_name) c
            ON s.dealer_code = c.dealer_code
            ORDER BY total_sales DESC
        ''')
        
        results = cursor.fetchall()
        
        if results:
            df = pd.DataFrame(results, columns=[
                'Dealer_Code', 'Dealer_Name', 'Total_Sales', 'Total_Collections',
                'Outstanding_Balance', 'Sales_Days', 'Collection_Days'
            ])
            # Add serial number
            df.insert(0, 'Serial_No', range(1, len(df) + 1))
            return df
        else:
            return pd.DataFrame()
    
    def close(self):
        """Close database connection"""
        self.conn.close()

def process_collections_file(excel_file_path):
    """Process collections Excel file and return formatted DataFrame"""
    
    try:
        df = pd.read_excel(excel_file_path)
        print(f"Successfully loaded {len(df)} collection records from {excel_file_path}")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None
    
    # Convert Posting Date to datetime if not already
    df['Posting Date'] = pd.to_datetime(df['Posting Date'])
    
    # Clean and validate data
    df = df.dropna(subset=['Customer'])  # Remove rows with missing customer codes
    df['Customer'] = df['Customer'].astype(int)  # Ensure customer codes are integers
    
    return df

def display_data(df, title="Data"):
    """Display data in formatted way"""
    if df.empty:
        print(f"No data found for {title}")
        return
    
    print(f"\n{'='*120}")
    print(f"{title.upper()}")
    print(f"{'='*120}")
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 25)
    
    print(df.to_string(index=False))

def main():
    """Main function to process collections data and add to database"""
    
    # File paths
    collections_file = "/Users/akhiltayal/Downloads/Collection 1-20.xlsx"
    db_path = "/Users/akhiltayal/CascadeProjects/sales_collections_data.db"
    
    # Check if file exists
    if not os.path.exists(collections_file):
        print(f"Error: File not found at {collections_file}")
        return
    
    # Process collections file
    print("Processing Collections 1-20.xlsx...")
    collections_data = process_collections_file(collections_file)
    
    if collections_data is None:
        print("Failed to process collections file")
        return
    
    # Connect to database and add collections data
    db = SalesCollectionsDatabase(db_path)
    
    # Copy existing sales data if it exists
    old_db_path = "/Users/akhiltayal/CascadeProjects/sales_data.db"
    if os.path.exists(old_db_path):
        print("Copying existing sales data...")
        try:
            old_conn = sqlite3.connect(old_db_path)
            # Check if the table exists
            cursor = old_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sales_data'")
            if cursor.fetchone():
                # Copy data from old database
                old_data = pd.read_sql_query("SELECT * FROM sales_data", old_conn)
                old_data.to_sql('sales_data', db.conn, if_exists='replace', index=False)
                print(f"Sales data copied successfully: {len(old_data)} records")
            else:
                print("No sales_data table found in old database")
            old_conn.close()
        except Exception as e:
            print(f"Error copying sales data: {e}")
            print("Continuing without copying old sales data...")
    
    # Insert collections data
    inserted_count = db.insert_collections_data(collections_data)
    
    print("\n" + "="*80)
    print("COLLECTIONS DATA ADDED TO DATABASE")
    print("="*80)
    
    # Show collections summary
    collection_dates = db.get_available_collection_dates()
    print(f"\nCollection dates in database: {len(collection_dates)} days")
    print(f"Date range: {collection_dates[0]} to {collection_dates[-1]}")
    
    # Overall collections statistics
    overall_stats = db.get_collections_stats()
    print(f"\nOverall Collections Summary:")
    print(f"Total Dealers: {overall_stats['unique_dealers']}")
    print(f"Total Transactions: {overall_stats['total_transactions']}")
    print(f"Total Collections: ₹{overall_stats['total_amount']:,.2f}")
    print(f"Average Collection: ₹{overall_stats['avg_amount']:,.2f}")
    print(f"Min Collection: ₹{overall_stats['min_amount']:,.2f}")
    print(f"Max Collection: ₹{overall_stats['max_amount']:,.2f}")
    
    # Show sample collections by dealer
    print("\n" + "="*80)
    print("TOP 10 DEALERS BY TOTAL COLLECTIONS")
    print("="*80)
    
    collections_summary = db.get_collections_summary_by_dealer()
    if not collections_summary.empty:
        top_10 = collections_summary.head(10)
        display_data(top_10, "Top 10 Dealers by Collections")
    
    db.close()
    print(f"\nDatabase updated and saved at: {db_path}")

if __name__ == "__main__":
    main()

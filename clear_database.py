#!/usr/bin/env python3
"""
Clear all data from the sales database
"""

import sqlite3
import sys

def clear_database(db_path):
    """Clear all data from the database"""
    print(f"Clearing database: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # List of all tables to clear
        tables_to_clear = [
            'sales_data',
            'collections_data', 
            'opening_balances',
            'dealers',
            'credit_discounts',
            'vehicle_tracking',
            'vehicle_unloading',
            'dealer_material_balances',
            'vehicle_material_balances',
            'material_transactions'
        ]
        
        # Check current record counts
        print("Current records:")
        total_records = 0
        for table in tables_to_clear:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"  {table}: {count}")
                    total_records += count
            except sqlite3.OperationalError:
                # Table doesn't exist, skip it
                pass
        
        if total_records == 0:
            print("  Database is already empty")
            return
        
        print(f"  Total records: {total_records}")
        
        # Clear all data from all tables
        print("\nClearing tables...")
        for table in tables_to_clear:
            try:
                cursor.execute(f"DELETE FROM {table}")
                print(f"  ✓ Cleared {table}")
            except sqlite3.OperationalError:
                # Table doesn't exist, skip it
                pass
        
        # Reset auto-increment counters
        print("\nResetting auto-increment counters...")
        for table in tables_to_clear:
            try:
                cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")
            except sqlite3.OperationalError:
                # No auto-increment or table doesn't exist, skip it
                pass
        
        conn.commit()
        
        # Verify clearing
        print("\nAfter clearing:")
        total_after = 0
        for table in tables_to_clear:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"  {table}: {count}")
                    total_after += count
            except sqlite3.OperationalError:
                # Table doesn't exist, skip it
                pass
        
        if total_after == 0:
            print("✅ All database tables cleared successfully!")
        else:
            print(f"❌ Database clearing incomplete - {total_after} records remaining")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error clearing database: {e}")

if __name__ == "__main__":
    import os
    # Use relative path based on script location
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(BASE_DIR, "webapp_sales_collections.db")
    clear_database(db_path)

#!/usr/bin/env python3
"""
Backfill plant_depot values for legacy vehicle_unloading records.

Logic:
1. For each unloading record with NULL plant_depot:
   - Check if there's billing on the same date for that vehicle
   - If single plant_depot billing exists on that date, use it
   - If multiple plant_depot billings exist (DEPOT + PLANT), match by dealer_code
   - If no billing on that date, look for billing within +/- 3 days and match by dealer_code
   - If still no match, check if vehicle has only PLANT or only DEPOT billings historically
"""

import sqlite3
from datetime import datetime, timedelta

def backfill_plant_depot(db_path='webapp_sales_collections.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all unloading records with NULL plant_depot
    cursor.execute('''
        SELECT id, truck_number, unloading_date, dealer_code
        FROM vehicle_unloading
        WHERE plant_depot IS NULL
        ORDER BY unloading_date, truck_number
    ''')
    
    null_records = cursor.fetchall()
    print(f"Found {len(null_records)} unloading records with NULL plant_depot")
    
    updated_count = 0
    skipped_count = 0
    
    for record_id, truck_number, unloading_date, unloading_dealer_code in null_records:
        unloading_dealer_code = str(unloading_dealer_code) if unloading_dealer_code else ''
        
        # Strategy 1: Check billing on the same date
        cursor.execute('''
            SELECT DISTINCT plant_depot, dealer_code
            FROM sales_data
            WHERE truck_number = ? AND sale_date = ?
        ''', (truck_number, unloading_date))
        
        same_date_billings = cursor.fetchall()
        
        if len(same_date_billings) == 1:
            # Single billing on same date - use its plant_depot
            plant_depot = same_date_billings[0][0]
            cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                         (plant_depot, record_id))
            updated_count += 1
            continue
        
        elif len(same_date_billings) > 1:
            # Multiple billings on same date - match by dealer_code
            matched = False
            for billing_plant_depot, billing_dealer_code in same_date_billings:
                if str(billing_dealer_code) == unloading_dealer_code:
                    cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                                 (billing_plant_depot, record_id))
                    updated_count += 1
                    matched = True
                    break
            
            if matched:
                continue
            
            # If no dealer_code match, check if all billings are same plant_depot
            unique_plant_depots = set(b[0] for b in same_date_billings)
            if len(unique_plant_depots) == 1:
                plant_depot = list(unique_plant_depots)[0]
                cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                             (plant_depot, record_id))
                updated_count += 1
                continue
        
        # Strategy 2: Check billing within +/- 3 days and match by dealer_code
        unloading_dt = datetime.strptime(unloading_date, '%Y-%m-%d')
        date_from = (unloading_dt - timedelta(days=3)).strftime('%Y-%m-%d')
        date_to = (unloading_dt + timedelta(days=3)).strftime('%Y-%m-%d')
        
        cursor.execute('''
            SELECT plant_depot, dealer_code, sale_date
            FROM sales_data
            WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ?
            ORDER BY ABS(julianday(sale_date) - julianday(?))
        ''', (truck_number, date_from, date_to, unloading_date))
        
        nearby_billings = cursor.fetchall()
        
        if unloading_dealer_code:
            for billing_plant_depot, billing_dealer_code, _ in nearby_billings:
                if str(billing_dealer_code) == unloading_dealer_code:
                    cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                                 (billing_plant_depot, record_id))
                    updated_count += 1
                    break
            else:
                # No dealer_code match in nearby dates
                if nearby_billings:
                    # Use the closest billing's plant_depot
                    plant_depot = nearby_billings[0][0]
                    cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                                 (plant_depot, record_id))
                    updated_count += 1
                else:
                    skipped_count += 1
        else:
            # No dealer_code in unloading - use closest billing
            if nearby_billings:
                plant_depot = nearby_billings[0][0]
                cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                             (plant_depot, record_id))
                updated_count += 1
            else:
                # Strategy 3: Check vehicle's historical billing pattern
                cursor.execute('''
                    SELECT DISTINCT plant_depot
                    FROM sales_data
                    WHERE truck_number = ?
                ''', (truck_number,))
                
                historical_plant_depots = [row[0] for row in cursor.fetchall()]
                
                if len(historical_plant_depots) == 1:
                    # Vehicle only has one type of billing historically
                    plant_depot = historical_plant_depots[0]
                    cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                                 (plant_depot, record_id))
                    updated_count += 1
                else:
                    # Can't determine - default to PLANT (most common)
                    cursor.execute('UPDATE vehicle_unloading SET plant_depot = ? WHERE id = ?', 
                                 ('PLANT', record_id))
                    updated_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\nBackfill complete:")
    print(f"  Updated: {updated_count} records")
    print(f"  Skipped: {skipped_count} records")
    print(f"  Total: {len(null_records)} records")

if __name__ == '__main__':
    backfill_plant_depot()

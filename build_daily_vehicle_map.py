#!/usr/bin/env python3
"""
Build daily vehicle pending map from November 1, 2025 to current date.
This script processes billing and unloading transactions day by day to maintain
an accurate running balance of pending vehicles.
"""

import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

def build_daily_map():
    conn = sqlite3.connect('webapp_sales_collections.db')
    cursor = conn.cursor()
    
    # Get all transaction dates from Nov 1, 2025 onwards
    cursor.execute("""
        SELECT DISTINCT sale_date as txn_date
        FROM (
            SELECT sale_date FROM sales_data WHERE sale_date >= '2025-11-01'
            UNION
            SELECT sale_date FROM other_dealers_billing WHERE sale_date >= '2025-11-01'
            UNION
            SELECT unloading_date as sale_date FROM vehicle_unloading WHERE unloading_date >= '2025-11-01'
        )
        ORDER BY txn_date
    """)
    
    dates = [row[0] for row in cursor.fetchall()]
    print(f"Processing {len(dates)} dates from {dates[0]} to {dates[-1]}")
    
    # Initialize Nov 1 opening balances from pending_vehicle_unloading
    print("Initializing Nov 1, 2025 opening balances...")
    cursor.execute("""
        SELECT vehicle_number, billing_date, dealer_code, ppc_qty, premium_qty, opc_qty
        FROM pending_vehicle_unloading
        WHERE month_year = '2025-11'
    """)
    
    nov1_count = 0
    for row in cursor.fetchall():
        vehicle_number = row[0]
        billing_date = row[1] or '2025-10-31'
        dealer_code = row[2]
        ppc_qty = row[3] or 0
        premium_qty = row[4] or 0
        opc_qty = row[5] or 0
        
        # Only insert if there's a positive balance
        if ppc_qty + premium_qty + opc_qty > 0.01:
            cursor.execute("""
                INSERT OR REPLACE INTO daily_vehicle_pending
                (date, vehicle_number, ppc_qty, premium_qty, opc_qty, dealer_code, last_billing_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, ('2025-11-01', vehicle_number, ppc_qty, premium_qty, opc_qty, dealer_code, billing_date))
            nov1_count += 1
    
    conn.commit()
    print(f"Initialized {nov1_count} vehicles for Nov 1, 2025")
    
    # Process each date
    for date in dates:
        if date == '2025-11-01':
            continue  # Already initialized above
        
        # Get previous date's balances
        cursor.execute("""
            SELECT vehicle_number, ppc_qty, premium_qty, opc_qty, dealer_code, last_billing_date
            FROM daily_vehicle_pending
            WHERE date = (
                SELECT MAX(date) FROM daily_vehicle_pending WHERE date < ?
            )
        """, (date,))
        
        prev_balances = {}
        for row in cursor.fetchall():
            vehicle = row[0]
            prev_balances[vehicle] = {
                'ppc': row[1],
                'premium': row[2],
                'opc': row[3],
                'dealer_code': row[4],
                'last_billing_date': row[5]
            }
        
        # Get today's billing (sales_data)
        cursor.execute("""
            SELECT truck_number, dealer_code, 
                   SUM(ppc_quantity), SUM(premium_quantity), SUM(opc_quantity)
            FROM sales_data
            WHERE sale_date = ?
            GROUP BY truck_number, dealer_code
        """, (date,))
        
        billing_today = {}
        for row in cursor.fetchall():
            vehicle = row[0]  # Use full vehicle number from sales_data directly
            
            if vehicle not in billing_today:
                billing_today[vehicle] = {'ppc': 0, 'premium': 0, 'opc': 0, 'dealer_code': row[1]}
            
            billing_today[vehicle]['ppc'] += row[2] or 0
            billing_today[vehicle]['premium'] += row[3] or 0
            billing_today[vehicle]['opc'] += row[4] or 0
        
        # Get today's billing (other_dealers_billing)
        cursor.execute("""
            SELECT truck_number, 
                   SUM(ppc_quantity), SUM(premium_quantity), SUM(opc_quantity)
            FROM other_dealers_billing
            WHERE sale_date = ?
            GROUP BY truck_number
        """, (date,))
        
        for row in cursor.fetchall():
            vehicle = row[0]  # Use full vehicle number directly
            
            if vehicle not in billing_today:
                billing_today[vehicle] = {'ppc': 0, 'premium': 0, 'opc': 0, 'dealer_code': None}
            
            billing_today[vehicle]['ppc'] += row[1] or 0
            billing_today[vehicle]['premium'] += row[2] or 0
            billing_today[vehicle]['opc'] += row[3] or 0
        
        # Get today's unloading
        cursor.execute("""
            SELECT truck_number, 
                   SUM(ppc_unloaded), SUM(premium_unloaded), SUM(opc_unloaded)
            FROM vehicle_unloading
            WHERE unloading_date = ?
            GROUP BY truck_number
        """, (date,))
        
        unloading_today = {}
        for row in cursor.fetchall():
            vehicle = row[0]  # Use full vehicle number directly
            
            unloading_today[vehicle] = {
                'ppc': row[1] or 0,
                'premium': row[2] or 0,
                'opc': row[3] or 0
            }
        
        # Calculate new balances for today
        new_balances = {}
        
        # Start with all vehicles from previous day
        for vehicle, prev in prev_balances.items():
            new_balances[vehicle] = {
                'ppc': prev['ppc'],
                'premium': prev['premium'],
                'opc': prev['opc'],
                'dealer_code': prev['dealer_code'],
                'last_billing_date': prev['last_billing_date']
            }
        
        # Add today's billing
        for vehicle, billing in billing_today.items():
            if vehicle not in new_balances:
                new_balances[vehicle] = {
                    'ppc': 0,
                    'premium': 0,
                    'opc': 0,
                    'dealer_code': billing.get('dealer_code'),
                    'last_billing_date': date
                }
            
            new_balances[vehicle]['ppc'] += billing['ppc']
            new_balances[vehicle]['premium'] += billing['premium']
            new_balances[vehicle]['opc'] += billing['opc']
            new_balances[vehicle]['last_billing_date'] = date
            if billing.get('dealer_code'):
                new_balances[vehicle]['dealer_code'] = billing['dealer_code']
        
        # Subtract today's unloading
        for vehicle, unloading in unloading_today.items():
            if vehicle in new_balances:
                new_balances[vehicle]['ppc'] -= unloading['ppc']
                new_balances[vehicle]['premium'] -= unloading['premium']
                new_balances[vehicle]['opc'] -= unloading['opc']
                
                # Ensure non-negative
                new_balances[vehicle]['ppc'] = max(0, new_balances[vehicle]['ppc'])
                new_balances[vehicle]['premium'] = max(0, new_balances[vehicle]['premium'])
                new_balances[vehicle]['opc'] = max(0, new_balances[vehicle]['opc'])
        
        # Save today's balances (only vehicles with pending > 0)
        for vehicle, balance in new_balances.items():
            total = balance['ppc'] + balance['premium'] + balance['opc']
            if total > 0.01:
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_vehicle_pending 
                    (date, vehicle_number, ppc_qty, premium_qty, opc_qty, dealer_code, last_billing_date, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (date, vehicle, balance['ppc'], balance['premium'], balance['opc'], 
                      balance['dealer_code'], balance['last_billing_date']))
        
        conn.commit()
        print(f"Processed {date}: {len([v for v in new_balances.values() if v['ppc'] + v['premium'] + v['opc'] > 0.01])} vehicles pending")
    
    conn.close()
    print("Daily vehicle map built successfully!")

if __name__ == '__main__':
    build_daily_map()

#!/usr/bin/env python3
"""
WhatsApp Message Generator for Dealer Billing
Generates formatted WhatsApp messages with billing details for dealers
"""

import sqlite3
from datetime import datetime, timedelta
import sys

# Database path
DB_PATH = "/Users/akhiltayal/CascadeProjects/DSR/webapp_sales_collections.db"

# Bank holidays (you can update this list as needed)
BANK_HOLIDAYS = [
    "2025-01-26",  # Republic Day
    "2025-03-14",  # Holi
    "2025-08-15",  # Independence Day
    "2025-10-02",  # Gandhi Jayanti
    "2025-10-24",  # Dussehra
    "2025-11-12",  # Diwali
    "2025-12-25",  # Christmas
    # Add more holidays as needed
]

def calculate_due_date(billing_date, working_days=4):
    """Calculate due date excluding weekends and bank holidays"""
    current_date = datetime.strptime(billing_date, '%Y-%m-%d')
    days_added = 0
    
    while days_added < working_days:
        current_date += timedelta(days=1)
        
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() >= 5:
            continue
            
        # Skip bank holidays
        if current_date.strftime('%Y-%m-%d') in BANK_HOLIDAYS:
            continue
            
        days_added += 1
    
    return current_date.strftime('%Y-%m-%d')

def get_dealer_billing_data(dealer_code, billing_date):
    """Get individual invoice data for a specific dealer on a specific date"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get individual sales records (each represents an invoice)
        cursor.execute('''
            SELECT id, dealer_name, ppc_quantity, premium_quantity, opc_quantity, 
                   ppc_purchase_value, premium_purchase_value, opc_purchase_value,
                   total_purchase_value, total_quantity, truck_number, invoice_number
            FROM sales_data 
            WHERE dealer_code = ? AND sale_date = ?
            ORDER BY id
        ''', (dealer_code, billing_date))
        
        sales_records = cursor.fetchall()
        
        if not sales_records:
            return None
        
        invoices = []
        for record in sales_records:
            invoice_id, dealer_name, ppc_qty, premium_qty, opc_qty, ppc_val, premium_val, opc_val, total_val, total_qty, truck_number, invoice_number = record
            
            # Convert MT to bags (1 MT = 20 bags)
            total_bags = int(total_qty * 20) if total_qty > 0 else 0
            
            # Create invoice data
            invoice = {
                'invoice_id': invoice_id,
                'dealer_name': dealer_name,
                'total_bags': total_bags,
                'total_value': total_val,
                'truck_number': truck_number,
                'invoice_number': invoice_number,
                'materials': []
            }
            
            # Add material details
            if ppc_qty > 0:
                ppc_bags = int(ppc_qty * 20)
                ppc_price_per_bag = ppc_val / ppc_bags if ppc_bags > 0 else 0
                invoice['materials'].append({
                    'type': 'PPC',
                    'bags': ppc_bags,
                    'price_per_bag': ppc_price_per_bag
                })
            
            if premium_qty > 0:
                premium_bags = int(premium_qty * 20)
                premium_price_per_bag = premium_val / premium_bags if premium_bags > 0 else 0
                invoice['materials'].append({
                    'type': 'Premium',
                    'bags': premium_bags,
                    'price_per_bag': premium_price_per_bag
                })
            
            if opc_qty > 0:
                opc_bags = int(opc_qty * 20)
                opc_price_per_bag = opc_val / opc_bags if opc_bags > 0 else 0
                invoice['materials'].append({
                    'type': 'OPC',
                    'bags': opc_bags,
                    'price_per_bag': opc_price_per_bag
                })
            
            invoices.append(invoice)
        
        conn.close()
        
        return {
            'dealer_name': sales_records[0][1],
            'dealer_code': dealer_code,
            'billing_date': billing_date,
            'invoices': invoices
        }
        
    except Exception as e:
        print(f"Error getting billing data: {str(e)}")
        return None

def generate_whatsapp_message(dealer_code, billing_date, truck_numbers=None):
    """Generate WhatsApp message for dealer billing with separate invoices"""
    
    # Get billing data
    billing_data = get_dealer_billing_data(dealer_code, billing_date)
    
    if not billing_data:
        return f"No billing data found for dealer {dealer_code} on {billing_date}"
    
    # Calculate due date
    due_date = calculate_due_date(billing_date)
    
    # Format dates for display
    billing_date_formatted = datetime.strptime(billing_date, '%Y-%m-%d').strftime('%d/%m/%Y')
    due_date_formatted = datetime.strptime(due_date, '%Y-%m-%d').strftime('%d/%m/%Y')
    
    # Build the message
    message = f"""*Billing Date:* {billing_date_formatted}

*INVOICE DETAILS:*"""

    # Add each invoice separately
    total_amount = 0
    for i, invoice in enumerate(billing_data['invoices']):
        # Priority order for truck number:
        # 1. Truck number from database (highest priority)
        # 2. User-provided truck number
        # 3. Auto-generated truck number (fallback)
        
        if invoice['truck_number']:
            truck_number = invoice['truck_number']
        elif truck_numbers and len(truck_numbers) > i:
            truck_number = truck_numbers[i]
        else:
            truck_number = f"TRK-{billing_date.replace('-', '')}-{i+1:02d}"
        
        message += f"\n\n*Truck:* {truck_number}"
        
        # Add invoice number if available
        if invoice['invoice_number']:
            message += f"\n*Invoice:* {invoice['invoice_number']}"
        
        # Add material details for this invoice
        for material in invoice['materials']:
            message += f"\n{material['type']}: {material['bags']} bags @ Rs.{material['price_per_bag']:.2f}/bag"
        
        message += f"\n*Invoice Amount:* Rs.{invoice['total_value']:,.2f}"
        total_amount += invoice['total_value']
    
    message += f"""

*TOTAL AMOUNT:* Rs.{total_amount:,.2f}
*PAYMENT DUE DATE:* {due_date_formatted}"""

    return message

def generate_messages_for_date(billing_date, truck_numbers=None):
    """Generate WhatsApp messages for all dealers who had billing on a specific date"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all dealers who had sales on the billing date
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name
            FROM sales_data 
            WHERE sale_date = ?
            ORDER BY dealer_name
        ''', (billing_date,))
        
        dealers = cursor.fetchall()
        conn.close()
        
        if not dealers:
            print(f"No dealers found with billing on {billing_date}")
            return
        
        print(f"=== WHATSAPP MESSAGES FOR {billing_date} ===")
        print(f"Found {len(dealers)} dealers with billing")
        print()
        
        for i, (dealer_code, dealer_name) in enumerate(dealers, 1):
            # Get truck numbers for this dealer (could be multiple)
            dealer_truck_numbers = None
            if truck_numbers:
                # Assume truck numbers are provided in order for all invoices
                dealer_truck_numbers = truck_numbers
            
            message = generate_whatsapp_message(dealer_code, billing_date, dealer_truck_numbers)
            
            print(f"{'='*60}")
            print(f"MESSAGE {i}: {dealer_name}")
            print(f"{'='*60}")
            print(message)
            print()
        
    except Exception as e:
        print(f"Error generating messages: {str(e)}")

def main():
    """Main function for testing"""
    if len(sys.argv) < 2:
        print("Usage: python whatsapp_message_generator.py <billing_date> [truck_numbers...]")
        print("Example: python whatsapp_message_generator.py 2025-11-20 TRK001 TRK002 TRK003")
        return
    
    billing_date = sys.argv[1]
    truck_numbers = sys.argv[2:] if len(sys.argv) > 2 else None
    
    generate_messages_for_date(billing_date, truck_numbers)

if __name__ == "__main__":
    main()

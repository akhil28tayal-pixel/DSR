#!/usr/bin/env python3
"""
Sales and Collections Web Application - Simplified Version
Flask web app for uploading files and generating reports
"""

from flask import Flask, render_template, request, jsonify
import pandas as pd
import sqlite3
import os
from datetime import datetime
from werkzeug.utils import secure_filename

# Get the base directory of the application
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Import our database functions
import sys
sys.path.insert(0, BASE_DIR)
from sales_collections_database import SalesCollectionsDatabase

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Database configuration - use relative path
DB_PATH = os.path.join(BASE_DIR, "webapp_sales_collections.db")

# Configure upload settings - use relative path
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'pdf'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_depot_abbreviation(plant_description):
    """Get 4-letter abbreviation for depot/plant description"""
    if not plant_description:
        return None
    
    # Common depot abbreviations mapping
    depot_map = {
        'DL NASIRPUR TR': 'NASR',
        'DL OKHLA': 'OKHL',
        'DL NARELA': 'NARE',
        'DL MUNDKA': 'MUND',
        'DL PATPARGANJ': 'PATP',
        'DL SHAHDARA': 'SHAH',
        'GGN MANESAR': 'MANE',
        'GGN SOHNA': 'SOHN',
        'FBD BALLABGARH': 'BALL',
        'FBD FARIDABAD': 'FARI',
        'NDA NOIDA': 'NOID',
        'NDA GREATER NOIDA': 'GNOI',
        'GZB GHAZIABAD': 'GHAZ',
        'DADRI': 'DADR',
        'PALWAL': 'PALW',
        'REWARI': 'REWA',
        'ROHTAK': 'ROHT',
        'SONIPAT': 'SONI',
        'PANIPAT': 'PANI',
        'KARNAL': 'KARN',
        'AMBALA': 'AMBA',
        'JIND': 'JIND',
        'HISAR': 'HISA',
        'BHIWANI': 'BHIW',
    }
    
    plant_upper = plant_description.upper().strip()
    
    # Check for exact match first
    if plant_upper in depot_map:
        return depot_map[plant_upper]
    
    # Check for partial match
    for key, abbr in depot_map.items():
        if key in plant_upper or plant_upper in key:
            return abbr
    
    # Generate abbreviation from first 4 characters of significant words
    words = plant_upper.replace('TR', '').replace('DEPOT', '').replace('PLANT', '').split()
    if words:
        # Take first 4 chars of the most significant word (usually the location name)
        significant_word = max(words, key=len) if words else plant_upper
        return significant_word[:4].upper()
    
    return plant_upper[:4].upper() if len(plant_upper) >= 4 else plant_upper.upper()

def categorize_product(product_desc):
    """Categorize product into PPC, Premium, or OPC based on description"""
    product_desc = str(product_desc).upper()
    
    if 'OPC' in product_desc:
        return 'OPC'
    elif 'PREM' in product_desc or 'PREMIUM' in product_desc:
        return 'Premium'
    elif 'PPC' in product_desc:
        return 'PPC'
    else:
        # Default to PPC if unclear
        return 'PPC'

def process_new_sales_format(df):
    """Process the new sales file format with product line items"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        successful_invoices = 0
        duplicate_invoices = 0
        error_rows = []
        
        # Group by invoice to aggregate product line items
        invoice_groups = df.groupby('Invoice Number')
        
        for invoice_number, invoice_df in invoice_groups:
            try:
                # Check if invoice already exists (duplicate check)
                cursor.execute('SELECT id FROM sales_data WHERE invoice_number = ?', (int(invoice_number),))
                if cursor.fetchone():
                    duplicate_invoices += 1
                    continue
                
                # Get common invoice data from first row
                first_row = invoice_df.iloc[0]
                
                # Extract basic invoice info
                sale_date = pd.to_datetime(first_row['Invoice Date']).strftime('%Y-%m-%d')
                dealer_code = int(first_row['Customer Code'])
                dealer_name = str(first_row['Customer Name/Sold To']).strip()
                truck_number = str(first_row['Truck Number']).strip()
                plant_depot = str(first_row['Plant/Depot']).strip()
                plant_description = str(first_row.get('Plant Description', '')).strip()
                if plant_description.lower() in ['nan', 'none', '']:
                    plant_description = None
                
                # Initialize quantities and values
                ppc_quantity = 0.0
                premium_quantity = 0.0
                opc_quantity = 0.0
                ppc_purchase_value = 0.0
                premium_purchase_value = 0.0
                opc_purchase_value = 0.0
                
                # Aggregate products by type
                for _, row in invoice_df.iterrows():
                    product_type = categorize_product(row['Product Desc.'])
                    quantity = float(row['Invoice Quantity'])
                    amount = float(row['Total Amount'])
                    
                    if product_type == 'PPC':
                        ppc_quantity += quantity
                        ppc_purchase_value += amount
                    elif product_type == 'Premium':
                        premium_quantity += quantity
                        premium_purchase_value += amount
                    elif product_type == 'OPC':
                        opc_quantity += quantity
                        opc_purchase_value += amount
                
                # Calculate totals
                total_quantity = ppc_quantity + premium_quantity + opc_quantity
                total_purchase_value = ppc_purchase_value + premium_purchase_value + opc_purchase_value
                
                # Insert aggregated invoice data
                cursor.execute('''
                    INSERT INTO sales_data 
                    (sale_date, dealer_code, dealer_name, invoice_number, 
                     ppc_quantity, premium_quantity, opc_quantity, total_quantity, 
                     ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value, 
                     truck_number, plant_depot, plant_description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (sale_date, dealer_code, dealer_name, invoice_number,
                      ppc_quantity, premium_quantity, opc_quantity, total_quantity, 
                      ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value,
                      truck_number, plant_depot, plant_description))
                
                successful_invoices += 1
                
            except Exception as row_error:
                error_rows.append(f"Invoice {invoice_number}: {str(row_error)}")
        
        db.conn.commit()
        db.close()
        
        if successful_invoices > 0 or duplicate_invoices > 0:
            message = f"Successfully uploaded {successful_invoices} invoices"
            if duplicate_invoices > 0:
                message += f", {duplicate_invoices} duplicates skipped"
            if error_rows:
                message += f", {len(error_rows)} errors"
            return jsonify({'success': True, 'message': message, 'errors': error_rows, 'duplicates': duplicate_invoices})
        else:
            detailed_message = f'No valid invoices found. All {len(invoice_groups)} invoices failed to process.'
            if error_rows:
                detailed_message += f' First error: {error_rows[0] if error_rows else "Unknown error"}'
            return jsonify({'success': False, 'message': detailed_message, 'errors': error_rows})
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error processing new format file: {str(e)}'})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload_sales', methods=['POST'])
def upload_sales():
    """Handle sales file upload"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file selected'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'})
    
    if file and allowed_file(file.filename):
        try:
            # Read Excel file
            df = pd.read_excel(file)
            
            # Check if this is the new format (has 'Customer Code' instead of 'Dealer Code')
            if 'Customer Code' in df.columns and 'Invoice Date' in df.columns:
                return process_new_sales_format(df)
            
            # Process and save to database (original format)
            db = None
            try:
                db = SalesCollectionsDatabase(DB_PATH)
                cursor = db.conn.cursor()
                
                successful_rows = 0
                duplicate_rows = 0
                error_rows = []
                
                for index, row in df.iterrows():
                    try:
                        # Extract data from row
                        sale_date_raw = row.get('Sale Date', row.get('sale_date', ''))
                        if pd.isna(sale_date_raw) or sale_date_raw == '':
                            raise ValueError("Sale Date is missing or empty")
                        sale_date = pd.to_datetime(sale_date_raw).strftime('%Y-%m-%d')
                        
                        dealer_code_raw = row.get('Dealer Code', row.get('dealer_code', 0))
                        if pd.isna(dealer_code_raw) or dealer_code_raw == 0:
                            raise ValueError("Dealer Code is missing or zero")
                        dealer_code = int(dealer_code_raw)
                        
                        dealer_name_raw = row.get('Dealer Name', row.get('dealer_name', ''))
                        if pd.isna(dealer_name_raw) or str(dealer_name_raw).strip() == '':
                            raise ValueError("Dealer Name is missing or empty")
                        dealer_name = str(dealer_name_raw).strip()
                        
                        # Extract truck number and invoice number
                        truck_number = str(row.get('Truck Number', row.get('truck_number', row.get('Vehicle Number', '')))).strip()
                        invoice_number = row.get('Invoice Number', row.get('invoice_number', row.get('Invoice No', None)))
                        
                        # Handle empty truck number
                        if not truck_number or truck_number.lower() in ['nan', 'none', '']:
                            truck_number = None
                        
                        # Handle invoice number
                        if pd.isna(invoice_number) or invoice_number == '':
                            invoice_number = None
                        else:
                            invoice_number = int(invoice_number)
                        
                        # Check for duplicate invoice
                        if invoice_number:
                            cursor.execute('SELECT id FROM sales_data WHERE invoice_number = ?', (invoice_number,))
                            if cursor.fetchone():
                                duplicate_rows += 1
                                continue
                        
                        # Material quantities
                        ppc_quantity = float(row.get('PPC Quantity', row.get('ppc_quantity', 0)))
                        premium_quantity = float(row.get('Premium Quantity', row.get('premium_quantity', 0)))
                        opc_quantity = float(row.get('OPC Quantity', row.get('opc_quantity', 0)))
                        total_quantity = ppc_quantity + premium_quantity + opc_quantity
                        
                        # Purchase values
                        ppc_purchase_value = float(row.get('PPC Purchase Value', row.get('ppc_purchase_value', 0)))
                        premium_purchase_value = float(row.get('Premium Purchase Value', row.get('premium_purchase_value', 0)))
                        opc_purchase_value = float(row.get('OPC Purchase Value', row.get('opc_purchase_value', 0)))
                        total_purchase_value = ppc_purchase_value + premium_purchase_value + opc_purchase_value
                        
                        # Extract plant/depot information if available
                        plant_depot = str(row.get('Plant/Depot', row.get('plant_depot', row.get('Source', '')))).strip()
                        if not plant_depot or plant_depot.lower() in ['nan', 'none', '']:
                            plant_depot = None
                        
                        # Insert sales data
                        cursor.execute('''
                            INSERT INTO sales_data 
                            (sale_date, dealer_code, dealer_name, invoice_number, 
                             ppc_quantity, premium_quantity, opc_quantity, total_quantity, 
                             ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value, 
                             truck_number, plant_depot)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (sale_date, dealer_code, dealer_name, invoice_number,
                              ppc_quantity, premium_quantity, opc_quantity, total_quantity, 
                              ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value,
                              truck_number, plant_depot))
                        
                        successful_rows += 1
                        
                    except Exception as row_error:
                        error_rows.append(f"Row {index + 2}: {str(row_error)}")
                
                db.conn.commit()
                
                if successful_rows > 0 or duplicate_rows > 0:
                    message = f"Successfully uploaded {successful_rows} sales records"
                    if duplicate_rows > 0:
                        message += f", {duplicate_rows} duplicates skipped"
                    if error_rows:
                        message += f", {len(error_rows)} errors"
                    return jsonify({'success': True, 'message': message, 'errors': error_rows, 'duplicates': duplicate_rows})
                else:
                    detailed_message = f'No valid data found in file. All {len(df)} rows failed to process.'
                    if error_rows:
                        detailed_message += f' First error: {error_rows[0] if error_rows else "Unknown error"}'
                    return jsonify({'success': False, 'message': detailed_message, 'errors': error_rows, 'total_rows': len(df), 'columns': list(df.columns)})
                    
            finally:
                if db is not None:
                    try:
                        db.close()
                    except:
                        pass
                        
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error processing file: {str(e)}'})
    
    return jsonify({'success': False, 'message': 'Invalid file type. Please upload Excel files only.'})

@app.route('/upload_collections', methods=['POST'])
def upload_collections():
    """Handle collections file upload"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file selected'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'})
    
    if file and allowed_file(file.filename):
        try:
            # Read Excel file
            df = pd.read_excel(file)
            
            # Process and save to database
            db = None
            try:
                db = SalesCollectionsDatabase(DB_PATH)
                cursor = db.conn.cursor()
                
                successful_rows = 0
                duplicate_rows = 0
                error_rows = []
                
                for index, row in df.iterrows():
                    try:
                        # Extract data from row - support multiple column name formats
                        posting_date = pd.to_datetime(row.get('Posting Date', row.get('posting_date', row.get('Date', '')))).strftime('%Y-%m-%d')
                        
                        # Dealer code: try 'Customer', 'customer', 'Dealer Code', 'dealer_code'
                        dealer_code = row.get('Customer', row.get('customer', row.get('Dealer Code', row.get('dealer_code', 0))))
                        dealer_code = int(dealer_code) if pd.notna(dealer_code) else 0
                        
                        # Dealer name: try 'Name of Customer', 'Name of customer', 'Dealer Name', 'dealer_name'
                        dealer_name = row.get('Name of Customer', row.get('Name of customer', row.get('Dealer Name', row.get('dealer_name', ''))))
                        dealer_name = str(dealer_name).strip() if pd.notna(dealer_name) else ''
                        
                        amount = float(row.get('Amount', row.get('amount', 0)))
                        
                        # Optional fields
                        district_name = row.get('District Name', row.get('district_name', ''))
                        district_name = str(district_name).strip() if pd.notna(district_name) else ''
                        
                        collection_type = row.get('Collection Type', row.get('collection_type', ''))
                        collection_type = str(collection_type).strip() if pd.notna(collection_type) else ''
                        
                        payment_reference = row.get('Payment Reference', row.get('payment_reference', ''))
                        payment_reference = str(payment_reference).strip() if pd.notna(payment_reference) else ''
                        
                        # Check for duplicate collection (same date, dealer, amount, and payment_reference)
                        cursor.execute('''
                            SELECT id FROM collections_data 
                            WHERE posting_date = ? AND dealer_code = ? AND amount = ? AND payment_reference = ?
                        ''', (posting_date, dealer_code, amount, payment_reference))
                        if cursor.fetchone():
                            duplicate_rows += 1
                            continue
                        
                        # Insert collections data
                        cursor.execute('''
                            INSERT INTO collections_data 
                            (posting_date, dealer_code, dealer_name, amount, district_name, collection_type, payment_reference)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (posting_date, dealer_code, dealer_name, amount, district_name, collection_type, payment_reference))
                        
                        successful_rows += 1
                        
                    except Exception as row_error:
                        error_rows.append(f"Row {index + 2}: {str(row_error)}")
                
                db.conn.commit()
                
                if successful_rows > 0 or duplicate_rows > 0:
                    message = f"Successfully uploaded {successful_rows} collections records"
                    if duplicate_rows > 0:
                        message += f", {duplicate_rows} duplicates skipped"
                    if error_rows:
                        message += f", {len(error_rows)} errors"
                    return jsonify({'success': True, 'message': message, 'errors': error_rows, 'duplicates': duplicate_rows})
                else:
                    return jsonify({'success': False, 'message': 'No valid data found in file', 'errors': error_rows})
                    
            finally:
                if db is not None:
                    try:
                        db.close()
                    except:
                        pass
                        
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error processing file: {str(e)}'})
    
    return jsonify({'success': False, 'message': 'Invalid file type. Please upload Excel files only.'})

@app.route('/get_available_dates')
def get_available_dates():
    """Get available dates from database"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get distinct dates from both sales and collections
        cursor.execute("SELECT DISTINCT sale_date FROM sales_data ORDER BY sale_date")
        sales_dates = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT posting_date FROM collections_data ORDER BY posting_date")
        collection_dates = [row[0] for row in cursor.fetchall()]
        
        # Combine and sort all dates
        all_dates = sorted(list(set(sales_dates + collection_dates)))
        
        db.close()
        return jsonify({'success': True, 'dates': all_dates})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_available_months')
def get_available_months():
    """Get available months from database for month-wise reports"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get distinct months from sales data
        cursor.execute("SELECT DISTINCT strftime('%Y-%m', sale_date) as month_year FROM sales_data ORDER BY month_year")
        sales_months = [row[0] for row in cursor.fetchall()]
        
        # Get distinct months from collections data
        cursor.execute("SELECT DISTINCT strftime('%Y-%m', posting_date) as month_year FROM collections_data ORDER BY month_year")
        collection_months = [row[0] for row in cursor.fetchall()]
        
        # Combine and sort all months
        all_months = sorted(list(set(sales_months + collection_months)))
        
        # Format months for display
        formatted_months = []
        for month in all_months:
            if month:  # Skip None values
                try:
                    date_obj = datetime.strptime(month, '%Y-%m')
                    formatted_month = date_obj.strftime('%B %Y')
                    formatted_months.append({'value': month, 'display': formatted_month})
                except:
                    pass
        
        db.close()
        return jsonify({'success': True, 'months': formatted_months, 'raw_months': all_months})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# Helper functions for report generation
def get_previous_month(month_year):
    """Get previous month in YYYY-MM format"""
    try:
        date_obj = datetime.strptime(month_year, '%Y-%m')
        if date_obj.month == 1:
            prev_month = date_obj.replace(year=date_obj.year - 1, month=12)
        else:
            prev_month = date_obj.replace(month=date_obj.month - 1)
        return prev_month.strftime('%Y-%m')
    except:
        return None

def calculate_month_closing_balances(month_year):
    """Calculate closing balances for all dealers for a specific month"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get opening balances for the month
        opening_balances_map = get_opening_balances_with_auto_calculation(month_year)
        
        # Get total sales for the month
        cursor.execute('''
            SELECT dealer_code, dealer_name, SUM(total_purchase_value) as total_sales
            FROM sales_data 
            WHERE strftime('%Y-%m', sale_date) = ?
            GROUP BY dealer_code, dealer_name
        ''', (month_year,))
        
        sales_data = {f"{row[0]}_{row[1]}": row[2] for row in cursor.fetchall()}
        
        # Get total collections for the month
        cursor.execute('''
            SELECT dealer_code, dealer_name, SUM(amount) as total_collections
            FROM collections_data 
            WHERE strftime('%Y-%m', posting_date) = ?
            GROUP BY dealer_code, dealer_name
        ''', (month_year,))
        
        collections_data = {f"{row[0]}_{row[1]}": row[2] for row in cursor.fetchall()}
        
        # Get credit notes for the month (reduces balance)
        credits_data = {}
        try:
            cursor.execute('''
                SELECT dealer_code, dealer_name, SUM(credit_discount) as total_credit
                FROM credit_discounts 
                WHERE month_year = ?
                GROUP BY dealer_code, dealer_name
            ''', (month_year,))
            credits_data = {f"{row[0]}_{row[1]}": row[2] for row in cursor.fetchall()}
        except:
            pass
        
        # Get debit notes for the month (increases balance)
        debits_data = {}
        try:
            cursor.execute('''
                SELECT dealer_code, dealer_name, SUM(debit_amount) as total_debit
                FROM debit_notes 
                WHERE month_year = ?
                GROUP BY dealer_code, dealer_name
            ''', (month_year,))
            debits_data = {f"{row[0]}_{row[1]}": row[2] for row in cursor.fetchall()}
        except:
            pass
        
        # Calculate closing balances = opening + sales - collections - credits + debits
        closing_balances = {}
        for dealer_key, opening_balance in opening_balances_map.items():
            sales = sales_data.get(dealer_key, 0)
            collections = collections_data.get(dealer_key, 0)
            credits = credits_data.get(dealer_key, 0)
            debits = debits_data.get(dealer_key, 0)
            closing = opening_balance + sales - collections - credits + debits
            closing_balances[dealer_key] = round(closing, 2)
        
        db.close()
        return closing_balances
        
    except Exception as e:
        return {}

def get_opening_balances_with_auto_calculation(month_year):
    """Get opening balances with auto-calculation from previous month's closing balances"""
    try:
        from dateutil.relativedelta import relativedelta
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Calculate previous month
        current_month_dt = datetime.strptime(month_year + '-01', '%Y-%m-%d')
        prev_month_dt = current_month_dt - relativedelta(months=1)
        prev_month_year = prev_month_dt.strftime('%Y-%m')
        
        # First, get manual opening balances for this month
        cursor.execute('''
            SELECT dealer_code, dealer_name, opening_balance 
            FROM opening_balances 
            WHERE month_year = ?
        ''', (month_year,))
        
        manual_balances = {}
        has_manual_balances = False
        for row in cursor.fetchall():
            key = f"{row[0]}_{row[1]}"
            manual_balances[key] = round(row[2], 2)
            has_manual_balances = True
        
        # Get all dealers who have transactions in this month OR previous month
        # Group by dealer_code only to avoid duplicates from name variations
        cursor.execute('''
            SELECT dealer_code, MAX(dealer_name) as dealer_name FROM (
                SELECT dealer_code, dealer_name FROM sales_data WHERE strftime('%Y-%m', sale_date) = ?
                UNION
                SELECT dealer_code, dealer_name FROM collections_data WHERE strftime('%Y-%m', posting_date) = ?
                UNION
                SELECT dealer_code, dealer_name FROM sales_data WHERE strftime('%Y-%m', sale_date) = ?
                UNION
                SELECT dealer_code, dealer_name FROM collections_data WHERE strftime('%Y-%m', posting_date) = ?
                UNION
                SELECT dealer_code, dealer_name FROM opening_balances WHERE month_year = ?
            )
            GROUP BY dealer_code
        ''', (month_year, month_year, prev_month_year, prev_month_year, prev_month_year))
        
        all_dealers = cursor.fetchall()
        
        # For dealers without manual opening balances, calculate from previous month's closing
        result_balances = {}
        
        # If no manual balances for current month, calculate previous month's closing for all dealers
        if not has_manual_balances:
            # Get previous month's opening balances (use dealer_code only)
            cursor.execute('''
                SELECT dealer_code, MAX(opening_balance) as opening_balance 
                FROM opening_balances 
                WHERE month_year = ?
                GROUP BY dealer_code
            ''', (prev_month_year,))
            
            prev_opening = {}
            for row in cursor.fetchall():
                prev_opening[str(row[0])] = row[1] or 0
            
            # Get previous month's sales (group by dealer_code only)
            cursor.execute('''
                SELECT dealer_code, SUM(total_purchase_value) as total_sales
                FROM sales_data 
                WHERE strftime('%Y-%m', sale_date) = ?
                GROUP BY dealer_code
            ''', (prev_month_year,))
            
            prev_sales = {}
            for row in cursor.fetchall():
                prev_sales[str(row[0])] = row[1] or 0
            
            # Get previous month's collections (group by dealer_code only)
            cursor.execute('''
                SELECT dealer_code, SUM(amount) as total_collections
                FROM collections_data 
                WHERE strftime('%Y-%m', posting_date) = ?
                GROUP BY dealer_code
            ''', (prev_month_year,))
            
            prev_collections = {}
            for row in cursor.fetchall():
                prev_collections[str(row[0])] = row[1] or 0
            
            # Get previous month's credit notes (reduces balance, group by dealer_code only)
            prev_credits = {}
            try:
                cursor.execute('''
                    SELECT dealer_code, SUM(credit_discount) as total_credit
                    FROM credit_discounts 
                    WHERE month_year = ?
                    GROUP BY dealer_code
                ''', (prev_month_year,))
                for row in cursor.fetchall():
                    prev_credits[str(row[0])] = row[1] or 0
            except:
                pass
            
            # Get previous month's debit notes (increases balance, group by dealer_code only)
            prev_debits = {}
            try:
                cursor.execute('''
                    SELECT dealer_code, SUM(debit_amount) as total_debit
                    FROM debit_notes 
                    WHERE month_year = ?
                    GROUP BY dealer_code
                ''', (prev_month_year,))
                for row in cursor.fetchall():
                    prev_debits[str(row[0])] = row[1] or 0
            except:
                pass
            
            # Calculate previous month closing = opening + sales - collections - credits + debits
            for dealer_code, dealer_name in all_dealers:
                key = f"{dealer_code}_{dealer_name}"
                dealer_code_str = str(dealer_code)
                opening = prev_opening.get(dealer_code_str, 0)
                sales = prev_sales.get(dealer_code_str, 0)
                collections = prev_collections.get(dealer_code_str, 0)
                credits = prev_credits.get(dealer_code_str, 0)
                debits = prev_debits.get(dealer_code_str, 0)
                closing = opening + sales - collections - credits + debits
                result_balances[key] = round(closing, 2)
        else:
            # Use manual balances and calculate for missing dealers
            previous_month = get_previous_month(month_year)
            if previous_month:
                previous_closing = calculate_month_closing_balances(previous_month)
            else:
                previous_closing = {}
            
            for dealer_code, dealer_name in all_dealers:
                key = f"{dealer_code}_{dealer_name}"
                
                if key in manual_balances:
                    # Use manual opening balance
                    result_balances[key] = round(manual_balances[key], 2)
                elif key in previous_closing:
                    # Use previous month's closing balance
                    result_balances[key] = round(previous_closing.get(key, 0), 2)
                else:
                    # Default to 0
                    result_balances[key] = 0.0
        
        db.close()
        return result_balances
        
    except Exception as e:
        return {}

@app.route('/get_report', methods=['POST'])
def get_report():
    """Generate report for selected date"""
    try:
        data = request.get_json()
        selected_date = data.get('date')
        
        if not selected_date:
            return jsonify({'success': False, 'message': 'Date is required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Extract month-year for opening balances
        month_year = selected_date[:7]  # YYYY-MM format
        
        # Get sales data for the selected date, grouped by dealer_code
        cursor.execute('''
            SELECT dealer_code, MAX(dealer_name) as dealer_name, 
                   SUM(ppc_quantity) as ppc_quantity, 
                   SUM(premium_quantity) as premium_quantity, 
                   SUM(opc_quantity) as opc_quantity, 
                   SUM(total_quantity) as total_quantity, 
                   SUM(ppc_purchase_value) as ppc_purchase_value, 
                   SUM(premium_purchase_value) as premium_purchase_value, 
                   SUM(opc_purchase_value) as opc_purchase_value, 
                   SUM(total_purchase_value) as total_purchase_value
            FROM sales_data 
            WHERE sale_date = ?
            GROUP BY dealer_code
            ORDER BY dealer_name
        ''', (selected_date,))
        
        sales_results = cursor.fetchall()
        sales = []
        total_sales = 0
        
        for row in sales_results:
            sale_data = {
                'dealer_code': row[0],
                'dealer_name': row[1],
                'ppc_quantity': row[2] or 0,
                'premium_quantity': row[3] or 0,
                'opc_quantity': row[4] or 0,
                'total_quantity': row[5] or 0,
                'ppc_purchase_value': row[6] or 0,
                'premium_purchase_value': row[7] or 0,
                'opc_purchase_value': row[8] or 0,
                'total_purchase_value': row[9] or 0
            }
            sales.append(sale_data)
            total_sales += sale_data['total_purchase_value']
        
        # Get collections data for the selected date, grouped by dealer_code
        cursor.execute('''
            SELECT dealer_code, MAX(dealer_name) as dealer_name, SUM(amount) as amount
            FROM collections_data 
            WHERE posting_date = ?
            GROUP BY dealer_code
            ORDER BY dealer_name
        ''', (selected_date,))
        
        collections_results = cursor.fetchall()
        collections = []
        total_collections = 0
        
        for row in collections_results:
            collection_data = {
                'dealer_code': row[0],
                'dealer_name': row[1],
                'amount': row[2] or 0
            }
            collections.append(collection_data)
            total_collections += collection_data['amount']
        
        # Calculate month start date for cumulative sales (1st day of selected month)
        date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
        month_start = date_obj.replace(day=1).strftime('%Y-%m-%d')
        
        # Get cumulative sales from 1st of month to selected date
        cursor.execute('''
            SELECT dealer_code, dealer_name, 
                   SUM(ppc_quantity) as ppc_quantity, 
                   SUM(premium_quantity) as premium_quantity, 
                   SUM(opc_quantity) as opc_quantity, 
                   SUM(total_quantity) as total_quantity,
                   SUM(ppc_purchase_value) as ppc_purchase_value,
                   SUM(premium_purchase_value) as premium_purchase_value,
                   SUM(opc_purchase_value) as opc_purchase_value,
                   SUM(total_purchase_value) as total_purchase_value
            FROM sales_data 
            WHERE sale_date >= ? AND sale_date <= ?
            GROUP BY dealer_code, dealer_name
            ORDER BY dealer_name
        ''', (month_start, selected_date))
        
        cumulative_sales_results = cursor.fetchall()
        cumulative_sales = []
        
        for row in cumulative_sales_results:
            cumulative_sales.append({
                'dealer_code': row[0],
                'dealer_name': row[1],
                'ppc_quantity': row[2] or 0,
                'premium_quantity': row[3] or 0,
                'opc_quantity': row[4] or 0,
                'total_quantity': row[5] or 0,
                'ppc_purchase_value': row[6] or 0,
                'premium_purchase_value': row[7] or 0,
                'opc_purchase_value': row[8] or 0,
                'total_purchase_value': row[9] or 0
            })
        
        # Get cumulative collections from 1st of month to selected date
        cursor.execute('''
            SELECT dealer_code, dealer_name, SUM(amount) as total_amount
            FROM collections_data 
            WHERE posting_date >= ? AND posting_date <= ?
            GROUP BY dealer_code, dealer_name
            ORDER BY dealer_name
        ''', (month_start, selected_date))
        
        cumulative_collections_results = cursor.fetchall()
        cumulative_collections = []
        
        for row in cumulative_collections_results:
            cumulative_collections.append({
                'dealer_code': row[0],
                'dealer_name': row[1],
                'total_amount': row[2] or 0
            })
        
        # Get opening balances with auto-calculation
        opening_balances_map = get_opening_balances_with_auto_calculation(month_year)
        opening_balances = []
        
        # Get all unique dealers from current month AND previous month
        # Use dealer_code as primary key to avoid duplicates from name variations
        dealers_dict = {}
        for sale in sales + cumulative_sales:
            dealer_code = str(sale['dealer_code'])
            if dealer_code not in dealers_dict:
                dealers_dict[dealer_code] = sale['dealer_name']
        for collection in collections + cumulative_collections:
            dealer_code = str(collection['dealer_code'])
            if dealer_code not in dealers_dict:
                dealers_dict[dealer_code] = collection['dealer_name']
        
        # Also include dealers from opening_balances_map (includes previous month dealers)
        for key in opening_balances_map.keys():
            parts = key.split('_', 1)
            if len(parts) == 2:
                dealer_code = str(parts[0])
                if dealer_code not in dealers_dict:
                    dealers_dict[dealer_code] = parts[1]
        
        # For each dealer_code, try all possible name variants to find opening balance
        for dealer_code, primary_name in dealers_dict.items():
            # Try to find opening balance with any name variant for this dealer_code
            opening_balance = 0
            found = False
            for key in opening_balances_map.keys():
                if key.startswith(f"{dealer_code}_"):
                    opening_balance = opening_balances_map[key]
                    found = True
                    break
            
            if not found:
                opening_balance = opening_balances_map.get(f"{dealer_code}_{primary_name}", 0)
            
            opening_balances.append({
                'dealer_code': dealer_code,
                'dealer_name': primary_name,
                'opening_balance': round(opening_balance, 2)
            })
        
        # Get credit notes for the month (cumulative)
        credit_notes = {}
        try:
            cursor.execute('''
                SELECT dealer_code, credit_discount
                FROM credit_discounts
                WHERE month_year = ?
            ''', (month_year,))
            for row in cursor.fetchall():
                credit_notes[str(row[0])] = row[1] or 0
        except:
            pass
        
        # Get debit notes for the month (cumulative)
        debit_notes = {}
        try:
            cursor.execute('''
                SELECT dealer_code, debit_amount
                FROM debit_notes
                WHERE month_year = ?
            ''', (month_year,))
            for row in cursor.fetchall():
                debit_notes[str(row[0])] = row[1] or 0
        except:
            pass
        
        db.close()
        
        return jsonify({
            'success': True,
            'sales': sales,
            'collections': collections,
            'cumulative_sales': cumulative_sales,
            'cumulative_collections': cumulative_collections,
            'opening_balances': opening_balances,
            'credit_notes': credit_notes,
            'debit_notes': debit_notes,
            'total_sales': total_sales,
            'total_collections': total_collections,
            'selected_date': selected_date
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# Import WhatsApp message generator functions
from whatsapp_message_generator import generate_whatsapp_message, get_dealer_billing_data

@app.route('/whatsapp_generator')
def whatsapp_generator():
    """WhatsApp message generator page"""
    return render_template('whatsapp_generator.html')

@app.route('/get_dealers_for_date', methods=['POST'])
def get_dealers_for_date():
    """Get list of dealers who had billing on a specific date"""
    try:
        data = request.get_json()
        selected_date = data.get('date')
        
        if not selected_date:
            return jsonify({'success': False, 'message': 'Date is required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get dealers who had sales on the selected date
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name, COUNT(*) as invoice_count
            FROM sales_data 
            WHERE sale_date = ?
            GROUP BY dealer_code, dealer_name
            ORDER BY dealer_name
        ''', (selected_date,))
        
        dealers = []
        for row in cursor.fetchall():
            dealers.append({
                'dealer_code': row[0],
                'dealer_name': row[1],
                'invoice_count': row[2]
            })
        
        db.close()
        
        return jsonify({
            'success': True,
            'dealers': dealers,
            'date': selected_date
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_truck_numbers', methods=['POST'])
def get_truck_numbers_api():
    """API endpoint to get truck numbers for a dealer on a specific date"""
    try:
        data = request.get_json()
        dealer_code = data.get('dealer_code')
        billing_date = data.get('date')
        
        if not dealer_code or not billing_date:
            return jsonify({'success': False, 'message': 'Missing dealer_code or billing_date'})
        
        # Get truck numbers from database
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        cursor.execute('''
            SELECT invoice_number, truck_number
            FROM sales_data 
            WHERE dealer_code = ? AND sale_date = ?
            ORDER BY invoice_number
        ''', (int(dealer_code), billing_date))
        
        results = cursor.fetchall()
        db.close()
        
        truck_numbers = []
        for invoice_number, truck_number in results:
            truck_numbers.append({
                'invoice_number': invoice_number,
                'truck_number': truck_number if truck_number else ''
            })
        
        return jsonify({'success': True, 'truck_numbers': truck_numbers})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error getting truck numbers: {str(e)}'})

@app.route('/generate_whatsapp_message', methods=['POST'])
def generate_whatsapp_message_api():
    """Generate WhatsApp message for specific dealer and date"""
    try:
        data = request.get_json()
        dealer_code = data.get('dealer_code')
        billing_date = data.get('date')
        truck_numbers = data.get('truck_numbers', [])
        due_date = data.get('due_date')  # Get custom due date from request
        
        if not dealer_code or not billing_date:
            return jsonify({'success': False, 'message': 'Dealer code and date are required'})
        
        # Generate the WhatsApp message with custom due date
        message = generate_whatsapp_message(int(dealer_code), billing_date, truck_numbers, due_date)
        
        # Get billing data for additional info
        billing_data = get_dealer_billing_data(int(dealer_code), billing_date)
        
        return jsonify({
            'success': True,
            'message': message,
            'dealer_name': billing_data['dealer_name'] if billing_data else 'Unknown',
            'invoice_count': len(billing_data['invoices']) if billing_data else 0
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# ============== Payment Reminder Routes ==============

@app.route('/process_ageing_report', methods=['POST'])
def process_ageing_report():
    """Process uploaded ageing report Excel file and generate payment reminders"""
    try:
        import pandas as pd
        from datetime import datetime
        from werkzeug.utils import secure_filename
        import os
        
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file uploaded'})
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'})
        
        if not file.filename.endswith(('.xlsx', '.xls')):
            return jsonify({'success': False, 'message': 'Please upload an Excel file (.xlsx or .xls)'})
        
        # Read Excel file
        try:
            df = pd.read_excel(file)
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error reading Excel file: {str(e)}'})
        
        # Validate required columns
        required_columns = ['Customer', 'Cust.Name', 'Outstanding Amt.', 'SPL GL "Y" Balance', 'T1']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return jsonify({'success': False, 'message': f'Missing required columns: {", ".join(missing_columns)}'})
        
        # Process each customer and calculate payment due today
        reminders = []
        
        for index, row in df.iterrows():
            try:
                customer_code = str(row['Customer'])
                customer_name = str(row['Cust.Name'])
                outstanding_amt = float(row['Outstanding Amt.']) if pd.notna(row['Outstanding Amt.']) else 0.0
                spl_gl_balance = float(row['SPL GL "Y" Balance']) if pd.notna(row['SPL GL "Y" Balance']) else 0.0
                t1 = float(row['T1']) if pd.notna(row['T1']) else 0.0
                
                # Calculate total outstanding and payment due today
                total_outstanding = outstanding_amt + spl_gl_balance
                payment_due_today = outstanding_amt + spl_gl_balance - t1
                
                # Round to ceil of nearest 10
                import math
                total_outstanding_rounded = math.ceil(total_outstanding / 10) * 10
                payment_due_today_rounded = math.ceil(payment_due_today / 10) * 10
                
                # Only include customers with payment due today > 0
                if payment_due_today > 0.01:  # Small threshold to avoid floating point issues
                    # Generate WhatsApp message
                    message = f"""*PAYMENT REMINDER*

Total Outstanding: Rs. {total_outstanding_rounded:,.0f}
*Payment Due Today: Rs. {payment_due_today_rounded:,.0f}*"""
                    
                    reminders.append({
                        'customer_code': customer_code,
                        'customer_name': customer_name,
                        'outstanding_amt': outstanding_amt,
                        'spl_gl_balance': spl_gl_balance,
                        't1': t1,
                        'total_outstanding': total_outstanding,
                        'payment_due_today': payment_due_today,
                        'message': message
                    })
            except Exception as e:
                # Skip rows with errors
                continue
        
        # Sort by payment due today (descending)
        reminders.sort(key=lambda x: x['payment_due_today'], reverse=True)
        
        return jsonify({
            'success': True,
            'reminders': reminders,
            'total_customers': len(reminders),
            'total_amount_due': sum(r['payment_due_today'] for r in reminders)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error processing file: {str(e)}'})

# ============== Unloading WhatsApp Generator Routes ==============

@app.route('/get_dealers_for_unloading_date', methods=['POST'])
def get_dealers_for_unloading_date():
    """Get list of dealers who had unloading on a specific date, grouped by dealer_code"""
    try:
        data = request.get_json()
        selected_date = data.get('date')
        
        if not selected_date:
            return jsonify({'success': False, 'message': 'Date is required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get dealers who had unloading on the selected date, grouped by dealer_code
        cursor.execute('''
            SELECT dealer_code, unloading_dealer, 
                   COUNT(*) as unloading_count,
                   SUM(unloaded_quantity) as total_qty,
                   SUM(ppc_unloaded) as total_ppc,
                   SUM(premium_unloaded) as total_premium,
                   SUM(opc_unloaded) as total_opc
            FROM vehicle_unloading 
            WHERE unloading_date = ?
            GROUP BY dealer_code, unloading_dealer
            ORDER BY unloading_dealer
        ''', (selected_date,))
        
        dealers = []
        for row in cursor.fetchall():
            dealers.append({
                'dealer_code': row[0],
                'dealer_name': row[1],
                'unloading_count': row[2],
                'total_qty': row[3] or 0,
                'total_ppc': row[4] or 0,
                'total_premium': row[5] or 0,
                'total_opc': row[6] or 0
            })
        
        db.close()
        
        return jsonify({
            'success': True,
            'dealers': dealers,
            'date': selected_date
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/generate_unloading_whatsapp_message', methods=['POST'])
def generate_unloading_whatsapp_message():
    """Generate WhatsApp message for unloading details of a dealer on a specific date"""
    try:
        data = request.get_json()
        dealer_code = data.get('dealer_code')
        unloading_date = data.get('date')
        
        if not dealer_code or not unloading_date:
            return jsonify({'success': False, 'message': 'Dealer code and date are required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get dealer name
        cursor.execute('''
            SELECT DISTINCT unloading_dealer 
            FROM vehicle_unloading 
            WHERE dealer_code = ? AND unloading_date = ?
        ''', (dealer_code, unloading_date))
        dealer_row = cursor.fetchone()
        dealer_name = dealer_row[0] if dealer_row else 'Unknown Dealer'
        
        # Get all unloading records for this dealer on this date
        cursor.execute('''
            SELECT truck_number, unloading_point, 
                   ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity
            FROM vehicle_unloading 
            WHERE dealer_code = ? AND unloading_date = ?
            ORDER BY truck_number
        ''', (dealer_code, unloading_date))
        
        unloading_records = cursor.fetchall()
        
        if not unloading_records:
            db.close()
            return jsonify({'success': False, 'message': 'No unloading records found for this dealer on this date'})
        
        # Get billing for this dealer on this date
        cursor.execute('''
            SELECT truck_number, invoice_number, 
                   ppc_quantity, premium_quantity, opc_quantity, total_quantity
            FROM sales_data 
            WHERE dealer_code = ? AND sale_date = ?
            ORDER BY invoice_number
        ''', (dealer_code, unloading_date))
        
        billing_records = cursor.fetchall()
        
        # Get opening balance for this dealer on this date
        opening = get_dealer_opening_balance(cursor, dealer_name, unloading_date, is_other_dealer=False, dealer_code=dealer_code)
        
        # Calculate total billed today
        total_ppc_billed = 0
        total_premium_billed = 0
        total_opc_billed = 0
        
        for record in billing_records:
            total_ppc_billed += record[2] or 0
            total_premium_billed += record[3] or 0
            total_opc_billed += record[4] or 0
        
        # Calculate total unloaded today
        total_ppc_unloaded = 0
        total_premium_unloaded = 0
        total_opc_unloaded = 0
        
        for record in unloading_records:
            total_ppc_unloaded += record[2] or 0
            total_premium_unloaded += record[3] or 0
            total_opc_unloaded += record[4] or 0
        
        # Calculate closing balance (opening + billed - unloaded)
        closing_ppc = opening['ppc'] + total_ppc_billed - total_ppc_unloaded
        closing_premium = opening['premium'] + total_premium_billed - total_premium_unloaded
        closing_opc = opening['opc'] + total_opc_billed - total_opc_unloaded
        
        db.close()
        
        # Format date for display
        from datetime import datetime
        date_obj = datetime.strptime(unloading_date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d-%m-%Y')
        
        # Build WhatsApp message
        message_lines = []
        message_lines.append(f"*{dealer_name}*")
        message_lines.append(f"📅 Date: {formatted_date}")
        message_lines.append("")
        
        # Unloading section
        message_lines.append("*📦 Today's Unloading:*")
        message_lines.append("─" * 25)
        
        for record in unloading_records:
            truck_number = record[0]
            unloading_point = record[1] or '-'
            ppc = record[2] or 0
            premium = record[3] or 0
            opc = record[4] or 0
            total_qty = record[5] or 0
            
            # Convert to bags
            ppc_bags = round(ppc * 20)
            premium_bags = round(premium * 20)
            opc_bags = round(opc * 20)
            total_bags = round(total_qty * 20)
            
            message_lines.append(f"🚛 Truck: *{truck_number}*")
            message_lines.append(f"   📍 Point: {unloading_point}")
            
            # Show bags breakdown
            bag_parts = []
            if ppc_bags > 0:
                bag_parts.append(f"PPC: {ppc_bags}")
            if premium_bags > 0:
                bag_parts.append(f"Premium: {premium_bags}")
            if opc_bags > 0:
                bag_parts.append(f"OPC: {opc_bags}")
            
            if bag_parts:
                message_lines.append(f"   🎒 {', '.join(bag_parts)}")
            message_lines.append(f"   📊 Total: *{total_bags} bags*")
            message_lines.append("")
        
        # Total unloading summary
        total_unloaded_bags = round((total_ppc_unloaded + total_premium_unloaded + total_opc_unloaded) * 20)
        message_lines.append(f"*Total Unloaded: {total_unloaded_bags} bags*")
        message_lines.append("")
        
        # Material Balance section
        message_lines.append("─" * 25)
        message_lines.append("*📊 Material Balance:*")
        message_lines.append("")
        
        # Opening balance (in bags) - show even if negative
        opening_ppc_bags = round(opening['ppc'] * 20)
        opening_premium_bags = round(opening['premium'] * 20)
        opening_opc_bags = round(opening['opc'] * 20)
        
        message_lines.append("*Opening Balance:*")
        balance_parts = []
        if opening_ppc_bags != 0:
            status = "Advance" if opening_ppc_bags > 0 else "Pending"
            balance_parts.append(f"PPC: {abs(opening_ppc_bags)} {status}")
        if opening_premium_bags != 0:
            status = "Advance" if opening_premium_bags > 0 else "Pending"
            balance_parts.append(f"Premium: {abs(opening_premium_bags)} {status}")
        if opening_opc_bags != 0:
            status = "Advance" if opening_opc_bags > 0 else "Pending"
            balance_parts.append(f"OPC: {abs(opening_opc_bags)} {status}")
        
        if balance_parts:
            message_lines.append(f"  {', '.join(balance_parts)} bags")
        else:
            message_lines.append("  No opening balance")
        
        message_lines.append("")
        
        # Today's billing (in bags) - show total only in material balance
        billed_ppc_bags = round(total_ppc_billed * 20)
        billed_premium_bags = round(total_premium_billed * 20)
        billed_opc_bags = round(total_opc_billed * 20)
        
        message_lines.append("*Today's Billing (+):*")
        billing_parts = []
        if billed_ppc_bags != 0:
            billing_parts.append(f"PPC: {billed_ppc_bags}")
        if billed_premium_bags != 0:
            billing_parts.append(f"Premium: {billed_premium_bags}")
        if billed_opc_bags != 0:
            billing_parts.append(f"OPC: {billed_opc_bags}")
        
        if billing_parts:
            message_lines.append(f"  {', '.join(billing_parts)} bags")
        else:
            message_lines.append("  No billing today")
        
        message_lines.append("")
        
        # Today's unloading (in bags)
        unloaded_ppc_bags = round(total_ppc_unloaded * 20)
        unloaded_premium_bags = round(total_premium_unloaded * 20)
        unloaded_opc_bags = round(total_opc_unloaded * 20)
        
        message_lines.append("*Today's Unloading (-):*")
        unloading_parts = []
        if unloaded_ppc_bags != 0:
            unloading_parts.append(f"PPC: {unloaded_ppc_bags}")
        if unloaded_premium_bags != 0:
            unloading_parts.append(f"Premium: {unloaded_premium_bags}")
        if unloaded_opc_bags != 0:
            unloading_parts.append(f"OPC: {unloaded_opc_bags}")
        
        if unloading_parts:
            message_lines.append(f"  {', '.join(unloading_parts)} bags")
        
        message_lines.append("")
        
        # Closing balance (in bags)
        closing_ppc_bags = round(closing_ppc * 20)
        closing_premium_bags = round(closing_premium * 20)
        closing_opc_bags = round(closing_opc * 20)
        
        message_lines.append("*Closing Balance:*")
        closing_parts = []
        if closing_ppc_bags != 0:
            status = "Advance" if closing_ppc_bags > 0 else "Pending"
            closing_parts.append(f"PPC: {abs(closing_ppc_bags)} {status}")
        if closing_premium_bags != 0:
            status = "Advance" if closing_premium_bags > 0 else "Pending"
            closing_parts.append(f"Premium: {abs(closing_premium_bags)} {status}")
        if closing_opc_bags != 0:
            status = "Advance" if closing_opc_bags > 0 else "Pending"
            closing_parts.append(f"OPC: {abs(closing_opc_bags)} {status}")
        
        if closing_parts:
            message_lines.append(f"  {', '.join(closing_parts)} bags")
        else:
            message_lines.append("  No pending balance")
        
        message = '\n'.join(message_lines)
        
        return jsonify({
            'success': True,
            'message': message,
            'dealer_name': dealer_name,
            'unloading_count': len(unloading_records)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# ============== Vehicle Details Routes ==============

@app.route('/vehicle_details')
def vehicle_details():
    """Vehicle billing and unloading details page"""
    return render_template('vehicle_details.html')

@app.route('/dealer_balance')
def dealer_balance():
    """Dealer material balance report page"""
    return render_template('dealer_balance.html')

def get_dealer_opening_balance(cursor, dealer_name, before_date, is_other_dealer=False, dealer_code=None):
    """Calculate opening balance for a dealer using manual opening balance + cumulative transactions
    
    If no manual opening balance exists for the current month, it calculates from previous month's closing.
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
    # Get the month of the selected date
    month_year = before_date[:7]  # Extract YYYY-MM from date
    month_start = month_year + '-01'  # First day of the month
    
    # Start with manual opening balance from admin page (for 1st of month)
    manual_opening = {'ppc': 0, 'premium': 0, 'opc': 0}
    has_manual_opening = False
    
    try:
        row = None
        # For "Other" dealers, always match by dealer_name
        if is_other_dealer:
            cursor.execute('''
                SELECT ppc_qty, premium_qty, opc_qty
                FROM opening_material_balance
                WHERE month_year = ? AND dealer_name = ?
            ''', (month_year, dealer_name))
            row = cursor.fetchone()
        else:
            # For regular dealers, first try to match by dealer_code (most accurate)
            if dealer_code:
                cursor.execute('''
                    SELECT ppc_qty, premium_qty, opc_qty
                    FROM opening_material_balance
                    WHERE month_year = ? AND dealer_code = ?
                ''', (month_year, str(dealer_code)))
                row = cursor.fetchone()
            
            # If no match by dealer_code, try by name (only if exactly one match)
            if not row:
                cursor.execute('''
                    SELECT ppc_qty, premium_qty, opc_qty
                    FROM opening_material_balance
                    WHERE month_year = ? AND dealer_name = ?
                ''', (month_year, dealer_name))
                rows = cursor.fetchall()
                if len(rows) == 1:
                    row = rows[0]
        
        if row:
            manual_opening = {
                'ppc': row[0] or 0,
                'premium': row[1] or 0,
                'opc': row[2] or 0
            }
            has_manual_opening = True
    except Exception as e:
        # Table might not exist yet
        pass
    
    # If no manual opening balance for current month, calculate from previous month's closing
    if not has_manual_opening:
        try:
            # Calculate previous month
            current_month = datetime.strptime(month_year + '-01', '%Y-%m-%d')
            prev_month = current_month - relativedelta(months=1)
            prev_month_year = prev_month.strftime('%Y-%m')
            prev_month_start = prev_month_year + '-01'
            prev_month_end = current_month.strftime('%Y-%m-%d')  # First day of current month
            
            # Get previous month's opening balance (manual)
            prev_opening = {'ppc': 0, 'premium': 0, 'opc': 0}
            if is_other_dealer:
                cursor.execute('''
                    SELECT ppc_qty, premium_qty, opc_qty
                    FROM opening_material_balance
                    WHERE month_year = ? AND dealer_name = ?
                ''', (prev_month_year, dealer_name))
                prev_row = cursor.fetchone()
            else:
                prev_row = None
                if dealer_code:
                    cursor.execute('''
                        SELECT ppc_qty, premium_qty, opc_qty
                        FROM opening_material_balance
                        WHERE month_year = ? AND dealer_code = ?
                    ''', (prev_month_year, str(dealer_code)))
                    prev_row = cursor.fetchone()
                if not prev_row:
                    cursor.execute('''
                        SELECT ppc_qty, premium_qty, opc_qty
                        FROM opening_material_balance
                        WHERE month_year = ? AND dealer_name = ?
                    ''', (prev_month_year, dealer_name))
                    prev_rows = cursor.fetchall()
                    if len(prev_rows) == 1:
                        prev_row = prev_rows[0]
            
            if prev_row:
                prev_opening = {
                    'ppc': prev_row[0] or 0,
                    'premium': prev_row[1] or 0,
                    'opc': prev_row[2] or 0
                }
            
            # Get previous month's billed
            if not is_other_dealer:
                if dealer_code:
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), 
                               COALESCE(SUM(premium_quantity), 0), 
                               COALESCE(SUM(opc_quantity), 0)
                        FROM sales_data 
                        WHERE dealer_code = ? AND sale_date >= ? AND sale_date < ?
                    ''', (dealer_code, prev_month_start, prev_month_end))
                else:
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), 
                               COALESCE(SUM(premium_quantity), 0), 
                               COALESCE(SUM(opc_quantity), 0)
                        FROM sales_data 
                        WHERE dealer_name = ? AND sale_date >= ? AND sale_date < ?
                    ''', (dealer_name, prev_month_start, prev_month_end))
            else:
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), 
                           COALESCE(SUM(premium_quantity), 0), 
                           COALESCE(SUM(opc_quantity), 0)
                    FROM other_dealers_billing 
                    WHERE dealer_name = ? AND sale_date >= ? AND sale_date < ?
                ''', (dealer_name, prev_month_start, prev_month_end))
            
            prev_billed = cursor.fetchone()
            
            # Get previous month's unloaded
            if dealer_code and not is_other_dealer:
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_unloaded), 0), 
                           COALESCE(SUM(premium_unloaded), 0), 
                           COALESCE(SUM(opc_unloaded), 0)
                    FROM vehicle_unloading 
                    WHERE dealer_code = ? AND unloading_date >= ? AND unloading_date < ?
                ''', (dealer_code, prev_month_start, prev_month_end))
            else:
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_unloaded), 0), 
                           COALESCE(SUM(premium_unloaded), 0), 
                           COALESCE(SUM(opc_unloaded), 0)
                    FROM vehicle_unloading 
                    WHERE unloading_dealer = ? AND unloading_date >= ? AND unloading_date < ?
                ''', (dealer_name, prev_month_start, prev_month_end))
            
            prev_unloaded = cursor.fetchone()
            
            # Previous month closing = opening + billed - unloaded
            manual_opening = {
                'ppc': prev_opening['ppc'] + (prev_billed[0] or 0) - (prev_unloaded[0] or 0),
                'premium': prev_opening['premium'] + (prev_billed[1] or 0) - (prev_unloaded[1] or 0),
                'opc': prev_opening['opc'] + (prev_billed[2] or 0) - (prev_unloaded[2] or 0)
            }
        except Exception as e:
            # If calculation fails, use 0
            pass
    
    # Get cumulative billed from month start to before_date (within the month)
    if not is_other_dealer:
        # Use dealer_code if available to avoid matching multiple dealers with same name
        if dealer_code:
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_quantity), 0), 
                       COALESCE(SUM(premium_quantity), 0), 
                       COALESCE(SUM(opc_quantity), 0),
                       COALESCE(SUM(total_quantity), 0)
                FROM sales_data 
                WHERE dealer_code = ? AND sale_date >= ? AND sale_date < ?
            ''', (dealer_code, month_start, before_date))
        else:
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_quantity), 0), 
                       COALESCE(SUM(premium_quantity), 0), 
                       COALESCE(SUM(opc_quantity), 0),
                       COALESCE(SUM(total_quantity), 0)
                FROM sales_data 
                WHERE dealer_name = ? AND sale_date >= ? AND sale_date < ?
            ''', (dealer_name, month_start, before_date))
    else:
        cursor.execute('''
            SELECT COALESCE(SUM(ppc_quantity), 0), 
                   COALESCE(SUM(premium_quantity), 0), 
                   COALESCE(SUM(opc_quantity), 0),
                   COALESCE(SUM(total_quantity), 0)
            FROM other_dealers_billing 
            WHERE dealer_name = ? AND sale_date >= ? AND sale_date < ?
        ''', (dealer_name, month_start, before_date))
    
    billed = cursor.fetchone()
    
    # Get cumulative unloaded from month start to before_date (within the month)
    # Use dealer_code if available to avoid matching multiple dealers with same name
    if dealer_code and not is_other_dealer:
        cursor.execute('''
            SELECT COALESCE(SUM(ppc_unloaded), 0), 
                   COALESCE(SUM(premium_unloaded), 0), 
                   COALESCE(SUM(opc_unloaded), 0),
                   COALESCE(SUM(unloaded_quantity), 0)
            FROM vehicle_unloading 
            WHERE dealer_code = ? AND unloading_date >= ? AND unloading_date < ?
        ''', (dealer_code, month_start, before_date))
    else:
        cursor.execute('''
            SELECT COALESCE(SUM(ppc_unloaded), 0), 
                   COALESCE(SUM(premium_unloaded), 0), 
                   COALESCE(SUM(opc_unloaded), 0),
                   COALESCE(SUM(unloaded_quantity), 0)
            FROM vehicle_unloading 
            WHERE unloading_dealer = ? AND unloading_date >= ? AND unloading_date < ?
        ''', (dealer_name, month_start, before_date))
    
    unloaded = cursor.fetchone()
    
    # Opening balance = Manual opening + Billed since month start - Unloaded since month start
    ppc = manual_opening['ppc'] + (billed[0] or 0) - (unloaded[0] or 0)
    premium = manual_opening['premium'] + (billed[1] or 0) - (unloaded[1] or 0)
    opc = manual_opening['opc'] + (billed[2] or 0) - (unloaded[2] or 0)
    
    return {
        'ppc': ppc,
        'premium': premium,
        'opc': opc,
        'total': ppc + premium + opc
    }

@app.route('/get_dealer_balance', methods=['POST'])
def get_dealer_balance():
    """Get dealer-wise billed vs unloaded quantities for a date with opening balance"""
    try:
        data = request.get_json()
        selected_date = data.get('date')
        
        if not selected_date:
            return jsonify({'success': False, 'message': 'Date is required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        month_year = selected_date[:7]  # Extract YYYY-MM
        month_start = month_year + '-01'
        
        # Get ALL dealers who have any activity (opening balance, billed, or unloaded)
        all_dealers = {}  # {dealer_code: {'dealer_name': name, 'is_other': False}}
        
        # Calculate previous month for fallback
        from dateutil.relativedelta import relativedelta
        current_month_dt = datetime.strptime(month_year + '-01', '%Y-%m-%d')
        prev_month_dt = current_month_dt - relativedelta(months=1)
        prev_month_year = prev_month_dt.strftime('%Y-%m')
        prev_month_start = prev_month_year + '-01'
        prev_month_end = current_month_dt.strftime('%Y-%m-%d')
        
        # 1. Get dealers from opening_material_balance for this month
        has_current_month_opening = False
        try:
            cursor.execute('''
                SELECT dealer_code, dealer_name, dealer_type
                FROM opening_material_balance
                WHERE month_year = ?
            ''', (month_year,))
            rows = cursor.fetchall()
            if rows:
                has_current_month_opening = True
            for row in rows:
                dealer_code = str(row[0])
                is_other = row[2] == 'Other'
                dealer_name = row[1] or f'Dealer {dealer_code}'
                # For "Other" dealers, use dealer_name as key to avoid duplicates
                if is_other:
                    dealer_key = dealer_name
                else:
                    dealer_key = dealer_code
                all_dealers[dealer_key] = {
                    'dealer_name': dealer_name,
                    'is_other': is_other
                }
        except:
            pass
        
        # 1b. If no opening balance for current month, check if previous month has entries
        # If not, auto-calculate and save previous month's closing balances
        if not has_current_month_opening:
            # Check if previous month has opening_material_balance entries
            cursor.execute('''
                SELECT dealer_code, dealer_name, dealer_type
                FROM opening_material_balance
                WHERE month_year = ?
            ''', (prev_month_year,))
            prev_month_entries = cursor.fetchall()
            
            if prev_month_entries:
                # Use previous month's entries directly
                for row in prev_month_entries:
                    dealer_code = str(row[0])
                    is_other = row[2] == 'Other'
                    dealer_name = row[1] or f'Dealer {dealer_code}'
                    if is_other:
                        dealer_key = dealer_name
                    else:
                        dealer_key = dealer_code
                    if dealer_key not in all_dealers:
                        all_dealers[dealer_key] = {
                            'dealer_name': dealer_name,
                            'is_other': is_other
                        }
            else:
                # No previous month entries - need to calculate and save them
                print(f"INFO: No {prev_month_year} dealer entries found. Calculating and saving closing balances...")
                
                from dateutil.relativedelta import relativedelta
                from calendar import monthrange
                
                # Get previous month dates
                prev_month_dt = datetime.strptime(month_start, '%Y-%m-%d') - relativedelta(months=1)
                prev_prev_month_dt = prev_month_dt - relativedelta(months=1)
                prev_prev_month_year = prev_prev_month_dt.strftime('%Y-%m')
                prev_month_start_date = prev_month_dt.replace(day=1).strftime('%Y-%m-%d')
                last_day = monthrange(prev_month_dt.year, prev_month_dt.month)[1]
                prev_month_end_date = prev_month_dt.replace(day=last_day).strftime('%Y-%m-%d')
                
                # Get all dealers that had transactions in previous month OR had opening balance
                # Use dealer_name as key for "Other" type dealers, dealer_code for regular dealers
                dealers_to_process = {}
                
                # Get dealers from month before previous month's opening
                cursor.execute('''
                    SELECT dealer_code, dealer_name, dealer_type
                    FROM opening_material_balance
                    WHERE month_year = ?
                ''', (prev_prev_month_year,))
                for row in cursor.fetchall():
                    dealer_code = str(row[0])
                    dealer_name = row[1]
                    dealer_type = row[2]
                    # For "Other" type dealers, use dealer_name as key
                    if dealer_type == 'Other':
                        dealer_key = dealer_name
                    else:
                        dealer_key = dealer_code
                    dealers_to_process[dealer_key] = {
                        'dealer_code': dealer_code,
                        'dealer_name': dealer_name,
                        'dealer_type': dealer_type
                    }
                
                # Get dealers from previous month sales (these are regular dealers, not Other)
                cursor.execute('''
                    SELECT DISTINCT dealer_code, dealer_name
                    FROM sales_data
                    WHERE sale_date >= ? AND sale_date <= ?
                ''', (prev_month_start_date, prev_month_end_date))
                for row in cursor.fetchall():
                    dealer_code = str(row[0])
                    dealer_name = row[1]
                    # Regular dealers use dealer_code as key
                    if dealer_code not in dealers_to_process:
                        dealers_to_process[dealer_code] = {
                            'dealer_code': dealer_code,
                            'dealer_name': dealer_name,
                            'dealer_type': 'Active'
                        }
                
                # Get dealers from previous month unloading (regular dealers only)
                cursor.execute('''
                    SELECT DISTINCT dealer_code, unloading_dealer
                    FROM vehicle_unloading
                    WHERE unloading_date >= ? AND unloading_date <= ?
                    AND is_other_dealer = 0
                ''', (prev_month_start_date, prev_month_end_date))
                for row in cursor.fetchall():
                    dealer_code = str(row[0])
                    dealer_name = row[1]
                    # Regular dealers use dealer_code as key
                    if dealer_code not in dealers_to_process:
                        dealers_to_process[dealer_code] = {
                            'dealer_code': dealer_code,
                            'dealer_name': dealer_name,
                            'dealer_type': 'Active'
                        }
                
                # Calculate closing balance for each dealer
                # IMPORTANT: opening_material_balance stores CLOSING balances
                # Entry with month_year='2025-11' contains OCTOBER 31 closing (manually added as Nov opening)
                # We need to calculate NOVEMBER closing first, then use it as DECEMBER opening
                dealers_to_save = []
                
                # Get previous-previous month dates for November transactions
                prev_prev_month_start = prev_prev_month_dt.replace(day=1).strftime('%Y-%m-%d')
                last_day_prev_prev = monthrange(prev_prev_month_dt.year, prev_prev_month_dt.month)[1]
                prev_prev_month_end = prev_prev_month_dt.replace(day=last_day_prev_prev).strftime('%Y-%m-%d')
                
                for dealer_key, dealer_info in dealers_to_process.items():
                    dealer_code = dealer_info['dealer_code']
                    dealer_name = dealer_info['dealer_name']
                    dealer_type = dealer_info['dealer_type']
                    is_other = dealer_type == 'Other'
                    
                    # Get October closing (stored in Nov entry) - this is November opening
                    if is_other:
                        cursor.execute('''
                            SELECT ppc_qty, premium_qty, opc_qty
                            FROM opening_material_balance
                            WHERE month_year = ? AND dealer_name = ? AND dealer_type = 'Other'
                        ''', (prev_prev_month_year, dealer_name))
                    else:
                        cursor.execute('''
                            SELECT ppc_qty, premium_qty, opc_qty
                            FROM opening_material_balance
                            WHERE month_year = ? AND dealer_code = ?
                        ''', (prev_prev_month_year, dealer_code))
                    oct_closing_row = cursor.fetchone()
                    nov_opening_ppc = oct_closing_row[0] if oct_closing_row else 0
                    nov_opening_premium = oct_closing_row[1] if oct_closing_row else 0
                    nov_opening_opc = oct_closing_row[2] if oct_closing_row else 0
                    
                    # Get November billing (all November transactions)
                    if is_other:
                        cursor.execute('''
                            SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                            FROM other_dealers_billing
                            WHERE dealer_name = ? AND sale_date >= ? AND sale_date <= ?
                        ''', (dealer_name, prev_prev_month_start, prev_prev_month_end))
                    else:
                        cursor.execute('''
                            SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                            FROM sales_data
                            WHERE dealer_code = ? AND sale_date >= ? AND sale_date <= ?
                        ''', (dealer_code, prev_prev_month_start, prev_prev_month_end))
                    nov_billed = cursor.fetchone()
                    
                    # Get November unloading (all November transactions)
                    if is_other:
                        cursor.execute('''
                            SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                            FROM vehicle_unloading
                            WHERE unloading_dealer = ? AND is_other_dealer = 1 AND unloading_date >= ? AND unloading_date <= ?
                        ''', (dealer_name, prev_prev_month_start, prev_prev_month_end))
                    else:
                        cursor.execute('''
                            SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                            FROM vehicle_unloading
                            WHERE dealer_code = ? AND unloading_date >= ? AND unloading_date <= ?
                        ''', (dealer_code, prev_prev_month_start, prev_prev_month_end))
                    nov_unloaded = cursor.fetchone()
                    
                    # Calculate November closing = October closing + November billing - November unloading
                    nov_closing_ppc = nov_opening_ppc + (nov_billed[0] or 0) - (nov_unloaded[0] or 0)
                    nov_closing_premium = nov_opening_premium + (nov_billed[1] or 0) - (nov_unloaded[1] or 0)
                    nov_closing_opc = nov_opening_opc + (nov_billed[2] or 0) - (nov_unloaded[2] or 0)
                    
                    # Save PREVIOUS MONTH's closing (November closing) - allow negative balances for inactive/other dealers
                    total_nov_closing = nov_closing_ppc + nov_closing_premium + nov_closing_opc
                    if abs(total_nov_closing) > 0.01:  # Save if non-zero (positive or negative)
                        dealers_to_save.append((
                            dealer_code,
                            dealer_name,
                            dealer_type,
                            nov_closing_ppc,
                            nov_closing_premium,
                            nov_closing_opc
                        ))
                        # Add to all_dealers for current processing
                        all_dealers[dealer_key] = {
                            'dealer_name': dealer_name,
                            'is_other': is_other
                        }
                
                # Save to opening_material_balance
                if dealers_to_save:
                    print(f"INFO: Saving {len(dealers_to_save)} dealers to opening_material_balance for {prev_month_year}")
                    for dealer_code, dealer_name, dealer_type, ppc, premium, opc in dealers_to_save:
                        cursor.execute('''
                            INSERT INTO opening_material_balance
                            (month_year, dealer_code, dealer_name, dealer_type, ppc_qty, premium_qty, opc_qty)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (prev_month_year, dealer_code, dealer_name, dealer_type, ppc, premium, opc))
                    db.conn.commit()
                    print(f"INFO: Successfully saved {len(dealers_to_save)} dealers for {prev_month_year}")
            
            # Also get dealers who had activity in previous month (sales or unloading)
            try:
                cursor.execute('''
                    SELECT DISTINCT dealer_code, dealer_name
                    FROM sales_data 
                    WHERE sale_date >= ? AND sale_date < ?
                ''', (prev_month_start, prev_month_end))
                for row in cursor.fetchall():
                    dealer_code = str(row[0])
                    if dealer_code not in all_dealers:
                        all_dealers[dealer_code] = {
                            'dealer_name': row[1],
                            'is_other': False
                        }
            except:
                pass
            
            try:
                cursor.execute('''
                    SELECT DISTINCT dealer_code, unloading_dealer, is_other_dealer
                    FROM vehicle_unloading 
                    WHERE unloading_date >= ? AND unloading_date < ?
                ''', (prev_month_start, prev_month_end))
                for row in cursor.fetchall():
                    is_other = bool(row[2])
                    if is_other or not row[0] or row[0] == 'OTHER':
                        dealer_key = row[1]
                    else:
                        dealer_key = str(row[0])
                    if dealer_key not in all_dealers:
                        all_dealers[dealer_key] = {
                            'dealer_name': row[1],
                            'is_other': is_other
                        }
            except:
                pass
            
            try:
                cursor.execute('''
                    SELECT DISTINCT dealer_name
                    FROM other_dealers_billing 
                    WHERE sale_date >= ? AND sale_date < ?
                ''', (prev_month_start, prev_month_end))
                for row in cursor.fetchall():
                    dealer_name = row[0]
                    if dealer_name not in all_dealers:
                        all_dealers[dealer_name] = {
                            'dealer_name': dealer_name,
                            'is_other': True
                        }
            except:
                pass
        
        # 2. Get dealers from sales_data (any date in month up to selected date)
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name
            FROM sales_data 
            WHERE sale_date >= ? AND sale_date <= ?
        ''', (month_start, selected_date))
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            if dealer_code not in all_dealers:
                all_dealers[dealer_code] = {
                    'dealer_name': row[1],
                    'is_other': False
                }
        
        # 3. Get dealers from vehicle_unloading (any date in month up to selected date)
        cursor.execute('''
            SELECT DISTINCT dealer_code, unloading_dealer, is_other_dealer
            FROM vehicle_unloading 
            WHERE unloading_date >= ? AND unloading_date <= ?
        ''', (month_start, selected_date))
        for row in cursor.fetchall():
            is_other = bool(row[2])
            # For other dealers, use the unloading_dealer name as the key
            # This ensures each unique other dealer is tracked separately
            if is_other or not row[0] or row[0] == 'OTHER':
                dealer_key = row[1]  # Use dealer name as key for other dealers
            else:
                dealer_key = str(row[0])  # Use dealer code for regular dealers
            
            if dealer_key not in all_dealers:
                all_dealers[dealer_key] = {
                    'dealer_name': row[1],
                    'is_other': is_other
                }
        
        # 4. Get dealers from other_dealers_billing (any date in month up to selected date)
        cursor.execute('''
            SELECT DISTINCT dealer_name
            FROM other_dealers_billing 
            WHERE sale_date >= ? AND sale_date <= ?
        ''', (month_start, selected_date))
        for row in cursor.fetchall():
            dealer_name = row[0]
            if dealer_name not in all_dealers:
                all_dealers[dealer_name] = {
                    'dealer_name': dealer_name,
                    'is_other': True
                }
        
        # Get billed quantities by dealer from sales_data for selected date only
        # (opening balance already includes cumulative before this date)
        cursor.execute('''
            SELECT dealer_code, dealer_name,
                   SUM(ppc_quantity) as ppc,
                   SUM(premium_quantity) as premium,
                   SUM(opc_quantity) as opc,
                   SUM(total_quantity) as total
            FROM sales_data 
            WHERE sale_date = ?
            GROUP BY dealer_code, dealer_name
        ''', (selected_date,))
        
        billed_map = {}
        for row in cursor.fetchall():
            billed_map[str(row[0])] = {
                'dealer_name': row[1],
                'ppc': row[2] or 0,
                'premium': row[3] or 0,
                'opc': row[4] or 0,
                'total': row[5] or 0
            }
        
        # Get billed quantities from other_dealers_billing for selected date only
        # (opening balance already includes cumulative before this date)
        cursor.execute('''
            SELECT dealer_name,
                   SUM(ppc_quantity) as ppc,
                   SUM(premium_quantity) as premium,
                   SUM(opc_quantity) as opc,
                   SUM(total_quantity) as total
            FROM other_dealers_billing 
            WHERE sale_date = ?
            GROUP BY dealer_name
        ''', (selected_date,))
        
        other_billed_map = {}
        for row in cursor.fetchall():
            other_billed_map[row[0]] = {
                'ppc': row[1] or 0,
                'premium': row[2] or 0,
                'opc': row[3] or 0,
                'total': row[4] or 0
            }
        
        # Get unloaded quantities by dealer from vehicle_unloading for selected date only
        # (opening balance already includes cumulative before this date)
        # Group by dealer_code to handle dealers with same name but different codes
        cursor.execute('''
            SELECT dealer_code,
                   unloading_dealer,
                   SUM(ppc_unloaded) as ppc,
                   SUM(premium_unloaded) as premium,
                   SUM(opc_unloaded) as opc,
                   SUM(unloaded_quantity) as total,
                   MAX(is_other_dealer) as is_other
            FROM vehicle_unloading 
            WHERE unloading_date = ?
            GROUP BY dealer_code, unloading_dealer
        ''', (selected_date,))
        
        unloading_map = {}  # Keyed by dealer_code for regular dealers
        unloading_map_by_name = {}  # Keyed by dealer_name for other dealers (no code)
        for row in cursor.fetchall():
            dealer_code = row[0]
            dealer_name = row[1]
            is_other = bool(row[6])
            unload_data = {
                'ppc': row[2] or 0,
                'premium': row[3] or 0,
                'opc': row[4] or 0,
                'total': row[5] or 0,
                'is_other': is_other
            }
            # For other dealers (is_other=True or dealer_code='OTHER'), use name-based map
            if is_other or dealer_code == 'OTHER' or not dealer_code:
                unloading_map_by_name[dealer_name] = unload_data
            else:
                unloading_map[dealer_code] = unload_data
        
        # Build dealer list with opening balance, billed, unloaded, and closing balance
        dealers = []
        
        # Cumulative totals for "Other Dealers"
        other_dealers_cumulative = {
            'opening_ppc': 0, 'opening_premium': 0, 'opening_opc': 0, 'opening_total': 0,
            'billed_ppc': 0, 'billed_premium': 0, 'billed_opc': 0, 'billed_total': 0,
            'unloaded_ppc': 0, 'unloaded_premium': 0, 'unloaded_opc': 0, 'unloaded_total': 0,
            'closing_ppc': 0, 'closing_premium': 0, 'closing_opc': 0, 'closing_total': 0
        }
        has_other_dealers = False
        
        # Process all dealers
        for dealer_code, dealer_info in all_dealers.items():
            dealer_name = dealer_info['dealer_name']
            is_other = dealer_info['is_other']
            
            # Get opening balance
            opening = get_dealer_opening_balance(cursor, dealer_name, selected_date, is_other_dealer=is_other, dealer_code=dealer_code)
            
            # Get today's billed
            if is_other:
                billed = other_billed_map.get(dealer_name, {'ppc': 0, 'premium': 0, 'opc': 0, 'total': 0})
            else:
                billed = billed_map.get(dealer_code, {'ppc': 0, 'premium': 0, 'opc': 0, 'total': 0})
            
            # Get today's unloaded - use dealer_code for regular dealers, name for other dealers
            if is_other:
                unloaded = unloading_map_by_name.get(dealer_name, {'ppc': 0, 'premium': 0, 'opc': 0, 'total': 0, 'is_other': True})
            else:
                unloaded = unloading_map.get(dealer_code, {'ppc': 0, 'premium': 0, 'opc': 0, 'total': 0, 'is_other': False})
            
            # Calculate closing
            closing_ppc = opening['ppc'] + billed['ppc'] - unloaded['ppc']
            closing_premium = opening['premium'] + billed['premium'] - unloaded['premium']
            closing_opc = opening['opc'] + billed['opc'] - unloaded['opc']
            closing_total = closing_ppc + closing_premium + closing_opc
            
            # Only include dealers with non-zero closing balance OR activity today
            has_activity = billed['total'] > 0 or unloaded['total'] > 0
            has_balance = abs(closing_total) > 0.01
            
            if has_activity or has_balance:
                if is_other:
                    # Aggregate into cumulative "Other Dealers" row
                    has_other_dealers = True
                    other_dealers_cumulative['opening_ppc'] += opening['ppc']
                    other_dealers_cumulative['opening_premium'] += opening['premium']
                    other_dealers_cumulative['opening_opc'] += opening['opc']
                    other_dealers_cumulative['opening_total'] += opening['total']
                    other_dealers_cumulative['billed_ppc'] += billed['ppc']
                    other_dealers_cumulative['billed_premium'] += billed['premium']
                    other_dealers_cumulative['billed_opc'] += billed['opc']
                    other_dealers_cumulative['billed_total'] += billed['total']
                    other_dealers_cumulative['unloaded_ppc'] += unloaded['ppc']
                    other_dealers_cumulative['unloaded_premium'] += unloaded['premium']
                    other_dealers_cumulative['unloaded_opc'] += unloaded['opc']
                    other_dealers_cumulative['unloaded_total'] += unloaded['total']
                    other_dealers_cumulative['closing_ppc'] += closing_ppc
                    other_dealers_cumulative['closing_premium'] += closing_premium
                    other_dealers_cumulative['closing_opc'] += closing_opc
                    other_dealers_cumulative['closing_total'] += closing_total
                else:
                    # Add regular dealer as individual row
                    dealers.append({
                        'dealer_code': dealer_code,
                        'dealer_name': dealer_name,
                        'is_other_dealer': False,
                        'opening_ppc': opening['ppc'],
                        'opening_premium': opening['premium'],
                        'opening_opc': opening['opc'],
                        'opening_total': opening['total'],
                        'billed_ppc': billed['ppc'],
                        'billed_premium': billed['premium'],
                        'billed_opc': billed['opc'],
                        'billed_total': billed['total'],
                        'unloaded_ppc': unloaded['ppc'],
                        'unloaded_premium': unloaded['premium'],
                        'unloaded_opc': unloaded['opc'],
                        'unloaded_total': unloaded['total'],
                        'closing_ppc': closing_ppc,
                        'closing_premium': closing_premium,
                        'closing_opc': closing_opc,
                        'closing_total': closing_total
                    })
        
        # Add cumulative "Other Dealers" row if there are any
        if has_other_dealers:
            dealers.append({
                'dealer_code': 'OTHER',
                'dealer_name': 'Other Dealers (Cumulative)',
                'is_other_dealer': True,
                'opening_ppc': other_dealers_cumulative['opening_ppc'],
                'opening_premium': other_dealers_cumulative['opening_premium'],
                'opening_opc': other_dealers_cumulative['opening_opc'],
                'opening_total': other_dealers_cumulative['opening_total'],
                'billed_ppc': other_dealers_cumulative['billed_ppc'],
                'billed_premium': other_dealers_cumulative['billed_premium'],
                'billed_opc': other_dealers_cumulative['billed_opc'],
                'billed_total': other_dealers_cumulative['billed_total'],
                'unloaded_ppc': other_dealers_cumulative['unloaded_ppc'],
                'unloaded_premium': other_dealers_cumulative['unloaded_premium'],
                'unloaded_opc': other_dealers_cumulative['unloaded_opc'],
                'unloaded_total': other_dealers_cumulative['unloaded_total'],
                'closing_ppc': other_dealers_cumulative['closing_ppc'],
                'closing_premium': other_dealers_cumulative['closing_premium'],
                'closing_opc': other_dealers_cumulative['closing_opc'],
                'closing_total': other_dealers_cumulative['closing_total']
            })
        
        # Get pending vehicles (billed but not fully unloaded) up to selected date
        # First, get all billing aggregated by truck and date (including other dealers billing)
        cursor.execute('''
            SELECT 
                truck_number,
                billing_date,
                dealers,
                SUM(billed_ppc) as billed_ppc,
                SUM(billed_premium) as billed_premium,
                SUM(billed_opc) as billed_opc
            FROM (
                SELECT 
                    s.truck_number,
                    s.sale_date as billing_date,
                    s.dealer_name as dealers,
                    s.ppc_quantity as billed_ppc,
                    s.premium_quantity as billed_premium,
                    s.opc_quantity as billed_opc
                FROM sales_data s
                WHERE s.sale_date >= ? AND s.sale_date <= ?
                  AND s.truck_number IS NOT NULL AND s.truck_number != ''
                UNION ALL
                SELECT 
                    o.truck_number,
                    o.sale_date as billing_date,
                    o.dealer_name as dealers,
                    o.ppc_quantity as billed_ppc,
                    o.premium_quantity as billed_premium,
                    o.opc_quantity as billed_opc
                FROM other_dealers_billing o
                WHERE o.sale_date >= ? AND o.sale_date <= ?
                  AND o.truck_number IS NOT NULL AND o.truck_number != ''
            )
            GROUP BY truck_number, billing_date
            ORDER BY billing_date, truck_number
        ''', (month_start, selected_date, month_start, selected_date))
        
        billing_data = cursor.fetchall()
        
        # Get all unloading aggregated by truck for current month only
        cursor.execute('''
            SELECT truck_number,
                   SUM(ppc_unloaded) as unloaded_ppc,
                   SUM(premium_unloaded) as unloaded_premium,
                   SUM(opc_unloaded) as unloaded_opc
            FROM vehicle_unloading
            WHERE unloading_date >= ? AND unloading_date <= ?
            GROUP BY truck_number
        ''', (month_start, selected_date))
        
        unloading_map_pending = {}
        for row in cursor.fetchall():
            unloading_map_pending[row[0]] = {
                'ppc': row[1] or 0,
                'premium': row[2] or 0,
                'opc': row[3] or 0
            }
        
        # Build pending vehicles list - check product-wise pending
        pending_vehicles = []
        truck_cumulative_billed = {}  # Track cumulative billing per truck
        truck_unloading_consumed = {}  # Track how much unloading has been attributed to previous billings
        
        # First, get opening balance vehicles to include in cumulative billing
        opening_balance_vehicles = {}
        has_current_month_pending = False
        try:
            cursor.execute('''
                SELECT vehicle_number, ppc_qty, premium_qty, opc_qty
                FROM pending_vehicle_unloading
                WHERE month_year = ?
            ''', (month_year,))
            rows = cursor.fetchall()
            if rows:
                has_current_month_pending = True
            for orow in rows:
                truck = orow[0]
                opening_balance_vehicles[truck] = {
                    'ppc': orow[1] or 0,
                    'premium': orow[2] or 0,
                    'opc': orow[3] or 0
                }
                # Initialize cumulative with opening balance
                truck_cumulative_billed[truck] = {
                    'ppc': orow[1] or 0,
                    'premium': orow[2] or 0,
                    'opc': orow[3] or 0
                }
                # Initialize consumed unloading for opening balance
                truck_unloading_consumed[truck] = {'ppc': 0, 'premium': 0, 'opc': 0}
                
                # Get unloading for this truck
                unloaded = unloading_map_pending.get(truck, {'ppc': 0, 'premium': 0, 'opc': 0})
                
                # Attribute unloading to opening balance first (FIFO)
                opening_unloaded_ppc = min(orow[1] or 0, unloaded['ppc'])
                opening_unloaded_premium = min(orow[2] or 0, unloaded['premium'])
                opening_unloaded_opc = min(orow[3] or 0, unloaded['opc'])
                
                # Track consumed unloading
                truck_unloading_consumed[truck] = {
                    'ppc': opening_unloaded_ppc,
                    'premium': opening_unloaded_premium,
                    'opc': opening_unloaded_opc
                }
        except:
            pass
        
        # If no pending vehicles for current month, calculate from previous month's closing
        if not has_current_month_pending:
            try:
                # Get previous month's pending vehicles and calculate their closing balance
                cursor.execute('''
                    SELECT vehicle_number, ppc_qty, premium_qty, opc_qty
                    FROM pending_vehicle_unloading
                    WHERE month_year = ?
                ''', (prev_month_year,))
                
                for orow in cursor.fetchall():
                    truck = orow[0]
                    prev_opening_ppc = orow[1] or 0
                    prev_opening_premium = orow[2] or 0
                    prev_opening_opc = orow[3] or 0
                    
                    # Get previous month's billing for this truck
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                        FROM sales_data
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                    ''', (truck, prev_month_start, prev_month_end))
                    prev_billed = cursor.fetchone()
                    
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                        FROM other_dealers_billing
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                    ''', (truck, prev_month_start, prev_month_end))
                    prev_other_billed = cursor.fetchone()
                    
                    # Get previous month's unloading for this truck
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                        FROM vehicle_unloading
                        WHERE truck_number = ? AND unloading_date >= ? AND unloading_date < ?
                    ''', (truck, prev_month_start, prev_month_end))
                    prev_unloaded = cursor.fetchone()
                    
                    # Calculate closing balance (which is opening for current month)
                    closing_ppc = prev_opening_ppc + (prev_billed[0] or 0) + (prev_other_billed[0] or 0) - (prev_unloaded[0] or 0)
                    closing_premium = prev_opening_premium + (prev_billed[1] or 0) + (prev_other_billed[1] or 0) - (prev_unloaded[1] or 0)
                    closing_opc = prev_opening_opc + (prev_billed[2] or 0) + (prev_other_billed[2] or 0) - (prev_unloaded[2] or 0)
                    
                    # Only add if there's pending material
                    if closing_ppc > 0.01 or closing_premium > 0.01 or closing_opc > 0.01:
                        opening_balance_vehicles[truck] = {
                            'ppc': closing_ppc,
                            'premium': closing_premium,
                            'opc': closing_opc
                        }
                        truck_cumulative_billed[truck] = {
                            'ppc': closing_ppc,
                            'premium': closing_premium,
                            'opc': closing_opc
                        }
                        truck_unloading_consumed[truck] = {'ppc': 0, 'premium': 0, 'opc': 0}
                        
                        # Get unloading for this truck in current month
                        unloaded = unloading_map_pending.get(truck, {'ppc': 0, 'premium': 0, 'opc': 0})
                        
                        # Attribute unloading to opening balance first (FIFO)
                        opening_unloaded_ppc = min(closing_ppc, unloaded['ppc'])
                        opening_unloaded_premium = min(closing_premium, unloaded['premium'])
                        opening_unloaded_opc = min(closing_opc, unloaded['opc'])
                        
                        truck_unloading_consumed[truck] = {
                            'ppc': opening_unloaded_ppc,
                            'premium': opening_unloaded_premium,
                            'opc': opening_unloaded_opc
                        }
                
                # Also check for vehicles billed in previous month (not in pending_vehicle_unloading)
                cursor.execute('''
                    SELECT DISTINCT truck_number FROM sales_data
                    WHERE sale_date >= ? AND sale_date < ? AND truck_number IS NOT NULL AND truck_number != ''
                    UNION
                    SELECT DISTINCT truck_number FROM other_dealers_billing
                    WHERE sale_date >= ? AND sale_date < ? AND truck_number IS NOT NULL AND truck_number != ''
                ''', (prev_month_start, prev_month_end, prev_month_start, prev_month_end))
                
                for (truck,) in cursor.fetchall():
                    if truck in opening_balance_vehicles:
                        continue  # Already processed
                    
                    # Get previous month's opening balance for this truck (from pending_vehicle_unloading)
                    cursor.execute('''
                        SELECT COALESCE(ppc_qty, 0), COALESCE(premium_qty, 0), COALESCE(opc_qty, 0)
                        FROM pending_vehicle_unloading
                        WHERE vehicle_number = ? AND month_year = ?
                    ''', (truck, prev_month_year))
                    prev_opening_row = cursor.fetchone()
                    prev_opening_ppc = prev_opening_row[0] if prev_opening_row else 0
                    prev_opening_premium = prev_opening_row[1] if prev_opening_row else 0
                    prev_opening_opc = prev_opening_row[2] if prev_opening_row else 0
                    
                    # Get previous month's billing for this truck
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                        FROM sales_data
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                    ''', (truck, prev_month_start, prev_month_end))
                    prev_billed = cursor.fetchone()
                    
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                        FROM other_dealers_billing
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                    ''', (truck, prev_month_start, prev_month_end))
                    prev_other_billed = cursor.fetchone()
                    
                    # Get previous month's unloading for this truck
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                        FROM vehicle_unloading
                        WHERE truck_number = ? AND unloading_date >= ? AND unloading_date < ?
                    ''', (truck, prev_month_start, prev_month_end))
                    prev_unloaded = cursor.fetchone()
                    
                    # Calculate closing balance (opening + billing - unloading)
                    closing_ppc = prev_opening_ppc + (prev_billed[0] or 0) + (prev_other_billed[0] or 0) - (prev_unloaded[0] or 0)
                    closing_premium = prev_opening_premium + (prev_billed[1] or 0) + (prev_other_billed[1] or 0) - (prev_unloaded[1] or 0)
                    closing_opc = prev_opening_opc + (prev_billed[2] or 0) + (prev_other_billed[2] or 0) - (prev_unloaded[2] or 0)
                    
                    # Cap negative values at 0
                    closing_ppc = max(0, closing_ppc)
                    closing_premium = max(0, closing_premium)
                    closing_opc = max(0, closing_opc)
                    
                    # Only add if there's pending material
                    if closing_ppc > 0.01 or closing_premium > 0.01 or closing_opc > 0.01:
                        opening_balance_vehicles[truck] = {
                            'ppc': closing_ppc,
                            'premium': closing_premium,
                            'opc': closing_opc
                        }
                        truck_cumulative_billed[truck] = {
                            'ppc': closing_ppc,
                            'premium': closing_premium,
                            'opc': closing_opc
                        }
                        truck_unloading_consumed[truck] = {'ppc': 0, 'premium': 0, 'opc': 0}
                        
                        # Get unloading for this truck in current month
                        unloaded = unloading_map_pending.get(truck, {'ppc': 0, 'premium': 0, 'opc': 0})
                        
                        opening_unloaded_ppc = min(closing_ppc, unloaded['ppc'])
                        opening_unloaded_premium = min(closing_premium, unloaded['premium'])
                        opening_unloaded_opc = min(closing_opc, unloaded['opc'])
                        
                        truck_unloading_consumed[truck] = {
                            'ppc': opening_unloaded_ppc,
                            'premium': opening_unloaded_premium,
                            'opc': opening_unloaded_opc
                        }
            except:
                pass
        
        for row in billing_data:
            truck_number = row[0]
            billing_date = row[1]
            dealer_names = row[2]  # Use different variable name to avoid overwriting dealers list
            billed_ppc = row[3] or 0
            billed_premium = row[4] or 0
            billed_opc = row[5] or 0
            
            # Get cumulative billed for this truck up to this date
            if truck_number not in truck_cumulative_billed:
                truck_cumulative_billed[truck_number] = {'ppc': 0, 'premium': 0, 'opc': 0}
            if truck_number not in truck_unloading_consumed:
                truck_unloading_consumed[truck_number] = {'ppc': 0, 'premium': 0, 'opc': 0}
            
            truck_cumulative_billed[truck_number]['ppc'] += billed_ppc
            truck_cumulative_billed[truck_number]['premium'] += billed_premium
            truck_cumulative_billed[truck_number]['opc'] += billed_opc
            
            # Get total unloading for this truck
            unloaded = unloading_map_pending.get(truck_number, {'ppc': 0, 'premium': 0, 'opc': 0})
            
            # Calculate remaining unloading available (not yet consumed by previous billings)
            remaining_unloaded_ppc = max(0, unloaded['ppc'] - truck_unloading_consumed[truck_number]['ppc'])
            remaining_unloaded_premium = max(0, unloaded['premium'] - truck_unloading_consumed[truck_number]['premium'])
            remaining_unloaded_opc = max(0, unloaded['opc'] - truck_unloading_consumed[truck_number]['opc'])
            
            # Attribute unloading to this billing (FIFO - first billed, first unloaded)
            this_billing_unloaded_ppc = min(billed_ppc, remaining_unloaded_ppc)
            this_billing_unloaded_premium = min(billed_premium, remaining_unloaded_premium)
            this_billing_unloaded_opc = min(billed_opc, remaining_unloaded_opc)
            
            # Update consumed unloading
            truck_unloading_consumed[truck_number]['ppc'] += this_billing_unloaded_ppc
            truck_unloading_consumed[truck_number]['premium'] += this_billing_unloaded_premium
            truck_unloading_consumed[truck_number]['opc'] += this_billing_unloaded_opc
            
            # Calculate pending for this specific billing
            pending_ppc = billed_ppc - this_billing_unloaded_ppc
            pending_premium = billed_premium - this_billing_unloaded_premium
            pending_opc = billed_opc - this_billing_unloaded_opc
            
            # Check if this specific billing has pending material
            if pending_ppc > 0.01 or pending_premium > 0.01 or pending_opc > 0.01:
                pending_vehicles.append({
                    'truck_number': truck_number,
                    'billing_date': billing_date,
                    'dealer_name': dealer_names,
                    'billed_ppc': billed_ppc,
                    'billed_premium': billed_premium,
                    'billed_opc': billed_opc,
                    'unloaded_ppc': this_billing_unloaded_ppc,
                    'unloaded_premium': this_billing_unloaded_premium,
                    'unloaded_opc': this_billing_unloaded_opc,
                    'is_manual': False
                })
        
        # Also get manually added pending vehicles from opening_material_balance page
        # If no current month pending vehicles, add from opening_balance_vehicles calculated earlier
        if has_current_month_pending:
            try:
                cursor.execute('''
                    SELECT p.vehicle_number, p.billing_date, p.dealer_code, 
                           p.ppc_qty, p.premium_qty, p.opc_qty,
                           COALESCE(u.unloaded_ppc, 0), COALESCE(u.unloaded_premium, 0), COALESCE(u.unloaded_opc, 0)
                    FROM pending_vehicle_unloading p
                    LEFT JOIN (
                        SELECT truck_number,
                               SUM(ppc_unloaded) as unloaded_ppc,
                               SUM(premium_unloaded) as unloaded_premium,
                               SUM(opc_unloaded) as unloaded_opc
                        FROM vehicle_unloading
                        WHERE unloading_date >= ? AND unloading_date <= ?
                        GROUP BY truck_number
                    ) u ON p.vehicle_number = u.truck_number
                    WHERE p.month_year = ?
                ''', (month_start, selected_date, month_year))
                
                # Get dealer names for manual vehicles
                for row in cursor.fetchall():
                    dealer_code = row[2]
                    # Look up dealer name
                    cursor.execute('SELECT dealer_name FROM sales_data WHERE dealer_code = ? LIMIT 1', (dealer_code,))
                    dealer_row = cursor.fetchone()
                    dealer_name = dealer_row[0] if dealer_row else f'Dealer {dealer_code}'
                    
                    billed_ppc = row[3] or 0
                    billed_premium = row[4] or 0
                    billed_opc = row[5] or 0
                    unloaded_ppc = row[6] or 0
                    unloaded_premium = row[7] or 0
                    unloaded_opc = row[8] or 0
                    
                    # Only add if there's pending material
                    pending_total = (billed_ppc - unloaded_ppc) + (billed_premium - unloaded_premium) + (billed_opc - unloaded_opc)
                    if pending_total > 0.01:
                        pending_vehicles.append({
                            'truck_number': row[0],
                            'billing_date': row[1] or 'Previous Month',
                            'dealer_name': dealer_name,
                            'billed_ppc': billed_ppc,
                            'billed_premium': billed_premium,
                            'billed_opc': billed_opc,
                            'unloaded_ppc': unloaded_ppc,
                            'unloaded_premium': unloaded_premium,
                            'unloaded_opc': unloaded_opc,
                            'is_manual': True
                        })
            except Exception as e:
                # Table might not exist
                pass
        else:
            # Add pending vehicles from previous month's closing (calculated in opening_balance_vehicles)
            # These need to be added separately as "Previous Month" entries
            for truck, balance in opening_balance_vehicles.items():
                # Get unloading for this truck in current month
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                    FROM vehicle_unloading
                    WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
                ''', (truck, month_start, selected_date))
                current_unloaded = cursor.fetchone()
                
                unloaded_ppc = current_unloaded[0] or 0
                unloaded_premium = current_unloaded[1] or 0
                unloaded_opc = current_unloaded[2] or 0
                
                pending_ppc = balance['ppc'] - unloaded_ppc
                pending_premium = balance['premium'] - unloaded_premium
                pending_opc = balance['opc'] - unloaded_opc
                
                # Only add if there's pending material
                if pending_ppc > 0.01 or pending_premium > 0.01 or pending_opc > 0.01:
                    # Get dealer name from previous month's billing
                    cursor.execute('''
                        SELECT dealer_name FROM sales_data WHERE truck_number = ? ORDER BY sale_date DESC LIMIT 1
                    ''', (truck,))
                    dealer_row = cursor.fetchone()
                    dealer_name = dealer_row[0] if dealer_row else 'Unknown'
                    
                    pending_vehicles.append({
                        'truck_number': truck,
                        'billing_date': 'Previous Month',
                        'dealer_name': dealer_name,
                        'billed_ppc': balance['ppc'],
                        'billed_premium': balance['premium'],
                        'billed_opc': balance['opc'],
                        'unloaded_ppc': unloaded_ppc,
                        'unloaded_premium': unloaded_premium,
                        'unloaded_opc': unloaded_opc,
                        'is_manual': True
                    })
        
        db.close()
        
        # Sort by dealer name
        dealers.sort(key=lambda x: x['dealer_name'])
        
        # Calculate totals
        totals = {
            'opening_ppc': sum(d['opening_ppc'] for d in dealers),
            'opening_premium': sum(d['opening_premium'] for d in dealers),
            'opening_opc': sum(d['opening_opc'] for d in dealers),
            'total_opening': sum(d['opening_total'] for d in dealers),
            'billed_ppc': sum(d['billed_ppc'] for d in dealers),
            'billed_premium': sum(d['billed_premium'] for d in dealers),
            'billed_opc': sum(d['billed_opc'] for d in dealers),
            'total_billed': sum(d['billed_total'] for d in dealers),
            'unloaded_ppc': sum(d['unloaded_ppc'] for d in dealers),
            'unloaded_premium': sum(d['unloaded_premium'] for d in dealers),
            'unloaded_opc': sum(d['unloaded_opc'] for d in dealers),
            'total_unloaded': sum(d['unloaded_total'] for d in dealers),
            'closing_ppc': sum(d['closing_ppc'] for d in dealers),
            'closing_premium': sum(d['closing_premium'] for d in dealers),
            'closing_opc': sum(d['closing_opc'] for d in dealers),
            'total_closing': sum(d['closing_total'] for d in dealers)
        }
        
        return jsonify({
            'success': True,
            'dealers': dealers,
            'totals': totals,
            'pending_vehicles': pending_vehicles,
            'date': selected_date
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_all_dealers', methods=['GET'])
def get_all_dealers():
    """Get all unique dealers from the database - one entry per dealer_code"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Group by dealer_code and pick the shortest dealer_name (without suffix like "(8632)")
        cursor.execute('''
            SELECT dealer_code, MIN(dealer_name) as dealer_name
            FROM sales_data 
            WHERE dealer_code IS NOT NULL AND dealer_code != ''
              AND dealer_name IS NOT NULL AND dealer_name != ''
            GROUP BY dealer_code
            ORDER BY MIN(dealer_name)
        ''')
        
        # First pass: collect all dealers
        raw_dealers = [(row[0], row[1]) for row in cursor.fetchall()]
        
        # Find duplicate names
        name_counts = {}
        for dealer_code, dealer_name in raw_dealers:
            name_counts[dealer_name] = name_counts.get(dealer_name, 0) + 1
        
        # Build final list, appending last 4 digits for duplicates
        dealers = []
        for dealer_code, dealer_name in raw_dealers:
            if name_counts.get(dealer_name, 0) > 1:
                # Append last 4 digits of dealer_code for duplicates
                display_name = f"{dealer_name} ({str(dealer_code)[-4:]})"
            else:
                display_name = dealer_name
            dealers.append({
                'dealer_code': dealer_code,
                'dealer_name': display_name
            })
        
        db.close()
        return jsonify({'success': True, 'dealers': dealers})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_vehicles_for_date', methods=['POST'])
def get_vehicles_for_date():
    """Get all vehicles billed on a specific date with their unloading details"""
    try:
        data = request.get_json()
        selected_date = data.get('date')
        
        if not selected_date:
            return jsonify({'success': False, 'message': 'Date is required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get all vehicles (invoices) for the selected date
        cursor.execute('''
            SELECT invoice_number, truck_number, dealer_code, dealer_name,
                   ppc_quantity, premium_quantity, opc_quantity, total_quantity,
                   ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value,
                   plant_depot
            FROM sales_data 
            WHERE sale_date = ? AND truck_number IS NOT NULL AND truck_number != ''
            ORDER BY truck_number, invoice_number
        ''', (selected_date,))
        
        vehicles_data = cursor.fetchall()
        
        # Get unloading details for these vehicles
        cursor.execute('''
            SELECT id, truck_number, invoice_number, dealer_code, unloading_dealer, 
                   unloading_point, ppc_unloaded, premium_unloaded, opc_unloaded, 
                   unloaded_quantity, notes
            FROM vehicle_unloading 
            WHERE unloading_date = ?
        ''', (selected_date,))
        
        unloading_data = cursor.fetchall()
        
        # Create unloading lookup by truck_number and invoice_number
        unloading_map = {}
        for row in unloading_data:
            key = f"{row[1]}_{row[2]}"  # truck_number_invoice_number
            if key not in unloading_map:
                unloading_map[key] = []
            unloading_map[key].append({
                'id': row[0],
                'truck_number': row[1],
                'invoice_number': row[2],
                'dealer_code': row[3],
                'unloading_dealer': row[4],
                'unloading_point': row[5],
                'ppc_unloaded': row[6] or 0,
                'premium_unloaded': row[7] or 0,
                'opc_unloaded': row[8] or 0,
                'unloaded_quantity': row[9] or 0,
                'notes': row[10]
            })
        
        vehicles = []
        for row in vehicles_data:
            invoice_number = row[0]
            truck_number = row[1]
            key = f"{truck_number}_{invoice_number}"
            
            vehicles.append({
                'invoice_number': invoice_number,
                'truck_number': truck_number,
                'dealer_code': row[2],
                'dealer_name': row[3],
                'ppc_quantity': row[4] or 0,
                'premium_quantity': row[5] or 0,
                'opc_quantity': row[6] or 0,
                'total_quantity': row[7] or 0,
                'ppc_purchase_value': row[8] or 0,
                'premium_purchase_value': row[9] or 0,
                'opc_purchase_value': row[10] or 0,
                'total_purchase_value': row[11] or 0,
                'plant_depot': row[12],
                'unloading_details': unloading_map.get(key, [])
            })
        
        db.close()
        
        return jsonify({
            'success': True,
            'vehicles': vehicles,
            'date': selected_date
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/get_consolidated_vehicles', methods=['POST'])
def get_consolidated_vehicles():
    """Get consolidated vehicles (one card per truck) with all billing and unloading details"""
    try:
        data = request.get_json()
        selected_date = data.get('date')
        
        if not selected_date:
            return jsonify({'success': False, 'message': 'Date is required'})
        
        from datetime import datetime
        selected_dt = datetime.strptime(selected_date, '%Y-%m-%d')
        month_start = selected_dt.replace(day=1).strftime('%Y-%m-%d')
        month_year = selected_dt.strftime('%Y-%m')
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get all invoices for the selected date grouped by truck
        cursor.execute('''
            SELECT truck_number, invoice_number, dealer_code, dealer_name,
                   ppc_quantity, premium_quantity, opc_quantity, total_quantity,
                   ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value,
                   plant_depot, sale_date, plant_description
            FROM sales_data 
            WHERE sale_date = ? AND truck_number IS NOT NULL AND truck_number != ''
            ORDER BY truck_number, dealer_name
        ''', (selected_date,))
        
        invoices_data = cursor.fetchall()
        
        # Get previous billings for trucks that are billed today (to check for re-billing)
        truck_numbers_today = list(set([row[0] for row in invoices_data]))
        previous_billings = {}
        
        # First, get opening balance vehicles for this month
        opening_balance_map = {}
        has_current_month_pending = False
        cursor.execute('''
            SELECT vehicle_number, billing_date, dealer_code, ppc_qty, premium_qty, opc_qty
            FROM pending_vehicle_unloading
            WHERE month_year = ?
        ''', (month_year,))
        rows = cursor.fetchall()
        if rows:
            has_current_month_pending = True
        for row in rows:
            truck = row[0]
            opening_balance_map[truck] = {
                'billing_date': row[1] or 'Previous Month',
                'dealer_code': row[2],
                'ppc': row[3] or 0,
                'premium': row[4] or 0,
                'opc': row[5] or 0,
                'total': (row[3] or 0) + (row[4] or 0) + (row[5] or 0)
            }
            # Add opening balance to previous_billings for trucks billed today
            if truck in truck_numbers_today:
                if truck not in previous_billings:
                    previous_billings[truck] = []
                previous_billings[truck].append({
                    'sale_date': row[1] or 'Opening',
                    'ppc': row[3] or 0,
                    'premium': row[4] or 0,
                    'opc': row[5] or 0,
                    'total': (row[3] or 0) + (row[4] or 0) + (row[5] or 0),
                    'dealers': 'Opening Balance'
                })
        
        # If no current month pending vehicles, calculate from previous month's closing
        if not has_current_month_pending:
            from dateutil.relativedelta import relativedelta
            from calendar import monthrange
            prev_month_dt = selected_dt - relativedelta(months=1)
            prev_month_year = prev_month_dt.strftime('%Y-%m')
            prev_month_start = prev_month_dt.replace(day=1).strftime('%Y-%m-%d')
            # Get last day of previous month
            last_day = monthrange(prev_month_dt.year, prev_month_dt.month)[1]
            prev_month_end = prev_month_dt.replace(day=last_day).strftime('%Y-%m-%d')
            
            # Check if previous month has entries in pending_vehicle_unloading
            cursor.execute('''
                SELECT vehicle_number, billing_date, dealer_code, ppc_qty, premium_qty, opc_qty
                FROM pending_vehicle_unloading
                WHERE month_year = ?
            ''', (prev_month_year,))
            prev_month_entries = cursor.fetchall()
            
            if prev_month_entries:
                # Use previous month's closing as current month's opening
                for row in prev_month_entries:
                    truck = row[0]
                    opening_balance_map[truck] = {
                        'billing_date': 'Previous Month',
                        'dealer_code': row[2],
                        'ppc': row[3] or 0,
                        'premium': row[4] or 0,
                        'opc': row[5] or 0,
                        'total': (row[3] or 0) + (row[4] or 0) + (row[5] or 0)
                    }
                    if truck in truck_numbers_today:
                        if truck not in previous_billings:
                            previous_billings[truck] = []
                        previous_billings[truck].append({
                            'sale_date': 'Opening',
                            'ppc': row[3] or 0,
                            'premium': row[4] or 0,
                            'opc': row[5] or 0,
                            'total': (row[3] or 0) + (row[4] or 0) + (row[5] or 0),
                            'dealers': 'Opening Balance'
                        })
            else:
                # No previous month entries - need to calculate and save them
                # This happens when viewing a new month for the first time
                # Calculate previous month's closing balance for all vehicles
                print(f"INFO: No {prev_month_year} entries found. Calculating and saving closing balances...")
                
                # Get all vehicles that had transactions in previous month
                cursor.execute('''
                    SELECT DISTINCT truck_number FROM sales_data
                    WHERE sale_date >= ? AND sale_date <= ?
                    UNION
                    SELECT DISTINCT truck_number FROM other_dealers_billing
                    WHERE sale_date >= ? AND sale_date <= ?
                    UNION
                    SELECT DISTINCT truck_number FROM vehicle_unloading
                    WHERE unloading_date >= ? AND unloading_date <= ?
                ''', (prev_month_start, prev_month_end, prev_month_start, prev_month_end, prev_month_start, prev_month_end))
                
                vehicles_to_process = [row[0] for row in cursor.fetchall()]
                
                # Also get vehicles from the month before previous month's pending
                prev_prev_month_dt = prev_month_dt - relativedelta(months=1)
                prev_prev_month_year = prev_prev_month_dt.strftime('%Y-%m')
                cursor.execute('''
                    SELECT DISTINCT vehicle_number FROM pending_vehicle_unloading
                    WHERE month_year = ?
                ''', (prev_prev_month_year,))
                for row in cursor.fetchall():
                    if row[0] not in vehicles_to_process:
                        vehicles_to_process.append(row[0])
                
                # Calculate closing balance for each vehicle
                # IMPORTANT: pending_vehicle_unloading stores CLOSING balances
                # Entry with month_year='2025-11' contains OCTOBER 31 closing (manually added as Nov opening)
                # We need to calculate NOVEMBER closing first, then use it as DECEMBER opening
                vehicles_to_save = []
                for truck in vehicles_to_process:
                    # Get October closing (stored in Nov entry) - this is the opening for November
                    # truck can be either full vehicle_number or just truck_number (last 4 digits)
                    cursor.execute('''
                        SELECT ppc_qty, premium_qty, opc_qty, dealer_code
                        FROM pending_vehicle_unloading
                        WHERE (vehicle_number = ? OR vehicle_number LIKE '%' || ?) AND month_year = ?
                    ''', (truck, truck, prev_prev_month_year))
                    oct_closing_row = cursor.fetchone()
                    nov_opening_ppc = oct_closing_row[0] if oct_closing_row else 0
                    nov_opening_premium = oct_closing_row[1] if oct_closing_row else 0
                    nov_opening_opc = oct_closing_row[2] if oct_closing_row else 0
                    dealer_code = oct_closing_row[3] if oct_closing_row else None
                    
                    # Calculate November closing = October closing + November billing - November unloading
                    prev_prev_month_start = prev_prev_month_dt.replace(day=1).strftime('%Y-%m-%d')
                    last_day_prev_prev = monthrange(prev_prev_month_dt.year, prev_prev_month_dt.month)[1]
                    prev_prev_month_end = prev_prev_month_dt.replace(day=last_day_prev_prev).strftime('%Y-%m-%d')
                    
                    # Get November billing (all November transactions)
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                        FROM sales_data
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ?
                    ''', (truck, prev_prev_month_start, prev_prev_month_end))
                    nov_billed = cursor.fetchone()
                    
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                        FROM other_dealers_billing
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ?
                    ''', (truck, prev_prev_month_start, prev_prev_month_end))
                    nov_other_billed = cursor.fetchone()
                    
                    # Get November unloading (all November transactions)
                    cursor.execute('''
                        SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                        FROM vehicle_unloading
                        WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
                    ''', (truck, prev_prev_month_start, prev_prev_month_end))
                    nov_unloaded = cursor.fetchone()
                    
                    # November closing = November opening + November billing - November unloading
                    nov_closing_ppc = max(0, nov_opening_ppc + (nov_billed[0] or 0) + (nov_other_billed[0] or 0) - (nov_unloaded[0] or 0))
                    nov_closing_premium = max(0, nov_opening_premium + (nov_billed[1] or 0) + (nov_other_billed[1] or 0) - (nov_unloaded[1] or 0))
                    nov_closing_opc = max(0, nov_opening_opc + (nov_billed[2] or 0) + (nov_other_billed[2] or 0) - (nov_unloaded[2] or 0))
                    
                    # Save PREVIOUS MONTH's closing (November closing) for current month (December) to use
                    total_nov_closing = nov_closing_ppc + nov_closing_premium + nov_closing_opc
                    if total_nov_closing > 0.01:
                        vehicles_to_save.append((truck, dealer_code, nov_closing_ppc, nov_closing_premium, nov_closing_opc))
                        opening_balance_map[truck] = {
                            'billing_date': 'Previous Month',
                            'dealer_code': dealer_code,
                            'ppc': nov_closing_ppc,
                            'premium': nov_closing_premium,
                            'opc': nov_closing_opc,
                            'total': total_nov_closing
                        }
                        if truck in truck_numbers_today:
                            if truck not in previous_billings:
                                previous_billings[truck] = []
                            previous_billings[truck].append({
                                'sale_date': 'Opening',
                                'ppc': nov_closing_ppc,
                                'premium': nov_closing_premium,
                                'opc': nov_closing_opc,
                                'total': total_nov_closing,
                                'dealers': 'Opening Balance'
                            })
                
                # Save to pending_vehicle_unloading for future use
                # IMPORTANT: We're saving PREVIOUS MONTH's CLOSING (e.g., Nov closing when viewing Dec)
                # This will be used as CURRENT MONTH's OPENING (e.g., Dec opening)
                if vehicles_to_save:
                    print(f"INFO: Saving {len(vehicles_to_save)} vehicles to pending_vehicle_unloading for {prev_month_year}")
                    for truck, dealer_code, ppc, premium, opc in vehicles_to_save:
                        cursor.execute('''
                            INSERT INTO pending_vehicle_unloading 
                            (month_year, vehicle_number, billing_date, dealer_code, ppc_qty, premium_qty, opc_qty)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (prev_month_year, truck, prev_prev_month_end, dealer_code, ppc, premium, opc))
                    db.conn.commit()
                    print(f"INFO: Successfully saved {len(vehicles_to_save)} vehicles for {prev_month_year}")
        
        if truck_numbers_today:
            placeholders = ','.join(['?' for _ in truck_numbers_today])
            # Include both sales_data and other_dealers_billing for previous billings
            cursor.execute(f'''
                SELECT truck_number, sale_date, 
                       SUM(ppc) as ppc, SUM(premium) as premium, 
                       SUM(opc) as opc, SUM(total) as total,
                       GROUP_CONCAT(DISTINCT dealers) as dealers
                FROM (
                    SELECT truck_number, sale_date, 
                           ppc_quantity as ppc, premium_quantity as premium, 
                           opc_quantity as opc, total_quantity as total,
                           dealer_name as dealers
                    FROM sales_data 
                    WHERE truck_number IN ({placeholders}) 
                      AND sale_date >= ? AND sale_date < ?
                    UNION ALL
                    SELECT truck_number, sale_date, 
                           ppc_quantity as ppc, premium_quantity as premium, 
                           opc_quantity as opc, total_quantity as total,
                           dealer_name as dealers
                    FROM other_dealers_billing 
                    WHERE truck_number IN ({placeholders}) 
                      AND sale_date >= ? AND sale_date < ?
                )
                GROUP BY truck_number, sale_date
                ORDER BY truck_number, sale_date
            ''', (*truck_numbers_today, month_start, selected_date, *truck_numbers_today, month_start, selected_date))
            
            for row in cursor.fetchall():
                truck = row[0]
                if truck not in previous_billings:
                    previous_billings[truck] = []
                previous_billings[truck].append({
                    'sale_date': row[1],
                    'ppc': row[2] or 0,
                    'premium': row[3] or 0,
                    'opc': row[4] or 0,
                    'total': row[5] or 0,
                    'dealers': row[6]
                })
        
        # Get ALL unloading details for trucks billed today (current month only)
        all_unloading_map = {}
        if truck_numbers_today:
            placeholders = ','.join(['?' for _ in truck_numbers_today])
            cursor.execute(f'''
                SELECT id, truck_number, unloading_dealer, unloading_point, 
                       ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                       notes, dealer_code, is_other_dealer, unloading_date, plant_depot
                FROM vehicle_unloading 
                WHERE truck_number IN ({placeholders}) AND unloading_date >= ? AND unloading_date <= ?
            ''', (*truck_numbers_today, month_start, selected_date))
            
            for row in cursor.fetchall():
                truck = row[1]
                if truck not in all_unloading_map:
                    all_unloading_map[truck] = []
                all_unloading_map[truck].append({
                    'id': row[0],
                    'truck_number': row[1],
                    'unloading_dealer': row[2],
                    'unloading_point': row[3],
                    'ppc_unloaded': row[4] or 0,
                    'premium_unloaded': row[5] or 0,
                    'opc_unloaded': row[6] or 0,
                    'unloaded_quantity': row[7] or 0,
                    'notes': row[8],
                    'dealer_code': row[9],
                    'is_other_dealer': bool(row[10]) if row[10] is not None else False,
                    'unloading_date': row[11],
                    'plant_depot': row[12]
                })
        
        # Get unloading details for today only (for display)
        cursor.execute('''
            SELECT id, truck_number, unloading_dealer, unloading_point, 
                   ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                   notes, dealer_code, is_other_dealer, unloading_date, plant_depot
            FROM vehicle_unloading 
            WHERE unloading_date = ?
        ''', (selected_date,))
        
        unloading_data = cursor.fetchall()
        
        # Get other dealer billing for the date
        cursor.execute('''
            SELECT id, truck_number, dealer_name, invoice_number, plant_depot,
                   ppc_quantity, premium_quantity, opc_quantity, total_quantity,
                   ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value
            FROM other_dealers_billing 
            WHERE sale_date = ?
        ''', (selected_date,))
        
        other_billing_data = cursor.fetchall()
        
        # Create unloading lookup by truck_number (today's unloading)
        unloading_map = {}
        for row in unloading_data:
            truck = row[1]
            if truck not in unloading_map:
                unloading_map[truck] = []
            unloading_map[truck].append({
                'id': row[0],
                'truck_number': row[1],
                'unloading_dealer': row[2],
                'unloading_point': row[3],
                'ppc_unloaded': row[4] or 0,
                'premium_unloaded': row[5] or 0,
                'opc_unloaded': row[6] or 0,
                'unloaded_quantity': row[7] or 0,
                'notes': row[8],
                'dealer_code': row[9],
                'is_other_dealer': bool(row[10]) if row[10] is not None else False,
                'unloading_date': row[11],
                'plant_depot': row[12]
            })
        
        # Create other billing lookup by truck_number
        other_billing_map = {}
        for row in other_billing_data:
            truck = row[1]
            if truck not in other_billing_map:
                other_billing_map[truck] = []
            other_billing_map[truck].append({
                'id': row[0],
                'truck_number': row[1],
                'dealer_name': row[2],
                'invoice_number': row[3],
                'plant_depot': row[4],
                'ppc_quantity': row[5] or 0,
                'premium_quantity': row[6] or 0,
                'opc_quantity': row[7] or 0,
                'total_quantity': row[8] or 0,
                'ppc_value': row[9] or 0,
                'premium_value': row[10] or 0,
                'opc_value': row[11] or 0,
                'total_value': row[12] or 0
            })
        
        # Build vehicles list - consolidate same truck on same day, but SEPARATE by plant_depot
        # This ensures PLANT and DEPOT billing for same truck are shown as separate cards
        vehicles_list = []
        trucks_today = {}  # Consolidate by truck_number + plant_depot for today's billing
        
        for row in invoices_data:
            truck_number = row[0]
            invoice_number = row[1]
            ppc_qty = row[4] or 0
            premium_qty = row[5] or 0
            opc_qty = row[6] or 0
            total_qty = row[7] or 0
            plant_depot = row[12] or 'PLANT'  # Default to PLANT if not specified
            
            # Use truck_number + plant_depot as key to separate PLANT and DEPOT
            card_key = f"{truck_number}_{plant_depot}"
            
            if card_key not in trucks_today:
                # Filter unloading details by dealer_code matching this card's dealers
                trucks_today[card_key] = {
                    'truck_number': truck_number,
                    'card_key': card_key,
                    'plant_depot': plant_depot,
                    'invoices': [],
                    'dealer_codes': set(),  # Track dealer codes for this card
                    'total_ppc': 0,
                    'total_premium': 0,
                    'total_opc': 0,
                    'total_quantity': 0,
                    'total_value': 0,
                    'billing_date': selected_date,
                    'unloading_details': [],  # Will be filtered later
                    'other_billing': other_billing_map.get(truck_number, [])
                }
            
            # Add invoice to this card's list
            plant_desc = row[14]
            dealer_code = row[2]
            trucks_today[card_key]['invoices'].append({
                'invoice_number': invoice_number,
                'dealer_code': dealer_code,
                'dealer_name': row[3],
                'ppc_quantity': ppc_qty,
                'premium_quantity': premium_qty,
                'opc_quantity': opc_qty,
                'total_quantity': total_qty,
                'total_value': row[11] or 0,
                'plant_depot': plant_depot,
                'plant_description': plant_desc,
                'depot_abbr': get_depot_abbreviation(plant_desc) if plant_depot == 'DEPOT' else None
            })
            
            # Track dealer codes for this card
            if dealer_code:
                trucks_today[card_key]['dealer_codes'].add(dealer_code)
            
            # Accumulate totals
            trucks_today[card_key]['total_ppc'] += ppc_qty
            trucks_today[card_key]['total_premium'] += premium_qty
            trucks_today[card_key]['total_opc'] += opc_qty
            trucks_today[card_key]['total_quantity'] += total_qty
            trucks_today[card_key]['total_value'] += row[11] or 0
        
        # Add cards for trucks that only have other_dealers_billing on selected date (no sales_data)
        for truck_number, other_billings in other_billing_map.items():
            # Check if this truck already has a card from sales_data
            truck_has_card = any(truck_number == td['truck_number'] for td in trucks_today.values())
            if not truck_has_card and other_billings:
                # Create a card for this truck based on other_dealers_billing
                for ob in other_billings:
                    plant_depot = ob.get('plant_depot', 'PLANT') or 'PLANT'
                    card_key = f"{truck_number}_{plant_depot}"
                    
                    if card_key not in trucks_today:
                        trucks_today[card_key] = {
                            'truck_number': truck_number,
                            'card_key': card_key,
                            'plant_depot': plant_depot,
                            'invoices': [],
                            'dealer_codes': set(),
                            'total_ppc': 0,
                            'total_premium': 0,
                            'total_opc': 0,
                            'total_quantity': 0,
                            'total_value': 0,
                            'billing_date': selected_date,
                            'unloading_details': unloading_map.get(truck_number, []),
                            'other_billing': other_billings,
                            'other_billing_added': True,  # Flag to prevent double-adding
                            # Initialize card pending amounts (will be updated below)
                            'card_pending_ppc': 0,
                            'card_pending_premium': 0,
                            'card_pending_opc': 0
                        }
                    
                    # Add totals from other_dealers_billing
                    trucks_today[card_key]['total_ppc'] += ob.get('ppc_quantity', 0)
                    trucks_today[card_key]['total_premium'] += ob.get('premium_quantity', 0)
                    trucks_today[card_key]['total_opc'] += ob.get('opc_quantity', 0)
                    trucks_today[card_key]['total_quantity'] += ob.get('total_quantity', 0)
                    trucks_today[card_key]['total_value'] += ob.get('total_value', 0)
                    # Also add to card pending (other_dealers_billing is all pending until unloaded)
                    trucks_today[card_key]['card_pending_ppc'] += ob.get('ppc_quantity', 0)
                    trucks_today[card_key]['card_pending_premium'] += ob.get('premium_quantity', 0)
                    trucks_today[card_key]['card_pending_opc'] += ob.get('opc_quantity', 0)
        
        # Assign unloading details to cards
        # For trucks with only one card (either PLANT or DEPOT), show all unloading
        # For trucks with multiple cards (both PLANT and DEPOT), filter by dealer_code
        # DEPOT cards get unloading that doesn't match any PLANT card's dealer_codes
        truck_card_count = {}
        truck_plant_dealer_codes = {}  # Track PLANT card dealer_codes per truck
        for card_key, truck_data in trucks_today.items():
            truck_number = truck_data['truck_number']
            truck_card_count[truck_number] = truck_card_count.get(truck_number, 0) + 1
            # Collect PLANT card dealer_codes
            if truck_data.get('plant_depot') == 'PLANT':
                if truck_number not in truck_plant_dealer_codes:
                    truck_plant_dealer_codes[truck_number] = set()
                for dc in truck_data.get('dealer_codes', []):
                    truck_plant_dealer_codes[truck_number].add(str(dc))
        
        # DEBUG: Log for HR55AZ1569, HR58D1569, and HR55AK1628
        for debug_truck in ['HR55AZ1569', 'HR58D1569', 'HR55AK1628']:
            if debug_truck in [td['truck_number'] for td in trucks_today.values()]:
                print(f"DEBUG {debug_truck}: truck_card_count={truck_card_count.get(debug_truck)}")
                print(f"DEBUG {debug_truck}: truck_plant_dealer_codes={truck_plant_dealer_codes.get(debug_truck)}")
                print(f"DEBUG {debug_truck}: unloading_map={unloading_map.get(debug_truck)}")
        
        # First pass: assign unloading_details (will be done after _pending cards are created)
        # For now, just initialize to empty
        for card_key, truck_data in trucks_today.items():
            truck_data['unloading_details'] = []
        
        # Helper function to transform unloading record field names for frontend
        def transform_unloading_record(unload):
            """Transform unloading_dealer and unloading_point to dealer_name and point"""
            return {
                'id': unload.get('id'),
                'truck_number': unload.get('truck_number'),
                'dealer_code': unload.get('dealer_code'),
                'dealer_name': unload.get('unloading_dealer'),
                'point': unload.get('unloading_point'),
                'unloading_dealer': unload.get('unloading_dealer'),  # Keep original for compatibility
                'unloading_point': unload.get('unloading_point'),    # Keep original for compatibility
                'ppc_unloaded': unload.get('ppc_unloaded', 0),
                'premium_unloaded': unload.get('premium_unloaded', 0),
                'opc_unloaded': unload.get('opc_unloaded', 0),
                'unloaded_quantity': unload.get('unloaded_quantity', 0),
                'unloading_date': unload.get('unloading_date'),
                'plant_depot': unload.get('plant_depot'),
                'notes': unload.get('notes', ''),
                'is_other_dealer': unload.get('is_other_dealer', False)
            }
        
        # Process unloading assignment in a separate loop after all cards are created
        def assign_unloading_to_cards():
            for card_key, truck_data in trucks_today.items():
                truck_number = truck_data['truck_number']
                dealer_codes = truck_data['dealer_codes']
                # Convert dealer_codes to strings for comparison
                dealer_codes_str = set(str(dc) for dc in dealer_codes)
                all_unloading = unloading_map.get(truck_number, [])
                plant_depot = truck_data.get('plant_depot', 'PLANT')
                
                # Skip _pending cards - they already have unloading assigned during creation
                if card_key.endswith('_pending'):
                    continue
                
                # Check if this truck has a "_pending" card (previous day pending)
                pending_card_key = None
                for ck in trucks_today.keys():
                    if ck.endswith('_pending') and trucks_today[ck]['truck_number'] == truck_number:
                        pending_card_key = ck
                        break
                
                # If this card is today's billing and there's a pending card, assign unloading
                # that wasn't already assigned to the pending card (FIFO logic)
                if pending_card_key:
                    pending_card = trucks_today[pending_card_key]
                    # Get the IDs of unloading records assigned to the pending card
                    pending_card_unloading_ids = set(u.get('id') for u in pending_card.get('unloading_details', []))
                    
                    # For Today cards, only assign unloading from the selected date
                    # Historical unloading should be consumed by Prev Day card via FIFO
                    today_card_unloading = []
                    for unload in all_unloading:
                        # Only include unloading from selected date (today)
                        # AND not already assigned to pending card
                        if (unload.get('unloading_date') == selected_date and 
                            unload.get('id') not in pending_card_unloading_ids):
                            today_card_unloading.append(transform_unloading_record(unload))
                    
                    truck_data['unloading_details'] = today_card_unloading
                # If this truck has only one card, show ALL unloading for the truck
                # If this truck has multiple cards (PLANT + DEPOT), filter by plant_depot first, then dealer_code
                elif truck_card_count.get(truck_number, 1) == 1:
                    # Single card - show all unloading
                    truck_data['unloading_details'] = [transform_unloading_record(u) for u in all_unloading]
                else:
                    # Multiple cards - filter by plant_depot and dealer_code
                    filtered_unloading = []
                    plant_codes = truck_plant_dealer_codes.get(truck_number, set())
                    for u in all_unloading:
                        u_dealer_code = str(u.get('dealer_code', '')) if u.get('dealer_code') else ''
                        u_plant_depot = u.get('plant_depot', '').upper() if u.get('plant_depot') else ''
                        
                        if truck_number in ['HR55AZ1569', 'HR58D1569']:
                            print(f"DEBUG FILTERING card={card_key}, card_plant_depot={plant_depot}, u_plant_depot={u_plant_depot}, match={u_plant_depot == plant_depot}")
                        
                        # First priority: Match by plant_depot if unloading has it specified
                        if u_plant_depot:
                            if u_plant_depot == plant_depot:
                                filtered_unloading.append(transform_unloading_record(u))
                        else:
                            # Legacy unloading without plant_depot - use dealer_code matching
                            if plant_depot == 'DEPOT':
                                # DEPOT card gets unloading that:
                                # 1. Matches its own dealer_codes, OR
                                # 2. Doesn't match any PLANT card's dealer_codes (unassigned unloading goes to DEPOT)
                                # 3. Has no dealer_code (legacy data)
                                if u_dealer_code in dealer_codes_str or u_dealer_code not in plant_codes or not u_dealer_code:
                                    filtered_unloading.append(transform_unloading_record(u))
                            else:
                                # PLANT card - only gets unloading matching its dealer_codes
                                if u_dealer_code in dealer_codes_str or not u_dealer_code:
                                    filtered_unloading.append(transform_unloading_record(u))
                    truck_data['unloading_details'] = filtered_unloading
                    
                    if truck_number in ['HR55AZ1569', 'HR58D1569']:
                        print(f"DEBUG FILTERED card={card_key}, filtered_count={len(filtered_unloading)}")
                        print(f"DEBUG ASSIGNED unloading_details to truck_data, count={len(truck_data.get('unloading_details', []))}")
                
                # Convert set to list for JSON serialization
                truck_data['dealer_codes'] = list(dealer_codes)
        
        # Create a set of actual truck numbers billed today (for checking if a truck is billed today)
        actual_trucks_billed_today = set(td['truck_number'] for td in trucks_today.values())
        
        # Add cards for vehicles billed earlier in the current month (not today) that still have pending material
        # Get vehicles billed earlier in the month
        cursor.execute('''
            SELECT DISTINCT truck_number
            FROM sales_data
            WHERE sale_date >= ? AND sale_date < ? AND truck_number IS NOT NULL AND truck_number != ''
        ''', (month_start, selected_date))
        earlier_billed_trucks = [row[0] for row in cursor.fetchall()]
        
        # Also include vehicles from opening_balance_map that are not in earlier_billed_trucks
        # This handles the case where selected_date = month_start (e.g., Jan 1)
        # and there are no trucks billed earlier in the month, but there are opening balances
        for truck_number in opening_balance_map.keys():
            if truck_number not in earlier_billed_trucks:
                earlier_billed_trucks.append(truck_number)
        
        for truck_number in earlier_billed_trucks:
            # For trucks NOT billed today: show all pending as a card
            # For trucks billed today: add pending from earlier to the existing card
            is_billed_today = truck_number in actual_trucks_billed_today
            
            # Calculate pending balance for this truck (from earlier billing, not including today)
            opening_ppc = opening_balance_map.get(truck_number, {}).get('ppc', 0)
            opening_premium = opening_balance_map.get(truck_number, {}).get('premium', 0)
            opening_opc = opening_balance_map.get(truck_number, {}).get('opc', 0)
            
            # Get current month's billing (up to but NOT including selected date for trucks billed today)
            billing_end_date = selected_date if not is_billed_today else selected_date
            billing_end_op = '<=' if not is_billed_today else '<'
            
            cursor.execute(f'''
                SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), 
                       COALESCE(SUM(opc_quantity), 0)
                FROM sales_data
                WHERE truck_number = ? AND sale_date >= ? AND sale_date {billing_end_op} ?
            ''', (truck_number, month_start, selected_date))
            month_billed = cursor.fetchone()
            
            # Get current month's other_dealers_billing
            cursor.execute(f'''
                SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), 
                       COALESCE(SUM(opc_quantity), 0)
                FROM other_dealers_billing
                WHERE truck_number = ? AND sale_date >= ? AND sale_date {billing_end_op} ?
            ''', (truck_number, month_start, selected_date))
            month_other_billed = cursor.fetchone()
            
            # Get current month's unloading
            # Always exclude today's unloading from FIFO calculation to prevent it from being
            # consumed by earlier billings. Today's unloading will be subtracted in remaining calculation.
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), 
                       COALESCE(SUM(opc_unloaded), 0)
                FROM vehicle_unloading
                WHERE truck_number = ? AND unloading_date >= ? AND unloading_date < ?
            ''', (truck_number, month_start, selected_date))
            month_unloaded = cursor.fetchone()
            
            # Calculate pending balance (for billing before today if billed today)
            pending_ppc = opening_ppc + (month_billed[0] or 0) + (month_other_billed[0] or 0) - (month_unloaded[0] or 0)
            pending_premium = opening_premium + (month_billed[1] or 0) + (month_other_billed[1] or 0) - (month_unloaded[1] or 0)
            pending_opc = opening_opc + (month_billed[2] or 0) + (month_other_billed[2] or 0) - (month_unloaded[2] or 0)
            
            # Check if this truck has unloading on the selected date
            cursor.execute('''
                SELECT COUNT(*) FROM vehicle_unloading
                WHERE truck_number = ? AND unloading_date = ?
            ''', (truck_number, selected_date))
            has_unloading_today = cursor.fetchone()[0] > 0
            
            # Add if there's positive pending material OR if there's unloading on selected date
            if pending_ppc > 0.01 or pending_premium > 0.01 or pending_opc > 0.01 or has_unloading_today:
                    # Get all billing info for this truck in current month (sorted by date ASC for FIFO)
                    # Include both sales_data and other_dealers_billing
                    cursor.execute('''
                        SELECT invoice_number, dealer_code, dealer_name, plant_depot, sale_date,
                               ppc_quantity, premium_quantity, opc_quantity, total_quantity,
                               ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value,
                               plant_description
                        FROM sales_data
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                        UNION ALL
                        SELECT 'OTHER' as invoice_number, '' as dealer_code, dealer_name, 
                               CASE WHEN plant_depot = 'Plant' OR plant_depot = 'PLANT' THEN 'PLANT' ELSE 'DEPOT' END as plant_depot, 
                               sale_date,
                               ppc_quantity, premium_quantity, opc_quantity, total_quantity,
                               0 as ppc_purchase_value, 0 as premium_purchase_value, 0 as opc_purchase_value, total_purchase_value,
                               '' as plant_description
                        FROM other_dealers_billing
                        WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                        ORDER BY sale_date ASC
                    ''', (truck_number, month_start, selected_date, truck_number, month_start, selected_date))
                    billing_rows = cursor.fetchall()
                    
                    if billing_rows:
                        # FIFO: Consume in date order - opening first, then all billing by date
                        unloaded_ppc = month_unloaded[0] or 0
                        unloaded_premium = month_unloaded[1] or 0
                        unloaded_opc = month_unloaded[2] or 0
                        
                        # First consume opening balance
                        consume_from_opening_ppc = min(opening_ppc, unloaded_ppc)
                        consume_from_opening_premium = min(opening_premium, unloaded_premium)
                        consume_from_opening_opc = min(opening_opc, unloaded_opc)
                        
                        remaining_to_consume_ppc = unloaded_ppc - consume_from_opening_ppc
                        remaining_to_consume_premium = unloaded_premium - consume_from_opening_premium
                        remaining_to_consume_opc = unloaded_opc - consume_from_opening_opc
                        
                        # Now consume invoices in FIFO order (by date) until we've consumed enough
                        # billing_rows is already sorted by date ASC
                        pending_invoices = []
                        last_pending_date = None
                        for row in billing_rows:
                            inv_ppc = row[5] or 0
                            inv_premium = row[6] or 0
                            inv_opc = row[7] or 0
                            
                            # How much of this invoice is consumed?
                            consume_this_ppc = min(remaining_to_consume_ppc, inv_ppc)
                            consume_this_premium = min(remaining_to_consume_premium, inv_premium)
                            consume_this_opc = min(remaining_to_consume_opc, inv_opc)
                            
                            remaining_to_consume_ppc -= consume_this_ppc
                            remaining_to_consume_premium -= consume_this_premium
                            remaining_to_consume_opc -= consume_this_opc
                            
                            # Remaining from this invoice
                            remaining_ppc = inv_ppc - consume_this_ppc
                            remaining_premium = inv_premium - consume_this_premium
                            remaining_opc = inv_opc - consume_this_opc
                            
                            # If any remaining, this invoice is pending
                            # Store original billed amounts for display, pending amounts for remaining calculation
                            if remaining_ppc > 0.01 or remaining_premium > 0.01 or remaining_opc > 0.01:
                                last_pending_date = row[4]
                                pending_invoices.append({
                                    'invoice_number': row[0],
                                    'dealer_code': row[1],
                                    'dealer_name': row[2],
                                    'ppc_quantity': inv_ppc,  # Original billed amount
                                    'premium_quantity': inv_premium,  # Original billed amount
                                    'opc_quantity': inv_opc,  # Original billed amount
                                    'total_quantity': inv_ppc + inv_premium + inv_opc,  # Original total
                                    'pending_ppc': remaining_ppc,  # Pending amount
                                    'pending_premium': remaining_premium,  # Pending amount
                                    'pending_opc': remaining_opc,  # Pending amount
                                    'total_value': row[12] or 0,
                                    'plant_depot': row[3],
                                    'plant_description': row[13],
                                    'sale_date': row[4]
                                })
                        
                        # Include all invoices from the same date AND plant_depot as the last pending invoice
                        # This shows the complete billing for that date/plant_depot, not just the pending part
                        if last_pending_date and pending_invoices:
                            # Get the plant_depot of the pending invoices
                            pending_plant_depots = set(inv['plant_depot'] for inv in pending_invoices)
                            for row in billing_rows:
                                if row[4] == last_pending_date and row[3] in pending_plant_depots:
                                    # Check if this invoice is already in pending_invoices
                                    invoice_num = row[0]
                                    if not any(inv['invoice_number'] == invoice_num for inv in pending_invoices):
                                        # Add this invoice with 0 pending (fully consumed but same date/plant_depot)
                                        inv_ppc = row[5] or 0
                                        inv_premium = row[6] or 0
                                        inv_opc = row[7] or 0
                                        pending_invoices.append({
                                            'invoice_number': row[0],
                                            'dealer_code': row[1],
                                            'dealer_name': row[2],
                                            'ppc_quantity': inv_ppc,
                                            'premium_quantity': inv_premium,
                                            'opc_quantity': inv_opc,
                                            'total_quantity': inv_ppc + inv_premium + inv_opc,
                                            'pending_ppc': 0,  # Fully consumed
                                            'pending_premium': 0,
                                            'pending_opc': 0,
                                            'total_value': row[12] or 0,
                                            'plant_depot': row[3],
                                            'plant_description': row[13],
                                            'sale_date': row[4]
                                        })
                        
                        # Create card if there are pending invoices OR if there's unloading today
                        # (to show vehicles that become fully unloaded on selected date)
                        if pending_invoices or (has_unloading_today and billing_rows):
                            # Use most recent invoice for card info
                            if pending_invoices:
                                recent = pending_invoices[-1]
                            else:
                                # No pending but has unloading today - use only the most recent billing date's invoices
                                # This shows the billing that was unloaded on the selected date
                                most_recent_date = billing_rows[-1][4]  # sale_date of last invoice
                                pending_invoices = []
                                for row in billing_rows:
                                    if row[4] == most_recent_date:  # Only invoices from most recent billing date
                                        inv_ppc = row[5] or 0
                                        inv_premium = row[6] or 0
                                        inv_opc = row[7] or 0
                                        pending_invoices.append({
                                            'invoice_number': row[0],
                                            'dealer_code': row[1],
                                            'dealer_name': row[2],
                                            'ppc_quantity': inv_ppc,  # Show actual billed amount
                                            'premium_quantity': inv_premium,
                                            'opc_quantity': inv_opc,
                                            'total_quantity': inv_ppc + inv_premium + inv_opc,
                                            'pending_ppc': 0,  # But pending is 0 (fully unloaded)
                                            'pending_premium': 0,
                                            'pending_opc': 0,
                                            'total_value': row[12] or 0,
                                            'plant_depot': row[3],
                                            'plant_description': row[13],
                                            'sale_date': row[4]
                                        })
                                recent = pending_invoices[-1]
                            
                            billing_date = recent['sale_date']
                            dealer_code = recent['dealer_code']
                            # Use PLANT if any invoice is from PLANT, otherwise use the last invoice's plant_depot
                            plant_depot = 'PLANT' if any(inv['plant_depot'] == 'PLANT' for inv in pending_invoices) else (recent['plant_depot'] or 'PLANT')
                            
                            dealer_codes_set = set(inv['dealer_code'] for inv in pending_invoices if inv['dealer_code'])
                            total_ppc = sum(inv['ppc_quantity'] for inv in pending_invoices)
                            total_premium = sum(inv['premium_quantity'] for inv in pending_invoices)
                            total_opc = sum(inv['opc_quantity'] for inv in pending_invoices)
                            total_qty = sum(inv['total_quantity'] for inv in pending_invoices)
                            total_val = sum(inv['total_value'] for inv in pending_invoices)
                            
                            # Calculate the pending amounts for this card (sum of pending from each invoice)
                            card_pending_ppc = sum(inv['pending_ppc'] for inv in pending_invoices)
                            card_pending_premium = sum(inv['pending_premium'] for inv in pending_invoices)
                            card_pending_opc = sum(inv['pending_opc'] for inv in pending_invoices)
                            
                            # If truck has other_dealers_billing today, merge pending to that card
                            # But if truck has sales_data today, keep them as separate cards
                            merged_to_existing = False
                            if is_billed_today:
                                # Find the existing card for this truck - only merge if it's from other_dealers_billing
                                existing_card_key = None
                                for ck, td in trucks_today.items():
                                    if td['truck_number'] == truck_number and td.get('other_billing_added'):
                                        # Only merge with cards created from other_dealers_billing
                                        existing_card_key = ck
                                        break
                                
                                if existing_card_key:
                                    # Add pending invoices to existing card
                                    # Only add the PENDING amounts, not the full billing amounts
                                    trucks_today[existing_card_key]['invoices'].extend(pending_invoices)
                                    trucks_today[existing_card_key]['total_ppc'] += card_pending_ppc
                                    trucks_today[existing_card_key]['total_premium'] += card_pending_premium
                                    trucks_today[existing_card_key]['total_opc'] += card_pending_opc
                                    trucks_today[existing_card_key]['total_quantity'] += card_pending_ppc + card_pending_premium + card_pending_opc
                                    trucks_today[existing_card_key]['total_value'] += total_val
                                    for dc in dealer_codes_set:
                                        if dc not in trucks_today[existing_card_key]['dealer_codes']:
                                            trucks_today[existing_card_key]['dealer_codes'].append(dc)
                                    # Update card pending amounts - include BOTH earlier pending AND today's billing
                                    # Today's billing is also pending since it hasn't been unloaded yet
                                    existing_today_ppc = trucks_today[existing_card_key].get('total_ppc', 0) - card_pending_ppc  # Today's billing before merge
                                    existing_today_premium = trucks_today[existing_card_key].get('total_premium', 0) - card_pending_premium
                                    existing_today_opc = trucks_today[existing_card_key].get('total_opc', 0) - card_pending_opc
                                    # Card pending = earlier pending + today's billing (all is pending)
                                    trucks_today[existing_card_key]['card_pending_ppc'] = card_pending_ppc + existing_today_ppc
                                    trucks_today[existing_card_key]['card_pending_premium'] = card_pending_premium + existing_today_premium
                                    trucks_today[existing_card_key]['card_pending_opc'] = card_pending_opc + existing_today_opc
                                    trucks_today[existing_card_key]['from_earlier_date'] = True
                                    merged_to_existing = True
                            
                            # Skip creating new card if merged to existing
                            if not merged_to_existing:
                                # Don't create Prev Day card if no actual pending
                                # This prevents showing fully unloaded cards from previous days
                                has_any_pending = card_pending_ppc > 0.01 or card_pending_premium > 0.01 or card_pending_opc > 0.01
                                if not has_any_pending:
                                    continue  # Skip this card - no pending material
                                
                                card_key = f"{truck_number}_{plant_depot}_pending"
                                
                                # For Prev Day cards, only show unloading from the selected date (current day)
                                # But calculate cumulative unloading for remaining calculation
                                prev_day_unloading = []
                                
                                # Calculate cumulative unloading for this card:
                                # This is the total unloading from billing_date to selected_date that applies to this billing
                                # For display purposes only - the remaining calculation uses card_pending directly
                                cursor.execute('''
                                    SELECT SUM(ppc_unloaded), SUM(premium_unloaded), SUM(opc_unloaded)
                                    FROM vehicle_unloading
                                    WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
                                ''', (truck_number, billing_date, selected_date))
                                period_unloading = cursor.fetchone()
                                cumulative_ppc = period_unloading[0] or 0
                                cumulative_premium = period_unloading[1] or 0
                                cumulative_opc = period_unloading[2] or 0
                                
                                # Query 2: Get unloading ONLY on the selected_date for display
                                cursor.execute('''
                                    SELECT id, dealer_code, unloading_dealer, unloading_point, ppc_unloaded, premium_unloaded, opc_unloaded, unloading_date
                                    FROM vehicle_unloading
                                    WHERE truck_number = ? AND unloading_date = ?
                                    ORDER BY unloading_date ASC
                                ''', (truck_number, selected_date))
                                
                                historical_unloading = cursor.fetchall()
                                
                                if historical_unloading and (has_any_pending or not is_billed_today):
                                    # Process historical unloading records
                                    # Only include unloading that matches the pending product types
                                    for unload_row in historical_unloading:
                                        unload_id = unload_row[0]
                                        dealer_code = unload_row[1]
                                        unloading_dealer = unload_row[2]
                                        unloading_point = unload_row[3]
                                        ppc_unloaded = unload_row[4] or 0
                                        premium_unloaded = unload_row[5] or 0
                                        opc_unloaded = unload_row[6] or 0
                                        unload_date = unload_row[7]
                                        
                                        # Only include unloading if it matches a product type that has pending
                                        has_matching_product = False
                                        if card_pending_ppc > 0.01 and ppc_unloaded > 0:
                                            has_matching_product = True
                                        if card_pending_premium > 0.01 and premium_unloaded > 0:
                                            has_matching_product = True
                                        if card_pending_opc > 0.01 and opc_unloaded > 0:
                                            has_matching_product = True
                                        
                                        if has_matching_product:
                                            unload = {
                                                'id': unload_id,
                                                'dealer_code': dealer_code,
                                                'dealer_name': unloading_dealer,
                                                'point': unloading_point,
                                                'ppc_unloaded': ppc_unloaded,
                                                'premium_unloaded': premium_unloaded,
                                                'opc_unloaded': opc_unloaded,
                                                'unloading_date': unload_date
                                            }
                                            prev_day_unloading.append(unload)
                                
                                # Create card if there's pending OR if there's unloading assigned
                                # This shows vehicles that have pending material (even if fully unloaded today)
                                has_pending = card_pending_ppc > 0.01 or card_pending_premium > 0.01 or card_pending_opc > 0.01
                                has_unloading = len(prev_day_unloading) > 0
                                
                                if has_pending or has_unloading:
                                    trucks_today[card_key] = {
                                        'truck_number': truck_number,
                                        'card_key': card_key,
                                        'plant_depot': plant_depot,
                                        'invoices': pending_invoices,
                                        'dealer_codes': list(dealer_codes_set),
                                        'total_ppc': total_ppc,
                                        'total_premium': total_premium,
                                        'total_opc': total_opc,
                                        'total_quantity': total_qty,
                                        'total_value': total_val,
                                        'billing_date': billing_date,
                                        'unloading_details': prev_day_unloading,  # Show today's unloading on prev day card (FIFO)
                                        'other_billing': [],
                                        'from_earlier_date': True,
                                        'is_previous_day_pending': True,  # Show "Prev Day" tag
                                        # Store card-specific pending for remaining calculation
                                        'card_pending_ppc': card_pending_ppc,
                                        # Store cumulative unloading for remaining calculation
                                        'cumulative_unloaded_ppc': cumulative_ppc,
                                        'cumulative_unloaded_premium': cumulative_premium,
                                        'cumulative_unloaded_opc': cumulative_opc,
                                        'card_pending_premium': card_pending_premium,
                                        'card_pending_opc': card_pending_opc
                                    }
        
        # Now that all cards (including _pending cards) are created, assign unloading
        assign_unloading_to_cards()
        
        # Add other_billing quantities to truck totals
        # ONLY for cards that were created from sales_data (not from other_billing_map)
        # Cards from other_billing_map already have these quantities included (flagged with other_billing_added)
        for card_key, truck_data in trucks_today.items():
            # Skip if other_billing quantities were already added to this card
            if truck_data.get('other_billing_added'):
                continue
            
            truck_number = truck_data['truck_number']
            other_billings = truck_data.get('other_billing', [])
            for ob in other_billings:
                truck_data['total_ppc'] += ob.get('ppc_quantity', 0) or 0
                truck_data['total_premium'] += ob.get('premium_quantity', 0) or 0
                truck_data['total_opc'] += ob.get('opc_quantity', 0) or 0
                truck_data['total_quantity'] += ob.get('total_quantity', 0) or 0
                truck_data['total_value'] += ob.get('total_value', 0) or 0
        
        # Now check for previous day billings that weren't fully unloaded
        for card_key, truck_data in trucks_today.items():
            truck_number = truck_data['truck_number']  # Get actual truck number from data
            card_dealer_codes = set(str(dc) for dc in truck_data.get('dealer_codes', []))
            has_pending_previous = False
            previous_pending_qty = 0
            previous_pending_ppc = 0
            previous_pending_premium = 0
            previous_pending_opc = 0
            
            # FIFO: Calculate remaining for today's billing
            # Get total billing for this truck up to and including selected_date
            # Include opening balance from previous month
            plant_depot = truck_data.get('plant_depot', 'PLANT')
            opening_ppc = opening_balance_map.get(truck_number, {}).get('ppc', 0)
            opening_premium = opening_balance_map.get(truck_number, {}).get('premium', 0)
            opening_opc = opening_balance_map.get(truck_number, {}).get('opc', 0)
            
            # Check if this truck has multiple plant_depot types
            cursor.execute('''
                SELECT COUNT(DISTINCT plant_depot) FROM sales_data 
                WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ?
            ''', (truck_number, month_start, selected_date))
            plant_depot_count = cursor.fetchone()[0] or 1
            
            # Get dealer_codes for this card
            card_dealer_codes = set(truck_data.get('dealer_codes', []))
            
            # First calculate global totals (including other_dealers_billing)
            cursor.execute('''
                SELECT SUM(ppc_quantity), SUM(premium_quantity), SUM(opc_quantity)
                FROM sales_data
                WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ?
            ''', (truck_number, month_start, selected_date))
            total_billing = cursor.fetchone()
            
            # Also get other_dealers_billing
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                FROM other_dealers_billing
                WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ?
            ''', (truck_number, month_start, selected_date))
            other_billing = cursor.fetchone()
            
            global_billed_ppc = (total_billing[0] or 0) + (other_billing[0] or 0) + opening_ppc
            global_billed_premium = (total_billing[1] or 0) + (other_billing[1] or 0) + opening_premium
            global_billed_opc = (total_billing[2] or 0) + (other_billing[2] or 0) + opening_opc
            
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                FROM vehicle_unloading
                WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
            ''', (truck_number, month_start, selected_date))
            total_unloading = cursor.fetchone()
            global_unloaded_ppc = total_unloading[0] or 0
            global_unloaded_premium = total_unloading[1] or 0
            global_unloaded_opc = total_unloading[2] or 0
            
            # Global pending for this truck
            global_pending_ppc = max(0, global_billed_ppc - global_unloaded_ppc)
            global_pending_premium = max(0, global_billed_premium - global_unloaded_premium)
            global_pending_opc = max(0, global_billed_opc - global_unloaded_opc)
            
            # For trucks with multiple plant_depot types, calculate card pending based on plant_depot
            if plant_depot_count > 1 and card_dealer_codes:
                # Get unloading for this card by matching plant_depot
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                    FROM vehicle_unloading
                    WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
                      AND plant_depot = ?
                ''', (truck_number, month_start, selected_date, plant_depot))
                
                card_unloading = cursor.fetchone()
                card_unloaded_ppc = card_unloading[0] or 0
                card_unloaded_premium = card_unloading[1] or 0
                card_unloaded_opc = card_unloading[2] or 0
                
                # Get billing for this card by matching plant_depot
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                    FROM sales_data
                    WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ? AND plant_depot = ?
                ''', (truck_number, month_start, selected_date, plant_depot))
                card_billing = cursor.fetchone()
                card_billed_ppc = card_billing[0] or 0
                card_billed_premium = card_billing[1] or 0
                card_billed_opc = card_billing[2] or 0
                
                # Card's pending = card's billing - card's unloading
                total_pending_ppc = max(0, card_billed_ppc - card_unloaded_ppc)
                total_pending_premium = max(0, card_billed_premium - card_unloaded_premium)
                total_pending_opc = max(0, card_billed_opc - card_unloaded_opc)
            else:
                # Single plant_depot OR no global pending - use global values
                total_pending_ppc = global_pending_ppc
                total_pending_premium = global_pending_premium
                total_pending_opc = global_pending_opc
            
            # FIFO: Pending is attributed to the LAST billing (today's billing)
            # So today's remaining = min(today's billed, total pending)
            truck_data['total_pending_for_fifo'] = {
                'ppc': total_pending_ppc,
                'premium': total_pending_premium,
                'opc': total_pending_opc
            }
            
            if truck_number in ['HR55AZ1569', 'HR58D1569']:
                print(f"DEBUG FIFO card={card_key}, total_pending_ppc={total_pending_ppc}, card_billed_ppc={card_billed_ppc if 'card_billed_ppc' in locals() else 'N/A'}, card_unloaded_ppc={card_unloaded_ppc if 'card_unloaded_ppc' in locals() else 'N/A'}")
            
            truck_data['has_pending_previous'] = has_pending_previous
            truck_data['previous_pending_qty'] = round(previous_pending_qty, 2)
            truck_data['previous_pending_ppc'] = round(previous_pending_ppc, 2)
            truck_data['previous_pending_premium'] = round(previous_pending_premium, 2)
            truck_data['previous_pending_opc'] = round(previous_pending_opc, 2)
            truck_data['previous_billings'] = []  # Previous billings shown as separate cards
            truck_data['is_rebilled'] = has_pending_previous
            truck_data['is_opening_vehicle'] = False
            
            # Calculate if today's billing is fully unloaded
            # Total billed = previous pending + today's billing
            total_billed_ppc = previous_pending_ppc + truck_data['total_ppc']
            total_billed_premium = previous_pending_premium + truck_data['total_premium']
            total_billed_opc = previous_pending_opc + truck_data['total_opc']
            
            # Check if this truck has an opening balance (previous month carryover)
            # Opening balance unloading should NOT count against today's billing
            # Use opening_balance_map which includes both pending_vehicle_unloading entries
            # AND calculated opening from previous month's closing
            opening_balance_ppc = 0
            opening_balance_premium = 0
            opening_balance_opc = 0
            if truck_number in opening_balance_map:
                opening_balance_ppc = opening_balance_map[truck_number].get('ppc', 0)
                opening_balance_premium = opening_balance_map[truck_number].get('premium', 0)
                opening_balance_opc = opening_balance_map[truck_number].get('opc', 0)
            
            # Get total unloaded for this card - ONLY count unloading on selected_date
            # (unloading on previous days applies to previous billings)
            total_unloaded_ppc = 0
            total_unloaded_premium = 0
            total_unloaded_opc = 0
            dealer_codes = set(truck_data.get('dealer_codes', []))
            dealer_codes_str = set(str(dc) for dc in dealer_codes)
            
            # Use unloading_map which only contains unloading on selected_date
            # Get PLANT dealer codes for this truck (for DEPOT card filtering)
            plant_codes_for_truck = truck_plant_dealer_codes.get(truck_number, set())
            card_plant_depot = truck_data.get('plant_depot', 'PLANT')
            
            if truck_number in unloading_map:
                for u in unloading_map[truck_number]:
                    # If single card, count all unloading
                    # If multiple cards, filter by plant_depot first, then dealer_code
                    if truck_card_count.get(truck_number, 1) == 1:
                        total_unloaded_ppc += u['ppc_unloaded']
                        total_unloaded_premium += u['premium_unloaded']
                        total_unloaded_opc += u['opc_unloaded']
                    else:
                        u_dealer_code = str(u.get('dealer_code', '')) if u.get('dealer_code') else ''
                        u_plant_depot = u.get('plant_depot', '').upper() if u.get('plant_depot') else ''
                        
                        if truck_number == 'HR55AZ1569':
                            print(f"DEBUG REMAINING CALC card={card_key}, card_plant_depot={card_plant_depot}, u_plant_depot={u_plant_depot}, u_ppc={u['ppc_unloaded']}, u_premium={u['premium_unloaded']}")
                        
                        # First priority: Match by plant_depot if unloading has it specified
                        if u_plant_depot:
                            if u_plant_depot == card_plant_depot:
                                total_unloaded_ppc += u['ppc_unloaded']
                                total_unloaded_premium += u['premium_unloaded']
                                total_unloaded_opc += u['opc_unloaded']
                                if truck_number == 'HR55AZ1569':
                                    print(f"DEBUG REMAINING CALC MATCHED by plant_depot, added ppc={u['ppc_unloaded']}, premium={u['premium_unloaded']}")
                        else:
                            # Legacy unloading without plant_depot - use dealer_code matching
                            if card_plant_depot == 'DEPOT':
                                # DEPOT card gets unloading that:
                                # 1. Matches its own dealer_codes, OR
                                # 2. Doesn't match any PLANT card's dealer_codes
                                # 3. Has no dealer_code (legacy data)
                                if u_dealer_code in dealer_codes_str or u_dealer_code not in plant_codes_for_truck or not u_dealer_code:
                                    total_unloaded_ppc += u['ppc_unloaded']
                                    total_unloaded_premium += u['premium_unloaded']
                                    total_unloaded_opc += u['opc_unloaded']
                            else:
                                # PLANT card - only gets unloading matching its dealer_codes
                                if u_dealer_code in dealer_codes_str or not u_dealer_code:
                                    total_unloaded_ppc += u['ppc_unloaded']
                                    total_unloaded_premium += u['premium_unloaded']
                                    total_unloaded_opc += u['opc_unloaded']
            
            if truck_number == 'HR55AZ1569':
                print(f"DEBUG REMAINING CALC FINAL card={card_key}, total_unloaded_ppc={total_unloaded_ppc}, total_unloaded_premium={total_unloaded_premium}")
            
            # FIFO: Total pending is attributed to today's billing (the last billing)
            # Get total pending from FIFO calculation
            total_pending_fifo = truck_data.get('total_pending_for_fifo', {'ppc': 0, 'premium': 0, 'opc': 0})
            total_pending_ppc = total_pending_fifo['ppc']
            total_pending_premium = total_pending_fifo['premium']
            total_pending_opc = total_pending_fifo['opc']
            
            # Today's remaining = min(today's billed, total pending)
            # This ensures that if total pending > today's billed, the excess shows on previous billings
            # For vehicles from earlier dates (not billed today), use card-specific pending as remaining
            if truck_data.get('from_earlier_date'):
                # Use cumulative unloading (from billing_date to selected_date) for remaining calculation
                # The unloading_details only shows today's unloading for display
                card_unloaded_ppc = truck_data.get('cumulative_unloaded_ppc', 0)
                card_unloaded_premium = truck_data.get('cumulative_unloaded_premium', 0)
                card_unloaded_opc = truck_data.get('cumulative_unloaded_opc', 0)
                
                # For Prev Day cards with multiple invoices from same date:
                # Use total_ppc if we added invoices with pending=0 (to show complete billing for that date)
                # Otherwise use card_pending_ppc (FIFO-calculated pending amount)
                # BUT: If card_pending_ppc = 0, always use 0 for remaining (FIFO determined no pending)
                card_pending_ppc_val = truck_data.get('card_pending_ppc', 0)
                card_pending_premium_val = truck_data.get('card_pending_premium', 0)
                card_pending_opc_val = truck_data.get('card_pending_opc', 0)
                
                # Calculate remaining for each product type separately based on its pending status
                # For vehicles billed today, card_pending excludes today's unloading (to prevent FIFO from consuming it)
                # So we need to subtract today's unloading from card_pending to get the actual remaining
                today_unloaded_ppc = sum(u.get('ppc_unloaded', 0) for u in truck_data.get('unloading_details', []))
                today_unloaded_premium = sum(u.get('premium_unloaded', 0) for u in truck_data.get('unloading_details', []))
                today_unloaded_opc = sum(u.get('opc_unloaded', 0) for u in truck_data.get('unloading_details', []))
                
                # PPC
                if card_pending_ppc_val < 0.01:
                    # FIFO determined 0 pending for PPC
                    remaining_ppc = 0
                elif truck_data.get('total_ppc', 0) > card_pending_ppc_val + 0.01:
                    # We added extra invoices - use total_ppc for remaining
                    remaining_ppc = max(0, truck_data.get('total_ppc', 0) - card_unloaded_ppc)
                else:
                    # Normal FIFO pending - subtract today's unloading from card_pending
                    remaining_ppc = max(0, card_pending_ppc_val - today_unloaded_ppc)
                
                # Premium
                if card_pending_premium_val < 0.01:
                    # FIFO determined 0 pending for Premium
                    remaining_premium = 0
                elif truck_data.get('total_premium', 0) > card_pending_premium_val + 0.01:
                    # We added extra invoices - use total_premium for remaining
                    remaining_premium = max(0, truck_data.get('total_premium', 0) - card_unloaded_premium)
                else:
                    # Normal FIFO pending - subtract today's unloading from card_pending
                    remaining_premium = max(0, card_pending_premium_val - today_unloaded_premium)
                
                # OPC
                if card_pending_opc_val < 0.01:
                    # FIFO determined 0 pending for OPC
                    remaining_opc = 0
                elif truck_data.get('total_opc', 0) > card_pending_opc_val + 0.01:
                    # We added extra invoices - use total_opc for remaining
                    remaining_opc = max(0, truck_data.get('total_opc', 0) - card_unloaded_opc)
                else:
                    # Normal FIFO pending - subtract today's unloading from card_pending
                    remaining_opc = max(0, card_pending_opc_val - today_unloaded_opc)
            else:
                # For today's cards, use simple calculation like dealer balance page:
                # Remaining = Today's Billed - Today's Unloaded (no complex FIFO)
                # Get today's unloaded from unloading_details
                today_unloaded_ppc = sum(u.get('ppc_unloaded', 0) for u in truck_data.get('unloading_details', []))
                today_unloaded_premium = sum(u.get('premium_unloaded', 0) for u in truck_data.get('unloading_details', []))
                today_unloaded_opc = sum(u.get('opc_unloaded', 0) for u in truck_data.get('unloading_details', []))
                
                remaining_ppc = max(0, truck_data['total_ppc'] - today_unloaded_ppc)
                remaining_premium = max(0, truck_data['total_premium'] - today_unloaded_premium)
                remaining_opc = max(0, truck_data['total_opc'] - today_unloaded_opc)
            
            # Calculate how much was unloaded for today's billing
            unloaded_for_today_ppc = truck_data['total_ppc'] - remaining_ppc
            unloaded_for_today_premium = truck_data['total_premium'] - remaining_premium
            unloaded_for_today_opc = truck_data['total_opc'] - remaining_opc
            remaining_total = remaining_ppc + remaining_premium + remaining_opc
            
            # Add calculated remaining to truck_data for frontend display
            truck_data['remaining_ppc'] = round(remaining_ppc, 2)
            truck_data['remaining_premium'] = round(remaining_premium, 2)
            truck_data['remaining_opc'] = round(remaining_opc, 2)
            truck_data['remaining_total'] = round(remaining_total, 2)
            
            # Calculate how much unloading applies to TODAY's billing (after previous consumed)
            # If previous pending consumed all unloading, today's billing has no unloading
            unloading_for_today_ppc = max(0, total_unloaded_ppc - previous_pending_ppc - (truck_data['total_ppc'] - remaining_ppc) if previous_pending_ppc > 0 else total_unloaded_ppc)
            unloading_for_today_premium = max(0, total_unloaded_premium - previous_pending_premium - (truck_data['total_premium'] - remaining_premium) if previous_pending_premium > 0 else total_unloaded_premium)
            unloading_for_today_opc = max(0, total_unloaded_opc - previous_pending_opc - (truck_data['total_opc'] - remaining_opc) if previous_pending_opc > 0 else total_unloaded_opc)
            
            # Filter unloading_details to only show unloading that applies to today's billing
            # by matching product types and excluding unloading consumed by previous billings
            # For vehicles from earlier dates, show today's unloading directly
            if truck_data.get('from_earlier_date'):
                # For pending vehicles from earlier dates, show all today's unloading
                today_has_unloading = len(truck_data.get('unloading_details', [])) > 0
            else:
                today_has_unloading = (unloaded_for_today_ppc > 0.01 or 
                                       unloaded_for_today_premium > 0.01 or 
                                       unloaded_for_today_opc > 0.01)
            
            if not today_has_unloading:
                # All unloading went to previous billings, not today's
                # BUT: Keep unloading_details if they exist (unloading was recorded on today's date)
                # Only clear if there are truly no unloading_details
                if truck_card_count.get(truck_number, 1) == 1:
                    # Single card - only clear if there are no unloading_details at all
                    if len(truck_data.get('unloading_details', [])) == 0:
                        truck_data['unloading_details'] = []
                # For multiple cards, keep the filtered unloading_details as they were matched by plant_depot
            elif truck_data.get('from_earlier_date'):
                # For pending vehicles from earlier dates, show all today's unloading without filtering
                # unloading_details is already set from unloading_map
                pass
            else:
                # Filter unloading details to only show the portion that applies to today's billing
                # Use FIFO: only show up to unloaded_for_today_* amounts
                filtered_unloading = []
                remaining_to_show_ppc = unloaded_for_today_ppc
                remaining_to_show_premium = unloaded_for_today_premium
                remaining_to_show_opc = unloaded_for_today_opc
                
                for u in truck_data.get('unloading_details', []):
                    record_ppc = u.get('ppc_unloaded', 0)
                    record_premium = u.get('premium_unloaded', 0)
                    record_opc = u.get('opc_unloaded', 0)
                    
                    # Only take up to what's remaining for today's billing
                    show_ppc = min(record_ppc, remaining_to_show_ppc) if truck_data['total_ppc'] > 0 else 0
                    show_premium = min(record_premium, remaining_to_show_premium) if truck_data['total_premium'] > 0 else 0
                    show_opc = min(record_opc, remaining_to_show_opc) if truck_data['total_opc'] > 0 else 0
                    
                    # Update remaining
                    remaining_to_show_ppc -= show_ppc
                    remaining_to_show_premium -= show_premium
                    remaining_to_show_opc -= show_opc
                    
                    if show_ppc > 0.01 or show_premium > 0.01 or show_opc > 0.01:
                        filtered_unloading.append({
                            'id': u['id'],
                            'truck_number': u['truck_number'],
                            'unloading_dealer': u.get('unloading_dealer'),
                            'unloading_point': u.get('unloading_point'),
                            'dealer_name': u.get('dealer_name') or u.get('unloading_dealer'),  # Frontend field
                            'point': u.get('point') or u.get('unloading_point'),              # Frontend field
                            'ppc_unloaded': round(show_ppc, 2),
                            'premium_unloaded': round(show_premium, 2),
                            'opc_unloaded': round(show_opc, 2),
                            'unloaded_quantity': round(show_ppc + show_premium + show_opc, 2),
                            'notes': u.get('notes', ''),
                            'dealer_code': u.get('dealer_code', ''),
                            'is_other_dealer': u.get('is_other_dealer', False),
                            'unloading_date': u.get('unloading_date', '')
                        })
                truck_data['unloading_details'] = filtered_unloading
            
            # Add all vehicles billed on this date (including fully unloaded)
            vehicles_list.append(truck_data)
        
        # Also get pending vehicles from opening material balance (previous month carryover)
        # These are vehicles that were billed in previous months but not fully unloaded
        from datetime import datetime
        selected_dt = datetime.strptime(selected_date, '%Y-%m-%d')
        month_year = selected_dt.strftime('%Y-%m')
        
        try:
            # Use the opening_balance_map which was already calculated earlier
            # (includes previous month's closing when no current month data exists)
            pending_data = []
            for truck_number, ob_data in opening_balance_map.items():
                pending_data.append((
                    truck_number,
                    ob_data.get('billing_date', 'Previous Month'),
                    ob_data.get('dealer_code'),
                    ob_data.get('ppc', 0),
                    ob_data.get('premium', 0),
                    ob_data.get('opc', 0)
                ))
            
            for row in pending_data:
                truck_number = row[0]
                billing_date = row[1] or 'Previous Month'
                dealer_code = row[2]
                
                # Look up dealer name
                cursor.execute('SELECT dealer_name FROM sales_data WHERE dealer_code = ? LIMIT 1', (dealer_code,))
                dealer_row = cursor.fetchone()
                dealer_name = dealer_row[0] if dealer_row else f'Dealer {dealer_code}'
                
                ppc_qty = row[3] or 0
                premium_qty = row[4] or 0
                opc_qty = row[5] or 0
                total_qty = ppc_qty + premium_qty + opc_qty
                
                # Get unloading details for this truck (current month only)
                cursor.execute('''
                    SELECT id, truck_number, unloading_dealer, unloading_point, 
                           ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                           notes, dealer_code, is_other_dealer, unloading_date
                    FROM vehicle_unloading 
                    WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
                ''', (truck_number, month_start, selected_date))
                
                pending_unloading = []
                for urow in cursor.fetchall():
                    pending_unloading.append({
                        'id': urow[0],
                        'truck_number': urow[1],
                        'unloading_dealer': urow[2],
                        'unloading_point': urow[3],
                        'ppc_unloaded': urow[4] or 0,
                        'premium_unloaded': urow[5] or 0,
                        'opc_unloaded': urow[6] or 0,
                        'unloaded_quantity': urow[7] or 0,
                        'notes': urow[8],
                        'dealer_code': urow[9],
                        'is_other_dealer': bool(urow[10]) if urow[10] is not None else False,
                        'unloading_date': urow[11]
                    })
                
                # Apply FIFO: opening balance gets unloaded first
                # Sort unloading by date to apply FIFO
                pending_unloading_sorted = sorted(pending_unloading, key=lambda x: x['unloading_date'] or '')
                
                # Calculate how much unloading applies to opening balance (FIFO)
                remaining_ppc = ppc_qty
                remaining_premium = premium_qty
                remaining_opc = opc_qty
                opening_unloading = []  # Unloading that applies to opening balance
                
                for u in pending_unloading_sorted:
                    if remaining_ppc <= 0.01 and remaining_premium <= 0.01 and remaining_opc <= 0.01:
                        break  # Opening balance fully consumed
                    
                    # Calculate how much of this unloading applies to opening
                    applied_ppc = min(remaining_ppc, u['ppc_unloaded'])
                    applied_premium = min(remaining_premium, u['premium_unloaded'])
                    applied_opc = min(remaining_opc, u['opc_unloaded'])
                    
                    if applied_ppc > 0 or applied_premium > 0 or applied_opc > 0:
                        opening_unloading.append(u)
                        remaining_ppc -= applied_ppc
                        remaining_premium -= applied_premium
                        remaining_opc -= applied_opc
                
                remaining = remaining_ppc + remaining_premium + remaining_opc
                
                # Check if this truck is already in today's billing
                truck_already_billed_today = truck_number in actual_trucks_billed_today
                
                # Check if any unloading that APPLIES TO OPENING happened on the selected date
                unloaded_today = any(u['unloading_date'] == selected_date for u in opening_unloading)
                
                # Show opening balance vehicles if:
                # 1. They have pending material, OR
                # 2. They were unloaded TODAY (unloading that applies to opening balance)
                if remaining <= 0.01:
                    # Fully unloaded - only show if unloaded today
                    if not unloaded_today:
                        continue
                
                # Add opening balance vehicles as separate entries
                # They represent previous month's pending material
                vehicles_list.append({
                    'truck_number': truck_number + '_OPENING',  # Add suffix to make unique key
                    'display_truck_number': truck_number,  # Original truck number for display
                    'billing_date': billing_date,
                    'invoices': [{
                        'invoice_number': 'OPENING',
                        'dealer_code': dealer_code,
                        'dealer_name': dealer_name,
                        'ppc_quantity': ppc_qty,
                        'premium_quantity': premium_qty,
                        'opc_quantity': opc_qty,
                        'total_quantity': total_qty,
                        'total_value': 0,
                        'plant_depot': 'Previous Month'
                    }],
                    'total_ppc': ppc_qty,
                    'total_premium': premium_qty,
                    'total_opc': opc_qty,
                    'total_quantity': total_qty,
                    'total_value': 0,
                    'unloading_details': opening_unloading,  # Only unloading that applies to opening
                    'other_billing': [],
                    'is_opening_vehicle': True,
                    'original_billing_date': billing_date,
                    'has_pending_previous': False,
                    'previous_pending_qty': 0,
                    'previous_pending_ppc': 0,
                    'previous_pending_premium': 0,
                    'previous_pending_opc': 0,
                    'previous_billings': [],
                    'is_rebilled': False,
                    'truck_also_billed_today': truck_already_billed_today,
                    'remaining_qty': remaining,
                    'is_fully_unloaded': remaining <= 0.01,
                    'remaining_ppc': round(remaining_ppc, 2),
                    'remaining_premium': round(remaining_premium, 2),
                    'remaining_opc': round(remaining_opc, 2),
                    'remaining_total': round(remaining, 2)
                })
        except Exception as e:
            # Table might not exist
            pass
        
        # Also get vehicles billed on previous days within the month that are still pending
        # (not billed today, not opening balance, but have pending material)
        try:
            # Get all trucks already in the list
            trucks_in_list = set()
            for v in vehicles_list:
                display_truck = v.get('display_truck_number', v.get('truck_number', ''))
                # Remove _OPENING suffix if present
                if display_truck.endswith('_OPENING'):
                    display_truck = display_truck[:-8]
                trucks_in_list.add(display_truck)
            
            # Get vehicles billed on previous days (not today) that might be pending
            # Include both sales_data and other_dealers_billing
            # SEPARATE by plant_depot to show PLANT and DEPOT as different cards
            cursor.execute('''
                SELECT truck_number, billing_date, plant_depot,
                       GROUP_CONCAT(DISTINCT dealers) as dealers,
                       GROUP_CONCAT(DISTINCT dealer_code) as dealer_codes,
                       SUM(ppc) as ppc,
                       SUM(premium) as premium,
                       SUM(opc) as opc
                FROM (
                    SELECT truck_number, sale_date as billing_date, plant_depot,
                           dealer_name as dealers, dealer_code,
                           ppc_quantity as ppc, premium_quantity as premium, opc_quantity as opc
                    FROM sales_data
                    WHERE sale_date >= ? AND sale_date < ?
                      AND truck_number IS NOT NULL AND truck_number != ''
                    UNION ALL
                    SELECT truck_number, sale_date as billing_date, 'DEPOT' as plant_depot,
                           dealer_name as dealers, '' as dealer_code,
                           ppc_quantity as ppc, premium_quantity as premium, opc_quantity as opc
                    FROM other_dealers_billing
                    WHERE sale_date >= ? AND sale_date < ?
                      AND truck_number IS NOT NULL AND truck_number != ''
                )
                GROUP BY truck_number, billing_date, plant_depot
                ORDER BY billing_date, truck_number, plant_depot
            ''', (month_start, selected_date, month_start, selected_date))
            
            prev_day_billings = cursor.fetchall()
            
            # Track cumulative billing per truck (including opening balance)
            truck_total_billed = {}
            for truck, ob in opening_balance_map.items():
                truck_total_billed[truck] = {
                    'ppc': ob['ppc'],
                    'premium': ob['premium'],
                    'opc': ob['opc']
                }
            
            for row in prev_day_billings:
                truck = row[0]
                if truck not in truck_total_billed:
                    truck_total_billed[truck] = {'ppc': 0, 'premium': 0, 'opc': 0}
                truck_total_billed[truck]['ppc'] += row[5] or 0
                truck_total_billed[truck]['premium'] += row[6] or 0
                truck_total_billed[truck]['opc'] += row[7] or 0
            
            # Get unloading for all trucks (current month only)
            cursor.execute('''
                SELECT truck_number,
                       SUM(ppc_unloaded) as ppc,
                       SUM(premium_unloaded) as premium,
                       SUM(opc_unloaded) as opc
                FROM vehicle_unloading
                WHERE unloading_date >= ? AND unloading_date <= ?
                GROUP BY truck_number
            ''', (month_start, selected_date))
            
            truck_unloaded = {}
            for row in cursor.fetchall():
                truck_unloaded[row[0]] = {
                    'ppc': row[1] or 0,
                    'premium': row[2] or 0,
                    'opc': row[3] or 0
                }
            
            # Find trucks with pending material - show each billing date + plant_depot separately
            # Track which truck+date+plant_depot combinations we've added
            added_truck_date_sources = set()
            for v in vehicles_list:
                display_truck = v.get('display_truck_number', v.get('truck_number', ''))
                if display_truck.endswith('_OPENING'):
                    display_truck = display_truck[:-8]
                if display_truck.endswith('_PREV'):
                    display_truck = display_truck[:-5]
                plant_depot = v.get('plant_depot', 'PLANT')
                added_truck_date_sources.add(f"{display_truck}_{v.get('billing_date', '')}_{plant_depot}")
            
            # Track which trucks we've already added for previous day unloading
            # Only show ONE card per truck (the most recent billing)
            added_prev_trucks = set()
            
            # Sort prev_day_billings by billing_date DESC to get most recent first
            prev_day_billings_sorted = sorted(prev_day_billings, key=lambda x: x[1], reverse=True)
            
            for row in prev_day_billings_sorted:
                truck_number = row[0]
                billing_date = row[1]
                plant_depot = row[2] or 'PLANT'
                dealers = row[3]
                dealer_codes_str = row[4] or ''
                dealer_codes = set(dealer_codes_str.split(',')) if dealer_codes_str else set()
                billed_ppc = row[5] or 0
                billed_premium = row[6] or 0
                billed_opc = row[7] or 0
                
                # Skip if this truck already has a pending card (from earlier_billed_trucks logic)
                if f"{truck_number}_PLANT_pending" in trucks_today or f"{truck_number}_DEPOT_pending" in trucks_today:
                    continue
                
                # Skip if this truck already has a card from today's billing (sales_data or other_dealers_billing)
                # This prevents duplicate cards when truck has other_dealers_billing today + sales_data from earlier
                truck_has_today_card = any(td['truck_number'] == truck_number for td in trucks_today.values())
                if truck_has_today_card:
                    continue
                
                # Skip if this specific truck+date+plant_depot is already in list (from today's billing)
                truck_date_source_key = f"{truck_number}_{billing_date}_{plant_depot}"
                if truck_date_source_key in added_truck_date_sources:
                    continue
                
                # Check if there's a billing on the selected date for this truck+plant_depot
                today_billing_key = f"{truck_number}_{selected_date}_{plant_depot}"
                has_today_billing = today_billing_key in added_truck_date_sources
                
                # Check if this truck has multiple plant_depot types in the month
                cursor.execute('''
                    SELECT COUNT(DISTINCT plant_depot) FROM sales_data 
                    WHERE truck_number = ? AND sale_date >= ? AND sale_date <= ?
                ''', (truck_number, month_start, selected_date))
                plant_depot_count = cursor.fetchone()[0] or 1
                
                # FIFO: Get total billing BEFORE this billing date (these consume unloading first)
                # Include opening balance from previous month
                # Use ALL billing (not filtered by dealer_code) for correct FIFO calculation
                opening_ppc = opening_balance_map.get(truck_number, {}).get('ppc', 0)
                opening_premium = opening_balance_map.get(truck_number, {}).get('premium', 0)
                opening_opc = opening_balance_map.get(truck_number, {}).get('opc', 0)
                
                # Get billing from sales_data BEFORE this billing date
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                    FROM sales_data
                    WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                ''', (truck_number, month_start, billing_date))
                earlier_billing = cursor.fetchone()
                
                # Also get other_dealers_billing BEFORE this date
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                    FROM other_dealers_billing
                    WHERE truck_number = ? AND sale_date >= ? AND sale_date < ?
                ''', (truck_number, month_start, billing_date))
                earlier_other = cursor.fetchone()
                
                earlier_billed_ppc = (earlier_billing[0] or 0) + (earlier_other[0] or 0) + opening_ppc
                earlier_billed_premium = (earlier_billing[1] or 0) + (earlier_other[1] or 0) + opening_premium
                earlier_billed_opc = (earlier_billing[2] or 0) + (earlier_other[2] or 0) + opening_opc
                
                # Get total unloading from month_start to selected_date
                # Use ALL unloading (not filtered by dealer_code) for correct FIFO calculation
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                    FROM vehicle_unloading
                    WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
                ''', (truck_number, month_start, selected_date))
                total_unloading = cursor.fetchone()
                total_unloaded_ppc = total_unloading[0] or 0
                total_unloaded_premium = total_unloading[1] or 0
                total_unloaded_opc = total_unloading[2] or 0
                
                # Get billing ON this billing date (ALL for the truck, not just this card)
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                    FROM sales_data
                    WHERE truck_number = ? AND sale_date = ?
                ''', (truck_number, billing_date))
                this_date_billing = cursor.fetchone()
                
                # Also get other_dealers_billing ON this date
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                    FROM other_dealers_billing
                    WHERE truck_number = ? AND sale_date = ?
                ''', (truck_number, billing_date))
                this_date_other = cursor.fetchone()
                
                this_date_billed_ppc = (this_date_billing[0] or 0) + (this_date_other[0] or 0)
                this_date_billed_premium = (this_date_billing[1] or 0) + (this_date_other[1] or 0)
                this_date_billed_opc = (this_date_billing[2] or 0) + (this_date_other[2] or 0)
                
                # Get billing AFTER this billing date (up to and including selected_date)
                # Use ALL billing (not filtered by dealer_code) for correct FIFO calculation
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                    FROM sales_data
                    WHERE truck_number = ? AND sale_date > ? AND sale_date <= ?
                ''', (truck_number, billing_date, selected_date))
                later_billing = cursor.fetchone()
                
                # Also get other_dealers_billing AFTER this date
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                    FROM other_dealers_billing
                    WHERE truck_number = ? AND sale_date > ? AND sale_date <= ?
                ''', (truck_number, billing_date, selected_date))
                later_other = cursor.fetchone()
                
                later_billed_ppc = (later_billing[0] or 0) + (later_other[0] or 0)
                later_billed_premium = (later_billing[1] or 0) + (later_other[1] or 0)
                later_billed_opc = (later_billing[2] or 0) + (later_other[2] or 0)
                
                # Total billing for this truck = earlier (includes opening) + this date (ALL) + later
                total_billed_ppc = earlier_billed_ppc + this_date_billed_ppc + later_billed_ppc
                total_billed_premium = earlier_billed_premium + this_date_billed_premium + later_billed_premium
                total_billed_opc = earlier_billed_opc + this_date_billed_opc + later_billed_opc
                
                # Total pending for this truck
                truck_pending_ppc = max(0, total_billed_ppc - total_unloaded_ppc)
                truck_pending_premium = max(0, total_billed_premium - total_unloaded_premium)
                truck_pending_opc = max(0, total_billed_opc - total_unloaded_opc)
                
                # FIFO: Pending is attributed to LATER billings first, then this billing date
                # Pending for later billings = min(later_billed, truck_pending)
                pending_for_later_ppc = min(later_billed_ppc, truck_pending_ppc)
                pending_for_later_premium = min(later_billed_premium, truck_pending_premium)
                pending_for_later_opc = min(later_billed_opc, truck_pending_opc)
                
                # Pending for THIS DATE = remaining pending after later billings
                pending_for_this_date_ppc = max(0, truck_pending_ppc - pending_for_later_ppc)
                pending_for_this_date_premium = max(0, truck_pending_premium - pending_for_later_premium)
                pending_for_this_date_opc = max(0, truck_pending_opc - pending_for_later_opc)
                
                # For trucks with multiple plant_depot types, calculate card pending based on dealer_code
                # But only if there's global pending (otherwise all cards show 0)
                if plant_depot_count > 1 and dealer_codes and pending_for_this_date_ppc + pending_for_this_date_premium + pending_for_this_date_opc > 0.01:
                    # Get unloading for this card's dealer_codes
                    placeholders = ','.join(['?' for _ in dealer_codes])
                    cursor.execute(f'''
                        SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                        FROM vehicle_unloading
                        WHERE truck_number = ? AND unloading_date >= ? AND unloading_date <= ?
                          AND dealer_code IN ({placeholders})
                    ''', (truck_number, month_start, selected_date, *dealer_codes))
                    card_unloading = cursor.fetchone()
                    card_unloaded_ppc = card_unloading[0] or 0
                    card_unloaded_premium = card_unloading[1] or 0
                    card_unloaded_opc = card_unloading[2] or 0
                    
                    # Card's pending = card's billing - card's unloading (capped at 0)
                    pending_ppc = max(0, billed_ppc - card_unloaded_ppc)
                    pending_premium = max(0, billed_premium - card_unloaded_premium)
                    pending_opc = max(0, billed_opc - card_unloaded_opc)
                else:
                    # Single plant_depot OR no global pending - use global FIFO
                    pending_ppc = min(billed_ppc, pending_for_this_date_ppc)
                    pending_premium = min(billed_premium, pending_for_this_date_premium)
                    pending_opc = min(billed_opc, pending_for_this_date_opc)
                
                pending_total = pending_ppc + pending_premium + pending_opc
                this_unloaded_ppc = billed_ppc - pending_ppc
                this_unloaded_premium = billed_premium - pending_premium
                this_unloaded_opc = billed_opc - pending_opc
                this_total_unloaded = this_unloaded_ppc + this_unloaded_premium + this_unloaded_opc
                
                # Check if this billing was already fully unloaded BEFORE the selected date (using FIFO)
                # Get total unloading from month_start to before selected_date
                # Use ALL unloading (not filtered by dealer_code) for correct FIFO calculation
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                    FROM vehicle_unloading
                    WHERE truck_number = ? AND unloading_date >= ? AND unloading_date < ?
                ''', (truck_number, month_start, selected_date))
                unloading_before_today = cursor.fetchone()
                unloaded_before_today_ppc = unloading_before_today[0] or 0
                unloaded_before_today_premium = unloading_before_today[1] or 0
                unloaded_before_today_opc = unloading_before_today[2] or 0
                
                # FIFO: Earlier billings consume unloading first
                consumed_before_ppc = min(earlier_billed_ppc, unloaded_before_today_ppc)
                consumed_before_premium = min(earlier_billed_premium, unloaded_before_today_premium)
                consumed_before_opc = min(earlier_billed_opc, unloaded_before_today_opc)
                
                # Unloading available for THIS billing before today
                available_before_ppc = max(0, unloaded_before_today_ppc - consumed_before_ppc)
                available_before_premium = max(0, unloaded_before_today_premium - consumed_before_premium)
                available_before_opc = max(0, unloaded_before_today_opc - consumed_before_opc)
                
                unloaded_before_ppc = min(billed_ppc, available_before_ppc)
                unloaded_before_premium = min(billed_premium, available_before_premium)
                unloaded_before_opc = min(billed_opc, available_before_opc)
                
                # Check if there's any unloading on selected date for this truck
                cursor.execute('''
                    SELECT COUNT(*) FROM vehicle_unloading 
                    WHERE truck_number = ? AND unloading_date = ?
                ''', (truck_number, selected_date))
                has_unloading_on_selected_date = cursor.fetchone()[0] > 0
                
                # Skip if this billing was fully unloaded before selected date
                was_fully_unloaded_before = (
                    unloaded_before_ppc >= billed_ppc - 0.01 and
                    unloaded_before_premium >= billed_premium - 0.01 and
                    unloaded_before_opc >= billed_opc - 0.01
                )
                if was_fully_unloaded_before:
                    continue
                
                # At this point, this billing has pending material
                # Show if: has pending material OR has unloading on selected date
                # (pending_ppc/premium/opc is calculated using FIFO including today's unloading)
                has_pending_now = (pending_ppc > 0.01 or pending_premium > 0.01 or pending_opc > 0.01)
                
                # Skip if no pending and no unloading on selected date
                if not has_pending_now and not has_unloading_on_selected_date:
                    continue
                
                
                # Check if THIS BILLING received any unloading on the selected date
                # Filter by dealer_codes for this specific card
                unloaded_on_selected_date = False
                unloaded_today_ppc = 0
                unloaded_today_premium = 0
                unloaded_today_opc = 0
                
                # Check for ANY unloading on this truck on the selected date
                cursor.execute('''
                    SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                    FROM vehicle_unloading 
                    WHERE truck_number = ? AND unloading_date = ?
                ''', (truck_number, selected_date))
                today_unloading = cursor.fetchone()
                unloaded_today_ppc = today_unloading[0] or 0
                unloaded_today_premium = today_unloading[1] or 0
                unloaded_today_opc = today_unloading[2] or 0
                
                if unloaded_today_ppc > 0.01 or unloaded_today_premium > 0.01 or unloaded_today_opc > 0.01:
                    unloaded_on_selected_date = True
                
                # ONLY show previous day vehicles if they have unloading on the selected date
                # that matches the product types billed (PPC unloading for PPC billing, etc.)
                has_unloading_for_this_billing = False
                if unloaded_on_selected_date:
                    # Check if there's unloading on selected date that matches this billing's product types
                    # Build conditions based on what was billed
                    product_conditions = []
                    if billed_ppc > 0:
                        product_conditions.append("ppc_unloaded > 0")
                    if billed_premium > 0:
                        product_conditions.append("premium_unloaded > 0")
                    if billed_opc > 0:
                        product_conditions.append("opc_unloaded > 0")
                    
                    if product_conditions:
                        product_filter = " OR ".join(product_conditions)
                        if plant_depot_count > 1 and dealer_codes:
                            placeholders = ','.join(['?' for _ in dealer_codes])
                            cursor.execute(f'''
                                SELECT COUNT(*) FROM vehicle_unloading 
                                WHERE truck_number = ? AND unloading_date = ? 
                                  AND dealer_code IN ({placeholders})
                                  AND ({product_filter})
                            ''', (truck_number, selected_date, *dealer_codes))
                        else:
                            cursor.execute(f'''
                                SELECT COUNT(*) FROM vehicle_unloading 
                                WHERE truck_number = ? AND unloading_date = ? 
                                  AND ({product_filter})
                            ''', (truck_number, selected_date))
                        has_unloading_for_this_billing = cursor.fetchone()[0] > 0
                
                # Use FIFO-calculated pending values
                # pending_ppc, pending_premium, pending_opc were calculated above using FIFO
                has_pending_material = (pending_ppc > 0.01 or pending_premium > 0.01 or pending_opc > 0.01)
                
                # Show if there's pending material OR unloading on selected date for this billing
                if has_pending_material or has_unloading_for_this_billing:
                    # For previous day vehicles, ONLY show unloading on the selected date
                    # Show ALL unloading for the truck (not filtered by dealer_codes)
                    # because unloading may be recorded for dealers not in billing
                    prev_unloading = []
                    
                    cursor.execute('''
                        SELECT id, truck_number, unloading_dealer, unloading_point, 
                               ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                               notes, dealer_code, is_other_dealer, unloading_date
                        FROM vehicle_unloading 
                        WHERE truck_number = ? AND unloading_date = ?
                        ORDER BY unloading_date
                    ''', (truck_number, selected_date))
                    
                    # Cap unloading at billed amount (FIFO)
                    remaining_to_show_ppc = billed_ppc
                    remaining_to_show_premium = billed_premium
                    remaining_to_show_opc = billed_opc
                    
                    for urow in cursor.fetchall():
                        record_ppc = urow[4] or 0
                        record_premium = urow[5] or 0
                        record_opc = urow[6] or 0
                        
                        # Cap at remaining billed amount
                        show_ppc = min(record_ppc, remaining_to_show_ppc)
                        show_premium = min(record_premium, remaining_to_show_premium)
                        show_opc = min(record_opc, remaining_to_show_opc)
                        
                        remaining_to_show_ppc -= show_ppc
                        remaining_to_show_premium -= show_premium
                        remaining_to_show_opc -= show_opc
                        
                        # Only include if there's something to show
                        if show_ppc > 0.01 or show_premium > 0.01 or show_opc > 0.01:
                            prev_unloading.append({
                                'id': urow[0],
                                'truck_number': urow[1],
                                'unloading_dealer': urow[2],
                                'unloading_point': urow[3],
                                'ppc_unloaded': round(show_ppc, 2),
                                'premium_unloaded': round(show_premium, 2),
                                'opc_unloaded': round(show_opc, 2),
                                'unloaded_quantity': round(show_ppc + show_premium + show_opc, 2),
                                'notes': urow[8],
                                'dealer_code': urow[9],
                                'is_other_dealer': bool(urow[10]) if urow[10] is not None else False,
                                'unloading_date': urow[11]
                            })
                    
                    # Add to list as a previous day pending vehicle
                    # Use unique key with billing_date and plant_depot to separate different billing dates
                    vehicles_list.append({
                        'truck_number': f"{truck_number}_PREV_{billing_date}_{plant_depot}",
                        'display_truck_number': truck_number,
                        'billing_date': billing_date,
                        'plant_depot': plant_depot,
                        'dealer_codes': list(dealer_codes),
                        'invoices': [{
                            'invoice_number': 'PREVIOUS',
                            'dealer_code': '',
                            'dealer_name': dealers,
                            'ppc_quantity': billed_ppc,
                            'premium_quantity': billed_premium,
                            'opc_quantity': billed_opc,
                            'total_quantity': billed_ppc + billed_premium + billed_opc,
                            'total_value': 0,
                            'plant_depot': plant_depot
                        }],
                        'total_ppc': billed_ppc,
                        'total_premium': billed_premium,
                        'total_opc': billed_opc,
                        'total_quantity': billed_ppc + billed_premium + billed_opc,
                        'total_value': 0,
                        'unloading_details': prev_unloading,
                        'other_billing': [],
                        'is_opening_vehicle': False,
                        'is_previous_day_pending': True,
                        'original_billing_date': billing_date,
                        'has_pending_previous': False,
                        'previous_pending_qty': 0,
                        'previous_pending_ppc': 0,
                        'previous_pending_premium': 0,
                        'previous_pending_opc': 0,
                        'previous_billings': [],
                        'is_rebilled': False,
                        'truck_also_billed_today': truck_number in actual_trucks_billed_today,
                        'remaining_ppc': round(pending_ppc, 2),
                        'remaining_premium': round(pending_premium, 2),
                        'remaining_opc': round(pending_opc, 2),
                        'remaining_total': round(pending_total, 2)
                    })
                    # Add to set so we don't add duplicates
                    trucks_in_list.add(truck_number)
                    # Mark this truck+billing_date+plant_depot as added for previous day unloading
                    added_prev_trucks.add(f"{truck_number}_{billing_date}_{plant_depot}")
        except Exception as e:
            pass
        
        db.close()
        
        # Sort by: 1) Pending first, Complete last  2) Opening/Previous before Today  3) Truck number
        def sort_key(x):
            # Completed vehicles go to bottom (remaining_total <= 0.01)
            remaining = x.get('remaining_total', x.get('remaining_qty', 0)) or 0
            is_complete = 1 if remaining <= 0.01 else 0
            
            # Opening/Previous day vehicles before today's billing
            is_today = x.get('billing_date') == selected_date and not x.get('is_opening_vehicle') and not x.get('is_previous_day_pending')
            
            return (is_complete, 0 if not is_today else 1, x['truck_number'])
        
        vehicles = sorted(vehicles_list, key=sort_key)
        
        return jsonify({
            'success': True,
            'vehicles': vehicles,
            'date': selected_date
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)})

@app.route('/save_other_dealer_billing', methods=['POST'])
def save_other_dealer_billing():
    """Save billing for other dealers (outside area)"""
    try:
        data = request.get_json()
        
        truck_number = data.get('truck_number')
        billing_date = data.get('billing_date')
        dealer_name = data.get('dealer_name')
        invoice_number = data.get('invoice_number', '')
        plant_depot = data.get('plant_depot', 'DEPOT')
        
        ppc_quantity = data.get('ppc_quantity', 0) or 0
        premium_quantity = data.get('premium_quantity', 0) or 0
        opc_quantity = data.get('opc_quantity', 0) or 0
        ppc_value = data.get('ppc_value', 0) or 0
        premium_value = data.get('premium_value', 0) or 0
        opc_value = data.get('opc_value', 0) or 0
        
        if not truck_number or not billing_date or not dealer_name:
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        total_quantity = ppc_quantity + premium_quantity + opc_quantity
        total_value = ppc_value + premium_value + opc_value
        
        if total_quantity <= 0:
            return jsonify({'success': False, 'message': 'Total quantity must be greater than 0'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        cursor.execute('''
            INSERT INTO other_dealers_billing 
            (truck_number, sale_date, dealer_name, invoice_number, plant_depot,
             ppc_quantity, premium_quantity, opc_quantity, total_quantity,
             ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (truck_number, billing_date, dealer_name, invoice_number, plant_depot,
              ppc_quantity, premium_quantity, opc_quantity, total_quantity,
              ppc_value, premium_value, opc_value, total_value))
        
        db.conn.commit()
        db.close()
        
        return jsonify({
            'success': True,
            'message': 'Other dealer billing saved successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/save_vehicle_unloading', methods=['POST'])
def save_vehicle_unloading():
    """Save vehicle unloading details"""
    try:
        data = request.get_json()
        
        truck_number = data.get('truck_number')
        unloading_date = data.get('unloading_date')
        invoice_number = data.get('invoice_number')
        dealer_code = data.get('dealer_code')
        plant_depot = data.get('plant_depot')
        unloading_dealer = data.get('unloading_dealer')
        unloading_point = data.get('unloading_point')
        ppc_unloaded = data.get('ppc_unloaded', 0) or 0
        premium_unloaded = data.get('premium_unloaded', 0) or 0
        opc_unloaded = data.get('opc_unloaded', 0) or 0
        notes = data.get('notes', '')
        is_other_dealer = 1 if data.get('is_other_dealer', False) else 0
        
        if not truck_number or not unloading_date or not unloading_dealer or not unloading_point:
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        # Calculate total unloaded quantity for this entry
        total_unloaded = ppc_unloaded + premium_unloaded + opc_unloaded
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get total billed quantity for this truck (from sales_data and other_dealers_billing)
        cursor.execute('''
            SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), 
                   COALESCE(SUM(opc_quantity), 0), COALESCE(SUM(total_quantity), 0)
            FROM sales_data 
            WHERE truck_number = ?
        ''', (truck_number,))
        sales_billed = cursor.fetchone()
        
        cursor.execute('''
            SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), 
                   COALESCE(SUM(opc_quantity), 0), COALESCE(SUM(total_quantity), 0)
            FROM other_dealers_billing 
            WHERE truck_number = ?
        ''', (truck_number,))
        other_billed = cursor.fetchone()
        
        # Also check pending_vehicle_unloading (opening balance)
        cursor.execute('''
            SELECT COALESCE(SUM(ppc_qty), 0), COALESCE(SUM(premium_qty), 0), 
                   COALESCE(SUM(opc_qty), 0)
            FROM pending_vehicle_unloading 
            WHERE vehicle_number = ?
        ''', (truck_number,))
        opening_billed = cursor.fetchone()
        
        total_billed_ppc = (sales_billed[0] or 0) + (other_billed[0] or 0) + (opening_billed[0] or 0)
        total_billed_premium = (sales_billed[1] or 0) + (other_billed[1] or 0) + (opening_billed[1] or 0)
        total_billed_opc = (sales_billed[2] or 0) + (other_billed[2] or 0) + (opening_billed[2] or 0)
        total_billed = total_billed_ppc + total_billed_premium + total_billed_opc
        
        # Get already unloaded quantity for this truck
        cursor.execute('''
            SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), 
                   COALESCE(SUM(opc_unloaded), 0), COALESCE(SUM(unloaded_quantity), 0)
            FROM vehicle_unloading 
            WHERE truck_number = ?
        ''', (truck_number,))
        already_unloaded = cursor.fetchone()
        
        already_unloaded_ppc = already_unloaded[0] or 0
        already_unloaded_premium = already_unloaded[1] or 0
        already_unloaded_opc = already_unloaded[2] or 0
        already_unloaded_total = already_unloaded[3] or 0
        
        # Validate: new unloading + already unloaded should not exceed total billed
        # Check by product type
        if (already_unloaded_ppc + ppc_unloaded) > (total_billed_ppc + 0.01):
            db.close()
            return jsonify({
                'success': False, 
                'message': f'PPC unloading ({already_unloaded_ppc + ppc_unloaded:.2f} MT) exceeds total billed PPC ({total_billed_ppc:.2f} MT) for this vehicle'
            })
        
        if (already_unloaded_premium + premium_unloaded) > (total_billed_premium + 0.01):
            db.close()
            return jsonify({
                'success': False, 
                'message': f'Premium unloading ({already_unloaded_premium + premium_unloaded:.2f} MT) exceeds total billed Premium ({total_billed_premium:.2f} MT) for this vehicle'
            })
        
        if (already_unloaded_opc + opc_unloaded) > (total_billed_opc + 0.01):
            db.close()
            return jsonify({
                'success': False, 
                'message': f'OPC unloading ({already_unloaded_opc + opc_unloaded:.2f} MT) exceeds total billed OPC ({total_billed_opc:.2f} MT) for this vehicle'
            })
        
        # Check total
        if (already_unloaded_total + total_unloaded) > (total_billed + 0.01):
            db.close()
            return jsonify({
                'success': False, 
                'message': f'Total unloading ({already_unloaded_total + total_unloaded:.2f} MT) exceeds total billed ({total_billed:.2f} MT) for this vehicle'
            })
        
        # Insert new unloading record (allows multiple unloadings per vehicle)
        cursor.execute('''
            INSERT INTO vehicle_unloading 
            (truck_number, unloading_date, invoice_number, dealer_code, plant_depot, unloading_dealer, 
             unloading_point, ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, notes, is_other_dealer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (truck_number, unloading_date, invoice_number, dealer_code, plant_depot, unloading_dealer,
              unloading_point, ppc_unloaded, premium_unloaded, opc_unloaded, total_unloaded, notes, is_other_dealer))
        
        db.conn.commit()
        db.close()
        
        return jsonify({
            'success': True,
            'message': 'Unloading details saved successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/delete_unloading/<int:unloading_id>', methods=['DELETE'])
def delete_unloading(unloading_id):
    """Delete a specific unloading record"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        cursor.execute('DELETE FROM vehicle_unloading WHERE id = ?', (unloading_id,))
        db.conn.commit()
        db.close()
        
        return jsonify({'success': True, 'message': 'Unloading record deleted'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# ============== Opening Material Balance Management ==============

@app.route('/opening_material_balance')
def opening_material_balance():
    """Hidden page for managing opening material balances"""
    return render_template('opening_material_balance.html')

@app.route('/get_dealers_list')
def get_dealers_list():
    """Get list of all dealers - one entry per dealer_code"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Group by dealer_code and pick the shortest dealer_name (without suffix like "(8632)")
        cursor.execute('''
            SELECT dealer_code, MIN(dealer_name) as dealer_name
            FROM sales_data 
            WHERE dealer_code IS NOT NULL AND dealer_code != '' 
              AND dealer_name IS NOT NULL AND dealer_name != ''
            GROUP BY dealer_code
            ORDER BY MIN(dealer_name)
        ''')
        
        # First pass: collect all dealers
        raw_dealers = [(row[0], row[1]) for row in cursor.fetchall()]
        
        # Find duplicate names
        name_counts = {}
        for dealer_code, dealer_name in raw_dealers:
            name_counts[dealer_name] = name_counts.get(dealer_name, 0) + 1
        
        # Build final list, appending last 4 digits for duplicates
        dealers = []
        for dealer_code, dealer_name in raw_dealers:
            if name_counts.get(dealer_name, 0) > 1:
                # Append last 4 digits of dealer_code for duplicates
                display_name = f"{dealer_name} ({str(dealer_code)[-4:]})"
            else:
                display_name = dealer_name
            dealers.append({
                'dealer_code': dealer_code,
                'dealer_name': display_name
            })
        
        db.close()
        return jsonify({'dealers': dealers})
        
    except Exception as e:
        return jsonify({'dealers': [], 'error': str(e)})

@app.route('/get_opening_material_balance', methods=['POST'])
def get_opening_material_balance():
    """Get opening material balance data for a month"""
    try:
        data = request.get_json()
        month_year = data.get('month_year', '')
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get pending vehicles
        cursor.execute('''
            SELECT id, vehicle_number, billing_date, dealer_code, ppc_qty, premium_qty, opc_qty
            FROM pending_vehicle_unloading
            WHERE month_year = ?
            ORDER BY billing_date, vehicle_number
        ''', (month_year,))
        
        pending_vehicles = []
        for row in cursor.fetchall():
            pending_vehicles.append({
                'id': row[0],
                'vehicle_number': row[1],
                'billing_date': row[2],
                'dealer_code': row[3],
                'ppc_qty': row[4] or 0,
                'premium_qty': row[5] or 0,
                'opc_qty': row[6] or 0
            })
        
        # Get dealer balances
        cursor.execute('''
            SELECT id, dealer_code, ppc_qty, premium_qty, opc_qty, dealer_name, dealer_type
            FROM opening_material_balance
            WHERE month_year = ?
            ORDER BY dealer_code
        ''', (month_year,))
        
        dealer_balances = []
        for row in cursor.fetchall():
            dealer_balances.append({
                'id': row[0],
                'dealer_code': row[1],
                'ppc_qty': row[2] or 0,
                'premium_qty': row[3] or 0,
                'opc_qty': row[4] or 0,
                'dealer_name': row[5] or '',
                'dealer_type': row[6] or 'Active'
            })
        
        db.close()
        
        return jsonify({
            'pending_vehicles': pending_vehicles,
            'dealer_balances': dealer_balances
        })
        
    except Exception as e:
        return jsonify({'pending_vehicles': [], 'dealer_balances': [], 'error': str(e)})

@app.route('/save_opening_material_balance', methods=['POST'])
def save_opening_material_balance():
    """Save opening material balance data"""
    try:
        data = request.get_json()
        month_year = data.get('month_year', '')
        pending_vehicles = data.get('pending_vehicles', [])
        dealer_balances = data.get('dealer_balances', [])
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_vehicle_unloading (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month_year TEXT NOT NULL,
                vehicle_number TEXT NOT NULL,
                billing_date DATE,
                dealer_code INTEGER,
                ppc_qty REAL DEFAULT 0,
                premium_qty REAL DEFAULT 0,
                opc_qty REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS opening_material_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month_year TEXT NOT NULL,
                dealer_code TEXT NOT NULL,
                dealer_name TEXT,
                dealer_type TEXT DEFAULT 'Active',
                ppc_qty REAL DEFAULT 0,
                premium_qty REAL DEFAULT 0,
                opc_qty REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(month_year, dealer_code)
            )
        ''')
        
        # Add columns if they don't exist (for existing tables)
        try:
            cursor.execute('ALTER TABLE opening_material_balance ADD COLUMN dealer_name TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE opening_material_balance ADD COLUMN dealer_type TEXT DEFAULT "Active"')
        except:
            pass
        
        # Clear existing data for this month
        cursor.execute('DELETE FROM pending_vehicle_unloading WHERE month_year = ?', (month_year,))
        cursor.execute('DELETE FROM opening_material_balance WHERE month_year = ?', (month_year,))
        
        # Insert pending vehicles
        for vehicle in pending_vehicles:
            cursor.execute('''
                INSERT INTO pending_vehicle_unloading 
                (month_year, vehicle_number, billing_date, dealer_code, ppc_qty, premium_qty, opc_qty)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (month_year, vehicle['vehicle_number'], vehicle['billing_date'], 
                  vehicle['dealer_code'], vehicle.get('ppc_qty', 0), 
                  vehicle.get('premium_qty', 0), vehicle.get('opc_qty', 0)))
        
        # Insert dealer balances
        for balance in dealer_balances:
            cursor.execute('''
                INSERT INTO opening_material_balance 
                (month_year, dealer_code, dealer_name, dealer_type, ppc_qty, premium_qty, opc_qty)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (month_year, balance['dealer_code'], balance.get('dealer_name', ''),
                  balance.get('dealer_type', 'Active'), balance.get('ppc_qty', 0),
                  balance.get('premium_qty', 0), balance.get('opc_qty', 0)))
        
        db.conn.commit()
        db.close()
        
        return jsonify({'success': True, 'message': 'Data saved successfully'})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/unloading_query')
def unloading_query_page():
    """Render the unloading query page"""
    return render_template('unloading_query.html')

@app.route('/api/unloading_query', methods=['POST'])
def api_unloading_query():
    """API endpoint for querying unloading data"""
    try:
        data = request.get_json()
        query_type = data.get('query_type')
        
        db = sqlite3.connect(DB_PATH)
        cursor = db.cursor()
        
        # Build query based on type
        base_query = '''
            SELECT id, truck_number, unloading_date, unloading_dealer, unloading_point,
                   ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity,
                   dealer_code, is_other_dealer
            FROM vehicle_unloading
            WHERE 1=1
        '''
        params = []
        
        if query_type == 'date':
            # Date-wise query
            from_date = data.get('from_date')
            to_date = data.get('to_date')
            if from_date:
                base_query += ' AND unloading_date >= ?'
                params.append(from_date)
            if to_date:
                base_query += ' AND unloading_date <= ?'
                params.append(to_date)
                
        elif query_type == 'truck':
            # Truck-wise query
            truck_number = data.get('truck_number', '').strip()
            if truck_number:
                base_query += ' AND truck_number LIKE ?'
                params.append(f'%{truck_number}%')
            from_date = data.get('from_date')
            to_date = data.get('to_date')
            if from_date:
                base_query += ' AND unloading_date >= ?'
                params.append(from_date)
            if to_date:
                base_query += ' AND unloading_date <= ?'
                params.append(to_date)
                
        elif query_type == 'dealer':
            # Dealer-wise query
            dealer_code = data.get('dealer_code')
            if dealer_code:
                base_query += ' AND dealer_code = ?'
                params.append(dealer_code)
            from_date = data.get('from_date')
            to_date = data.get('to_date')
            if from_date:
                base_query += ' AND unloading_date >= ?'
                params.append(from_date)
            if to_date:
                base_query += ' AND unloading_date <= ?'
                params.append(to_date)
        
        base_query += ' ORDER BY unloading_date DESC, truck_number'
        
        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        
        records = []
        total_ppc = 0
        total_premium = 0
        total_opc = 0
        total_qty = 0
        
        for row in rows:
            ppc = row[5] or 0
            premium = row[6] or 0
            opc = row[7] or 0
            qty = row[8] or 0
            
            total_ppc += ppc
            total_premium += premium
            total_opc += opc
            total_qty += qty
            
            records.append({
                'id': row[0],
                'truck_number': row[1],
                'unloading_date': row[2],
                'unloading_dealer': row[3],
                'unloading_point': row[4],
                'ppc_unloaded': ppc,
                'premium_unloaded': premium,
                'opc_unloaded': opc,
                'unloaded_quantity': qty,
                'dealer_code': row[9],
                'is_other_dealer': bool(row[10]) if row[10] is not None else False
            })
        
        db.close()
        
        return jsonify({
            'success': True,
            'records': records,
            'summary': {
                'ppc': total_ppc,
                'premium': total_premium,
                'opc': total_opc,
                'total': total_qty,
                'count': len(records)
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/dealer_summary_report', methods=['POST'])
def api_dealer_summary_report():
    """API endpoint for generating dealer summary report with opening balance, daily billing/unloading, and closing balance"""
    try:
        from dateutil.relativedelta import relativedelta
        from datetime import datetime, timedelta
        
        data = request.get_json()
        dealer_code = data.get('dealer_code')
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        if not dealer_code or not from_date or not to_date:
            return jsonify({'success': False, 'error': 'Dealer code, from_date, and to_date are required'})
        
        db = sqlite3.connect(DB_PATH)
        cursor = db.cursor()
        
        # Get dealer name
        cursor.execute('SELECT dealer_name FROM dealers WHERE dealer_code = ?', (dealer_code,))
        dealer_row = cursor.fetchone()
        dealer_name = dealer_row[0] if dealer_row else f'Dealer {dealer_code}'
        
        # Convert bags to MT: 1 MT = 20 bags, so bags = MT * 20
        MT_TO_BAGS = 20
        
        # Calculate opening balance (balance as of day before from_date)
        from_date_dt = datetime.strptime(from_date, '%Y-%m-%d')
        month_year = from_date_dt.strftime('%Y-%m')
        month_start = month_year + '-01'
        day_before_from = (from_date_dt - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Get opening material balance for the month
        opening_ppc = 0
        opening_premium = 0
        opening_opc = 0
        
        # First check opening_material_balance table for this month
        cursor.execute('''
            SELECT ppc_qty, premium_qty, opc_qty
            FROM opening_material_balance
            WHERE month_year = ? AND dealer_code = ?
        ''', (month_year, str(dealer_code)))
        opening_row = cursor.fetchone()
        
        if opening_row:
            opening_ppc = (opening_row[0] or 0) * MT_TO_BAGS
            opening_premium = (opening_row[1] or 0) * MT_TO_BAGS
            opening_opc = (opening_row[2] or 0) * MT_TO_BAGS
        else:
            # Calculate from previous month closing if no opening balance entry
            prev_month_dt = from_date_dt.replace(day=1) - timedelta(days=1)
            prev_month_year = prev_month_dt.strftime('%Y-%m')
            prev_month_start = prev_month_year + '-01'
            prev_month_end = from_date_dt.replace(day=1).strftime('%Y-%m-%d')
            
            # Check previous month opening
            cursor.execute('''
                SELECT ppc_qty, premium_qty, opc_qty
                FROM opening_material_balance
                WHERE month_year = ? AND dealer_code = ?
            ''', (prev_month_year, str(dealer_code)))
            prev_opening = cursor.fetchone()
            
            if prev_opening:
                opening_ppc = (prev_opening[0] or 0) * MT_TO_BAGS
                opening_premium = (prev_opening[1] or 0) * MT_TO_BAGS
                opening_opc = (prev_opening[2] or 0) * MT_TO_BAGS
            
            # Add previous month billing
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                FROM sales_data
                WHERE dealer_code = ? AND sale_date >= ? AND sale_date < ?
            ''', (dealer_code, prev_month_start, prev_month_end))
            prev_billing = cursor.fetchone()
            if prev_billing:
                opening_ppc += (prev_billing[0] or 0) * MT_TO_BAGS
                opening_premium += (prev_billing[1] or 0) * MT_TO_BAGS
                opening_opc += (prev_billing[2] or 0) * MT_TO_BAGS
            
            # Subtract previous month unloading
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                FROM vehicle_unloading
                WHERE dealer_code = ? AND unloading_date >= ? AND unloading_date < ?
            ''', (dealer_code, prev_month_start, prev_month_end))
            prev_unloading = cursor.fetchone()
            if prev_unloading:
                opening_ppc -= (prev_unloading[0] or 0) * MT_TO_BAGS
                opening_premium -= (prev_unloading[1] or 0) * MT_TO_BAGS
                opening_opc -= (prev_unloading[2] or 0) * MT_TO_BAGS
        
        # Add billing from month start to day before from_date (if from_date is not month start)
        if from_date != month_start:
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_quantity), 0), COALESCE(SUM(premium_quantity), 0), COALESCE(SUM(opc_quantity), 0)
                FROM sales_data
                WHERE dealer_code = ? AND sale_date >= ? AND sale_date < ?
            ''', (dealer_code, month_start, from_date))
            billing_before = cursor.fetchone()
            if billing_before:
                opening_ppc += (billing_before[0] or 0) * MT_TO_BAGS
                opening_premium += (billing_before[1] or 0) * MT_TO_BAGS
                opening_opc += (billing_before[2] or 0) * MT_TO_BAGS
            
            # Subtract unloading from month start to day before from_date
            cursor.execute('''
                SELECT COALESCE(SUM(ppc_unloaded), 0), COALESCE(SUM(premium_unloaded), 0), COALESCE(SUM(opc_unloaded), 0)
                FROM vehicle_unloading
                WHERE dealer_code = ? AND unloading_date >= ? AND unloading_date < ?
            ''', (dealer_code, month_start, from_date))
            unloading_before = cursor.fetchone()
            if unloading_before:
                opening_ppc -= (unloading_before[0] or 0) * MT_TO_BAGS
                opening_premium -= (unloading_before[1] or 0) * MT_TO_BAGS
                opening_opc -= (unloading_before[2] or 0) * MT_TO_BAGS
        
        opening_total = opening_ppc + opening_premium + opening_opc
        
        # Get daily billing and unloading data
        daily_data = []
        current_ppc = opening_ppc
        current_premium = opening_premium
        current_opc = opening_opc
        
        # Generate date range
        current_date = from_date_dt
        to_date_dt = datetime.strptime(to_date, '%Y-%m-%d')
        
        while current_date <= to_date_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            day_data = {
                'date': date_str,
                'billing': [],
                'unloading': [],
                'closing': None
            }
            
            # Get billing for this date
            cursor.execute('''
                SELECT truck_number, ppc_quantity, premium_quantity, opc_quantity, total_quantity
                FROM sales_data
                WHERE dealer_code = ? AND sale_date = ?
            ''', (dealer_code, date_str))
            billing_rows = cursor.fetchall()
            
            day_billing_ppc = 0
            day_billing_premium = 0
            day_billing_opc = 0
            
            for row in billing_rows:
                ppc = (row[1] or 0) * MT_TO_BAGS
                premium = (row[2] or 0) * MT_TO_BAGS
                opc = (row[3] or 0) * MT_TO_BAGS
                total = ppc + premium + opc
                
                day_billing_ppc += ppc
                day_billing_premium += premium
                day_billing_opc += opc
                
                day_data['billing'].append({
                    'truck': row[0] or '',
                    'ppc': ppc,
                    'premium': premium,
                    'opc': opc,
                    'total': total
                })
            
            # Get unloading for this date
            cursor.execute('''
                SELECT truck_number, unloading_point, ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity
                FROM vehicle_unloading
                WHERE dealer_code = ? AND unloading_date = ?
            ''', (dealer_code, date_str))
            unloading_rows = cursor.fetchall()
            
            day_unloading_ppc = 0
            day_unloading_premium = 0
            day_unloading_opc = 0
            
            for row in unloading_rows:
                ppc = (row[2] or 0) * MT_TO_BAGS
                premium = (row[3] or 0) * MT_TO_BAGS
                opc = (row[4] or 0) * MT_TO_BAGS
                total = ppc + premium + opc
                
                day_unloading_ppc += ppc
                day_unloading_premium += premium
                day_unloading_opc += opc
                
                day_data['unloading'].append({
                    'truck': row[0] or '',
                    'point': row[1] or '',
                    'ppc': ppc,
                    'premium': premium,
                    'opc': opc,
                    'total': total
                })
            
            # Calculate closing balance for the day
            current_ppc += day_billing_ppc - day_unloading_ppc
            current_premium += day_billing_premium - day_unloading_premium
            current_opc += day_billing_opc - day_unloading_opc
            
            # Only add day data if there's any activity
            if day_data['billing'] or day_data['unloading']:
                day_data['closing'] = {
                    'ppc': current_ppc,
                    'premium': current_premium,
                    'opc': current_opc,
                    'total': current_ppc + current_premium + current_opc
                }
                daily_data.append(day_data)
            
            current_date += timedelta(days=1)
        
        db.close()
        
        return jsonify({
            'success': True,
            'dealer_code': dealer_code,
            'dealer_name': dealer_name,
            'from_date': from_date,
            'to_date': to_date,
            'opening_balance': {
                'ppc': opening_ppc,
                'premium': opening_premium,
                'opc': opening_opc,
                'total': opening_total
            },
            'daily_data': daily_data,
            'closing_balance': {
                'ppc': current_ppc,
                'premium': current_premium,
                'opc': current_opc,
                'total': current_ppc + current_premium + current_opc
            }
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})

@app.route('/dealer_financial_balance')
def dealer_financial_balance():
    """Dealer financial balance management page"""
    return render_template('dealer_financial_balance.html')

@app.route('/api/dealer_financial_balance', methods=['POST'])
def get_dealer_financial_balance():
    """Get dealer financial balance data for a month"""
    try:
        from dateutil.relativedelta import relativedelta
        
        data = request.get_json()
        month_year = data.get('month_year', '')
        
        if not month_year:
            return jsonify({'success': False, 'error': 'Month year is required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Get month start and end dates
        month_start = month_year + '-01'
        # Get last day of month
        year, month = map(int, month_year.split('-'))
        if month == 12:
            next_month_start = f'{year + 1}-01-01'
        else:
            next_month_start = f'{year}-{month + 1:02d}-01'
        
        # Calculate previous month
        current_month_dt = datetime.strptime(month_year + '-01', '%Y-%m-%d')
        prev_month_dt = current_month_dt - relativedelta(months=1)
        prev_month_year = prev_month_dt.strftime('%Y-%m')
        prev_month_start = prev_month_year + '-01'
        
        # Get all dealers from current month AND previous month
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name FROM (
                SELECT dealer_code, dealer_name FROM sales_data
                WHERE sale_date >= ? AND sale_date < ?
                UNION
                SELECT dealer_code, dealer_name FROM collections_data
                WHERE posting_date >= ? AND posting_date < ?
                UNION
                SELECT dealer_code, dealer_name FROM sales_data
                WHERE sale_date >= ? AND sale_date < ?
                UNION
                SELECT dealer_code, dealer_name FROM collections_data
                WHERE posting_date >= ? AND posting_date < ?
                UNION
                SELECT dealer_code, dealer_name FROM opening_balances
                WHERE month_year = ?
            )
            ORDER BY dealer_name
        ''', (month_start, next_month_start, month_start, next_month_start,
              prev_month_start, month_start, prev_month_start, month_start, prev_month_year))
        
        dealers_map = {}
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            dealers_map[dealer_code] = {
                'dealer_code': dealer_code,
                'dealer_name': row[1],
                'opening_balance': 0,
                'purchase_value': 0,
                'collection': 0,
                'credit_note': 0,
                'debit_note': 0,
                'gst_hold': 0
            }
        
        # Get purchase values (total_purchase_value from sales_data)
        cursor.execute('''
            SELECT dealer_code, SUM(total_purchase_value) as total_purchase
            FROM sales_data
            WHERE sale_date >= ? AND sale_date < ?
            GROUP BY dealer_code
        ''', (month_start, next_month_start))
        
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            if dealer_code in dealers_map:
                dealers_map[dealer_code]['purchase_value'] = row[1] or 0
        
        # Get collections
        cursor.execute('''
            SELECT dealer_code, SUM(amount) as total_collection
            FROM collections_data
            WHERE posting_date >= ? AND posting_date < ?
            GROUP BY dealer_code
        ''', (month_start, next_month_start))
        
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            if dealer_code in dealers_map:
                dealers_map[dealer_code]['collection'] = row[1] or 0
        
        # Get manual opening balances for this month (if any)
        manual_opening = {}
        cursor.execute('''
            SELECT dealer_code, opening_balance
            FROM opening_balances
            WHERE month_year = ?
        ''', (month_year,))
        for row in cursor.fetchall():
            manual_opening[str(row[0])] = row[1] or 0
        
        # Always calculate previous month's closing for dealers without manual opening
        # Get previous month's opening balances
        cursor.execute('''
            SELECT dealer_code, opening_balance
            FROM opening_balances
            WHERE month_year = ?
        ''', (prev_month_year,))
        
        prev_opening = {}
        for row in cursor.fetchall():
            prev_opening[str(row[0])] = row[1] or 0
        
        # Get previous month's sales
        cursor.execute('''
            SELECT dealer_code, SUM(total_purchase_value)
            FROM sales_data
            WHERE sale_date >= ? AND sale_date < ?
            GROUP BY dealer_code
        ''', (prev_month_start, month_start))
        
        prev_sales = {}
        for row in cursor.fetchall():
            prev_sales[str(row[0])] = row[1] or 0
        
        # Get previous month's collections
        cursor.execute('''
            SELECT dealer_code, SUM(amount)
            FROM collections_data
            WHERE posting_date >= ? AND posting_date < ?
            GROUP BY dealer_code
        ''', (prev_month_start, month_start))
        
        prev_collections = {}
        for row in cursor.fetchall():
            prev_collections[str(row[0])] = row[1] or 0
        
        # Get previous month's credit notes
        prev_credits = {}
        try:
            cursor.execute('''
                SELECT dealer_code, SUM(credit_discount)
                FROM credit_discounts
                WHERE month_year = ?
                GROUP BY dealer_code
            ''', (prev_month_year,))
            for row in cursor.fetchall():
                prev_credits[str(row[0])] = row[1] or 0
        except:
            pass
        
        # Get previous month's debit notes
        prev_debits = {}
        try:
            cursor.execute('''
                SELECT dealer_code, SUM(debit_amount)
                FROM debit_notes
                WHERE month_year = ?
                GROUP BY dealer_code
            ''', (prev_month_year,))
            for row in cursor.fetchall():
                prev_debits[str(row[0])] = row[1] or 0
        except:
            pass
        
        # Set opening balance: use manual if exists, otherwise calculate from previous month
        for dealer_code in dealers_map:
            if dealer_code in manual_opening:
                # Use manual opening balance
                dealers_map[dealer_code]['opening_balance'] = manual_opening[dealer_code]
            else:
                # Calculate from previous month's closing
                opening = prev_opening.get(dealer_code, 0)
                sales = prev_sales.get(dealer_code, 0)
                collections = prev_collections.get(dealer_code, 0)
                credits = prev_credits.get(dealer_code, 0)
                debits = prev_debits.get(dealer_code, 0)
                dealers_map[dealer_code]['opening_balance'] = round(opening + sales - collections - credits + debits, 2)
        
        # Get credit notes and GST hold for current month
        cursor.execute('''
            SELECT dealer_code, credit_discount, gst_hold
            FROM credit_discounts
            WHERE month_year = ?
        ''', (month_year,))
        
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            if dealer_code in dealers_map:
                dealers_map[dealer_code]['credit_note'] = row[1] or 0
                dealers_map[dealer_code]['gst_hold'] = row[2] or 0
        
        # Get debit notes for current month
        cursor.execute('''
            SELECT dealer_code, debit_amount
            FROM debit_notes
            WHERE month_year = ?
        ''', (month_year,))
        
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            if dealer_code in dealers_map:
                dealers_map[dealer_code]['debit_note'] = row[1] or 0
        
        db.close()
        
        # Convert to list and sort by dealer name
        dealers = sorted(dealers_map.values(), key=lambda x: x['dealer_name'])
        
        return jsonify({
            'success': True,
            'dealers': dealers
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/save_dealer_financial_balance', methods=['POST'])
def save_dealer_financial_balance():
    """Save dealer financial balance data"""
    try:
        data = request.get_json()
        month_year = data.get('month_year', '')
        dealers = data.get('dealers', [])
        
        if not month_year:
            return jsonify({'success': False, 'error': 'Month year is required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        for dealer in dealers:
            dealer_code = dealer.get('dealer_code')
            dealer_name = dealer.get('dealer_name')
            opening_balance = dealer.get('opening_balance', 0)
            credit_note = dealer.get('credit_note', 0)
            debit_note = dealer.get('debit_note', 0)
            gst_hold = dealer.get('gst_hold', 0)
            
            # Upsert opening balance
            cursor.execute('''
                INSERT INTO opening_balances (dealer_code, dealer_name, opening_balance, month_year, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET opening_balance = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, opening_balance, month_year, opening_balance, dealer_name))
            
            # Upsert credit note and GST hold
            cursor.execute('''
                INSERT INTO credit_discounts (dealer_code, dealer_name, credit_discount, gst_hold, month_year, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET credit_discount = ?, gst_hold = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, credit_note, gst_hold, month_year, credit_note, gst_hold, dealer_name))
            
            # Upsert debit note
            cursor.execute('''
                INSERT INTO debit_notes (dealer_code, dealer_name, debit_amount, month_year, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET debit_amount = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, debit_note, month_year, debit_note, dealer_name))
        
        db.conn.commit()
        db.close()
        
        return jsonify({'success': True, 'message': f'Saved data for {len(dealers)} dealers'})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/upload_dealer_statement', methods=['POST'])
def upload_dealer_statement():
    """Upload and parse dealer PDF statement to extract CRN/DRN values"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file uploaded'})
        
        file = request.files['file']
        dealer_code = request.form.get('dealer_code')
        dealer_name = request.form.get('dealer_name')
        
        if not dealer_code:
            return jsonify({'success': False, 'message': 'Dealer code is required'})
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'})
        
        if not file.filename.lower().endswith('.pdf'):
            return jsonify({'success': False, 'message': 'Only PDF files are allowed'})
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            # Parse PDF
            import PyPDF2
            import re
            
            def _parse_amount_token(token: str):
                try:
                    return float(token.replace(',', ''))
                except Exception:
                    return None

            def _parse_crn_with_pdfplumber(pdf_path: str):
                try:
                    import pdfplumber
                except Exception:
                    return None

                amount_re = re.compile(r'^\d{1,3}(?:,\d{3})*\.\d{1,2}$|^\d+\.\d{1,2}$')
                date_re = re.compile(r'^\d{2}\.\d{2}\.\d{4}$')
                doc_re = re.compile(r'^(?:\d{8,}|DL\d{10,}|RJ\d{10,})$')
                crn_re = re.compile(r'^CRN[-/A-Za-z0-9]{4,}$')
                doc_any_re = re.compile(r'(?:DL\d{10,}|RJ\d{10,}|\d{8,})')
                crn_any_re = re.compile(r'(CRN[-/A-Za-z0-9]{4,})')

                total = 0.0
                entries = []
                seen = set()

                with pdfplumber.open(pdf_path) as pdf:
                    for page in pdf.pages:
                        words = page.extract_words(use_text_flow=True)
                        if not words:
                            continue

                        # Group words by visual line using the 'top' coordinate.
                        lines_map = {}
                        for w in words:
                            top_key = int(round(float(w.get('top', 0)) / 2.0) * 2)
                            lines_map.setdefault(top_key, []).append(w)

                        current = None
                        current_posting_date = None
                        pending_crn = False

                        def _flush_current():
                            nonlocal total
                            if not current:
                                return

                            if not current.get('has_crn'):
                                return

                            amount_tokens = current.get('amount_tokens') or []
                            if len(amount_tokens) < 2:
                                return

                            credit_token = amount_tokens[-2]
                            balance_token = amount_tokens[-1]
                            credit = _parse_amount_token(credit_token)
                            balance = _parse_amount_token(balance_token)
                            if credit is None or balance is None:
                                return

                            if not (100 < credit < 10000000):
                                return

                            posting_date = current.get('posting_date') or ''
                            doc_no = current.get('doc_no') or ''
                            dedup_key = f"{posting_date}|{doc_no}|{credit_token}|{balance_token}"
                            if dedup_key in seen:
                                return
                            seen.add(dedup_key)

                            total += credit
                            entries.append({'line': current.get('preview', '')[:100], 'amount': credit})
                            # Clear pending CRN marker once we've successfully recorded a CRN entry.
                            nonlocal pending_crn
                            pending_crn = False

                        for _, line_words in sorted(lines_map.items(), key=lambda x: x[0]):
                            line_words = sorted(line_words, key=lambda w: float(w.get('x0', 0)))
                            tokens = [w.get('text', '').strip() for w in line_words if w.get('text', '').strip()]
                            if not tokens:
                                continue

                            # Skip header lines
                            if 'INV/CRN' in tokens or ('Posting' in tokens and 'Doc' in tokens and 'No.' in tokens):
                                continue

                            posting_date = next((t for t in tokens if date_re.match(t)), None)
                            if posting_date:
                                current_posting_date = posting_date

                            # Doc/CRN tokens are sometimes concatenated (e.g. DL...6337...CRN-DL/25-26/)
                            doc_in_line = None
                            crn_in_line = None
                            for t in tokens:
                                if doc_in_line is None:
                                    m_doc = doc_any_re.search(t)
                                    if m_doc:
                                        doc_in_line = m_doc.group(0)
                                if crn_in_line is None:
                                    m_crn = crn_any_re.search(t)
                                    if m_crn:
                                        crn_in_line = m_crn.group(1)
                                if doc_in_line is not None and crn_in_line is not None:
                                    break

                            if doc_in_line:
                                # New transaction boundary: doc number changed (even within same posting date)
                                if current is not None and current.get('doc_no') and doc_in_line != current.get('doc_no'):
                                    _flush_current()
                                    current = None

                                if current is None:
                                    current = {
                                        'posting_date': current_posting_date or posting_date or '',
                                        'doc_no': doc_in_line,
                                        'has_crn': pending_crn,
                                        'amount_tokens': [],
                                        'preview': ' '.join(tokens)
                                    }

                            # Determine whether this visual line contains a CRN marker and/or amounts.
                            line_amount_tokens = [t for t in tokens if amount_re.match(t)]
                            # Only consider it a CRN if it explicitly starts with "CRN" pattern
                            # Exclude lines that contain invoice patterns (INV, Invoice, etc.)
                            line_text = ' '.join(tokens).upper()
                            is_invoice_line = 'INV' in line_text or 'INVOICE' in line_text or 'BILLING' in line_text
                            line_has_crn = not is_invoice_line and (crn_in_line is not None or any('CRN-' in t.upper() for t in tokens) or any(t.upper() == 'CRN' for t in tokens))

                            # Some PDFs place the CRN marker on one line, and the amounts on the next line.
                            # If we see a CRN marker but no amounts, carry a pending flag forward.
                            if line_has_crn and len(line_amount_tokens) < 2:
                                pending_crn = True

                            # If CRN is present on the same line where we start the transaction, capture it.
                            if current is not None and line_has_crn:
                                current['has_crn'] = True

                            # If a CRN marker was seen in a prior wrapped line, apply it to the current transaction.
                            if current is not None and pending_crn:
                                current['has_crn'] = True

                            if current is None and current_posting_date:
                                # If we haven't started a transaction yet, skip until we see a doc number.
                                continue

                            if current is None:
                                continue

                            # If we started with a date-only line and later find doc_no in wrapped lines
                            if current.get('doc_no') is None and doc_in_line:
                                current['doc_no'] = doc_in_line

                            if not current.get('has_crn') and not is_invoice_line:
                                current['has_crn'] = any(t.upper() == 'CRN' or crn_re.match(t) for t in tokens) or any('CRN-' in t.upper() for t in tokens) or any(crn_any_re.search(t) for t in tokens)

                            current['amount_tokens'].extend(line_amount_tokens)
                            if len(current.get('preview', '')) < 200:
                                current['preview'] = (current.get('preview', '') + ' ' + ' '.join(tokens)).strip()

                        _flush_current()

                return {
                    'total': round(total, 2),
                    'entries': entries
                }
            
            pdf = PyPDF2.PdfReader(open(filepath, 'rb'))
            full_text = '\n'.join([page.extract_text() for page in pdf.pages])
            
            # Extract opening balance
            opening_match = re.search(r'Opening Balance\(s\):\s*([\d,]+\.\d{2})\s*\(?(DR|CR)?\)?', full_text)
            opening_balance = 0
            if opening_match:
                opening_balance = float(opening_match.group(1).replace(',', ''))
                if opening_match.group(2) == 'CR':
                    opening_balance = -opening_balance
            
            # Extract period dates
            period_match = re.search(r'PERIOD:\s*(\d{2}\.\d{2}\.\d{4})\s*To\s*(\d{2}\.\d{2}\.\d{4})', full_text)
            period_start = None
            period_end = None
            month_year = None
            if period_match:
                period_start = period_match.group(1)
                period_end = period_match.group(2)
                # Extract month_year from period_end (format: DD.MM.YYYY)
                parts = period_end.split('.')
                if len(parts) == 3:
                    month_year = f"{parts[2]}-{parts[1]}"  # YYYY-MM format
            
            if not month_year:
                # Default to current month
                month_year = datetime.now().strftime('%Y-%m')
            
            # Extract GST Hold amount (closing balance)
            gst_hold_amount = 0
            # Look for "For GST Hold" section header with asterisk and table structure
            # Pattern: "For GST Hold *" followed by table headers and "Opening Balance"
            gst_hold_match = re.search(r'For GST Hold\s*\*\s*\n.*?(?:Posting Date|Document No\.).*?Opening Balance\s+(\d{3,}(?:,\d{3})*\.\d{2})[-]?', full_text, re.MULTILINE | re.DOTALL)
            if gst_hold_match:
                # Extract opening balance from the GST Hold section
                opening_amount = float(gst_hold_match.group(1).replace(',', ''))
                
                # Get the full GST Hold section to check for closing balance
                section_start = gst_hold_match.start()
                # Find section end (next "For" section or end of text)
                section_end_match = re.search(r'\n(?:For\s+[A-Z]|Total\s+Outstanding)', full_text[section_start:])
                if section_end_match:
                    gst_section = full_text[section_start:section_start + section_end_match.start()]
                else:
                    gst_section = full_text[section_start:section_start + 500]  # Limit to 500 chars
                
                # Find all amounts in the section (excluding dates)
                amounts = re.findall(r'(\d{3,}(?:,\d{3})*\.\d{2})[-]?', gst_section)
                if amounts:
                    # Take the last amount as closing balance
                    gst_hold_amount = float(amounts[-1].replace(',', ''))
            
            # Extract CRN (Credit Note) entries
            total_crn = 0
            crn_entries = []

            crn_plumber = _parse_crn_with_pdfplumber(filepath)
            if crn_plumber is not None:
                total_crn = crn_plumber['total']
                crn_entries = crn_plumber['entries']
            else:
                seen_crn_keys = set()
            
            # Pattern for CRN entries with amounts in Credit column
            # CRN entries have amounts that reduce the balance (credits)
            lines = full_text.split('\n')
            if crn_plumber is None:
                for i, line in enumerate(lines):
                    # Skip header lines containing 'INV/CRN'
                    if 'CRN' in line and 'INV/CRN' not in line:
                        # Get context around the line (CRN info spans multiple lines)
                        context = ' '.join(lines[max(0,i-1):min(len(lines),i+6)])

                        # Try to extract a CRN reference so we don't double count when the
                        # same CRN spans multiple wrapped lines in the extracted PDF text.
                        # Fallback to a normalized context key.
                        crn_ref_match = re.search(r'\bCRN[-/A-Za-z0-9]{4,}\b', context)
                        if crn_ref_match:
                            crn_key = crn_ref_match.group(0).strip()
                        else:
                            crn_key = re.sub(r'\s+', ' ', context.strip())[:200]

                        if crn_key in seen_crn_keys:
                            continue

                        # Look for amount pairs - credit amount followed by running balance.
                        # Choose a plausible credit amount to avoid picking balances.
                        amount_pairs = re.findall(r'(\d+[\d,]*\.\d{2})\s+(\d+[\d,]*\.\d{2})', context)
                        candidate_amount = None

                        for a_str, b_str in amount_pairs:
                            try:
                                a = float(a_str.replace(',', ''))
                                b = float(b_str.replace(',', ''))
                            except Exception:
                                continue

                            # Credit amount should generally be smaller than the running balance
                            # and within a sane band. (Band chosen to avoid small noise numbers.)
                            if 100 < a < 10000000 and b != a:
                                if b > a:
                                    candidate_amount = a
                                    break

                        if candidate_amount is not None:
                            seen_crn_keys.add(crn_key)
                            total_crn += candidate_amount
                            crn_entries.append({'line': line[:100], 'amount': candidate_amount})
            
            # Extract DRN (Debit Note) entries
            total_drn = 0
            drn_entries = []
            
            for i, line in enumerate(lines):
                if 'DRN' in line or 'DEBIT NOTE' in line.upper():
                    # Look for amount pattern
                    amounts = re.findall(r'(\d+\.\d{2})', line)
                    if amounts:
                        for amt_str in amounts:
                            amt = float(amt_str)
                            if amt > 100 and amt < 10000000:
                                total_drn += amt
                                drn_entries.append({'line': line[:100], 'amount': amt})
                                break
            
            # Get existing values from database for comparison
            # Use the same logic as financial balance page to get opening balance
            from dateutil.relativedelta import relativedelta
            
            db = SalesCollectionsDatabase(DB_PATH)
            cursor = db.conn.cursor()
            
            # Calculate opening balance the same way as financial balance page
            month_start = month_year + '-01'
            year, month = map(int, month_year.split('-'))
            if month == 12:
                next_month_start = f'{year + 1}-01-01'
            else:
                next_month_start = f'{year}-{month + 1:02d}-01'
            
            current_month_dt = datetime.strptime(month_year + '-01', '%Y-%m-%d')
            prev_month_dt = current_month_dt - relativedelta(months=1)
            prev_month_year = prev_month_dt.strftime('%Y-%m')
            prev_month_start = prev_month_year + '-01'
            
            # Check if manual opening balance exists for this month
            cursor.execute('SELECT opening_balance FROM opening_balances WHERE dealer_code = ? AND month_year = ?', 
                          (dealer_code, month_year))
            manual_opening = cursor.fetchone()
            
            if manual_opening:
                existing_opening_balance = manual_opening[0] or 0
            else:
                # Auto-calculate from previous month's closing
                # Previous month opening
                cursor.execute('SELECT opening_balance FROM opening_balances WHERE dealer_code = ? AND month_year = ?',
                              (dealer_code, prev_month_year))
                prev_opening_row = cursor.fetchone()
                prev_opening = prev_opening_row[0] if prev_opening_row else 0
                
                # Previous month sales
                cursor.execute('SELECT SUM(total_purchase_value) FROM sales_data WHERE dealer_code = ? AND sale_date >= ? AND sale_date < ?',
                              (dealer_code, prev_month_start, month_start))
                prev_sales_row = cursor.fetchone()
                prev_sales = prev_sales_row[0] if prev_sales_row and prev_sales_row[0] else 0
                
                # Previous month collections
                cursor.execute('SELECT SUM(amount) FROM collections_data WHERE dealer_code = ? AND posting_date >= ? AND posting_date < ?',
                              (dealer_code, prev_month_start, month_start))
                prev_coll_row = cursor.fetchone()
                prev_collections = prev_coll_row[0] if prev_coll_row and prev_coll_row[0] else 0
                
                # Previous month credit notes
                cursor.execute('SELECT SUM(credit_discount) FROM credit_discounts WHERE dealer_code = ? AND month_year = ?',
                              (dealer_code, prev_month_year))
                prev_credit_row = cursor.fetchone()
                prev_credits = prev_credit_row[0] if prev_credit_row and prev_credit_row[0] else 0
                
                # Previous month debit notes
                cursor.execute('SELECT SUM(debit_amount) FROM debit_notes WHERE dealer_code = ? AND month_year = ?',
                              (dealer_code, prev_month_year))
                prev_debit_row = cursor.fetchone()
                prev_debits = prev_debit_row[0] if prev_debit_row and prev_debit_row[0] else 0
                
                # Opening = prev_opening + prev_sales - prev_collections - prev_credits + prev_debits
                existing_opening_balance = round(prev_opening + prev_sales - prev_collections - prev_credits + prev_debits, 2)
            
            # Get existing credit note and GST hold
            cursor.execute('''
                SELECT credit_discount, gst_hold FROM credit_discounts
                WHERE dealer_code = ? AND month_year = ?
            ''', (dealer_code, month_year))
            existing_crn = cursor.fetchone()
            existing_crn_value = existing_crn[0] if existing_crn else 0
            existing_gst_hold = existing_crn[1] if existing_crn and len(existing_crn) > 1 else 0
            
            # Get existing debit note
            cursor.execute('''
                SELECT debit_amount FROM debit_notes
                WHERE dealer_code = ? AND month_year = ?
            ''', (dealer_code, month_year))
            existing_drn = cursor.fetchone()
            existing_drn_value = existing_drn[0] if existing_drn else 0
            
            db.close()
            
            # Clean up temp file
            os.remove(filepath)
            
            return jsonify({
                'success': True,
                'data': {
                    'dealer_code': dealer_code,
                    'dealer_name': dealer_name,
                    'month_year': month_year,
                    'period_start': period_start,
                    'period_end': period_end,
                    'opening_balance': {
                        'pdf_value': opening_balance,
                        'db_value': existing_opening_balance,
                        'match': abs(opening_balance - existing_opening_balance) < 1
                    },
                    'credit_notes': {
                        'pdf_value': total_crn,
                        'db_value': existing_crn_value,
                        'entries_count': len(crn_entries)
                    },
                    'debit_notes': {
                        'pdf_value': total_drn,
                        'db_value': existing_drn_value,
                        'entries_count': len(drn_entries)
                    },
                    'gst_hold': {
                        'pdf_value': gst_hold_amount,
                        'db_value': existing_gst_hold
                    }
                }
            })
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'success': False, 'message': f'Error parsing PDF: {str(e)}'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/save_statement_data', methods=['POST'])
def save_statement_data():
    """Save extracted statement data (CRN/DRN) to database"""
    try:
        data = request.get_json()
        dealer_code = data.get('dealer_code')
        dealer_name = data.get('dealer_name')
        month_year = data.get('month_year')
        credit_note = data.get('credit_note', 0)
        debit_note = data.get('debit_note', 0)
        gst_hold = data.get('gst_hold', 0)
        opening_balance = data.get('opening_balance')
        
        if not dealer_code or not month_year:
            return jsonify({'success': False, 'message': 'Dealer code and month_year are required'})
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Update credit note and GST hold if provided
        if credit_note > 0 or gst_hold > 0:
            cursor.execute('''
                INSERT INTO credit_discounts (dealer_code, dealer_name, credit_discount, gst_hold, month_year, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET credit_discount = ?, gst_hold = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, credit_note, gst_hold, month_year, credit_note, gst_hold, dealer_name))
        
        # Update debit note if provided
        if debit_note > 0:
            cursor.execute('''
                INSERT INTO debit_notes (dealer_code, dealer_name, debit_amount, month_year, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET debit_amount = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, debit_note, month_year, debit_note, dealer_name))
        
        # Update opening balance if provided
        if opening_balance is not None:
            cursor.execute('''
                INSERT INTO opening_balances (dealer_code, dealer_name, opening_balance, month_year, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET opening_balance = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, opening_balance, month_year, opening_balance, dealer_name))
        
        db.conn.commit()
        db.close()
        
        return jsonify({'success': True, 'message': 'Statement data saved successfully'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

if __name__ == '__main__':
    # Ensure upload directory exists
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    
    # Run the app
    app.run(debug=True, host='0.0.0.0', port=5001)

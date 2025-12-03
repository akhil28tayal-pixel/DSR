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
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
        error_rows = []
        
        # Group by invoice to aggregate product line items
        invoice_groups = df.groupby('Invoice Number')
        
        for invoice_number, invoice_df in invoice_groups:
            try:
                # Get common invoice data from first row
                first_row = invoice_df.iloc[0]
                
                # Extract basic invoice info
                sale_date = pd.to_datetime(first_row['Invoice Date']).strftime('%Y-%m-%d')
                dealer_code = int(first_row['Customer Code'])
                dealer_name = str(first_row['Customer Name/Sold To']).strip()
                truck_number = str(first_row['Truck Number']).strip()
                plant_depot = str(first_row['Plant/Depot']).strip()
                
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
                    INSERT OR REPLACE INTO sales_data 
                    (sale_date, dealer_code, dealer_name, invoice_number, 
                     ppc_quantity, premium_quantity, opc_quantity, total_quantity, 
                     ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value, 
                     truck_number, plant_depot)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (sale_date, dealer_code, dealer_name, invoice_number,
                      ppc_quantity, premium_quantity, opc_quantity, total_quantity, 
                      ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value,
                      truck_number, plant_depot))
                
                successful_invoices += 1
                
            except Exception as row_error:
                error_rows.append(f"Invoice {invoice_number}: {str(row_error)}")
        
        db.conn.commit()
        db.close()
        
        if successful_invoices > 0:
            message = f"Successfully uploaded {successful_invoices} invoices from {len(df)} product line items"
            if error_rows:
                message += f". {len(error_rows)} invoices had errors."
            return jsonify({'success': True, 'message': message, 'errors': error_rows})
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
                        
                        # Insert or update sales data with truck number
                        cursor.execute('''
                            INSERT OR REPLACE INTO sales_data 
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
                
                if successful_rows > 0:
                    message = f"Successfully uploaded {successful_rows} sales records"
                    if error_rows:
                        message += f". {len(error_rows)} rows had errors."
                    return jsonify({'success': True, 'message': message, 'errors': error_rows})
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
                
                if successful_rows > 0:
                    message = f"Successfully uploaded {successful_rows} collections records"
                    if error_rows:
                        message += f". {len(error_rows)} rows had errors."
                    return jsonify({'success': True, 'message': message, 'errors': error_rows})
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
        
        # Calculate closing balances
        closing_balances = {}
        for dealer_key, opening_balance in opening_balances_map.items():
            sales = sales_data.get(dealer_key, 0)
            collections = collections_data.get(dealer_key, 0)
            closing = opening_balance + sales - collections
            closing_balances[dealer_key] = round(closing, 2)
        
        db.close()
        return closing_balances
        
    except Exception as e:
        return {}

def get_opening_balances_with_auto_calculation(month_year):
    """Get opening balances with auto-calculation from previous month's closing balances"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # First, get manual opening balances for this month
        cursor.execute('''
            SELECT dealer_code, dealer_name, opening_balance 
            FROM opening_balances 
            WHERE month_year = ?
        ''', (month_year,))
        
        manual_balances = {}
        for row in cursor.fetchall():
            key = f"{row[0]}_{row[1]}"
            manual_balances[key] = round(row[2], 2)
        
        # Get all dealers who have transactions in this month
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name FROM (
                SELECT dealer_code, dealer_name FROM sales_data WHERE strftime('%Y-%m', sale_date) = ?
                UNION
                SELECT dealer_code, dealer_name FROM collections_data WHERE strftime('%Y-%m', posting_date) = ?
            )
        ''', (month_year, month_year))
        
        all_dealers = cursor.fetchall()
        
        # For dealers without manual opening balances, try to get from previous month's closing
        result_balances = {}
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
        
        # Get sales data for the selected date
        cursor.execute('''
            SELECT dealer_code, dealer_name, ppc_quantity, premium_quantity, opc_quantity, 
                   total_quantity, ppc_purchase_value, premium_purchase_value, opc_purchase_value, total_purchase_value
            FROM sales_data 
            WHERE sale_date = ?
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
        
        # Get collections data for the selected date
        cursor.execute('''
            SELECT dealer_code, dealer_name, amount
            FROM collections_data 
            WHERE posting_date = ?
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
        
        # Get all unique dealers
        all_dealers = set()
        for sale in sales + cumulative_sales:
            all_dealers.add((sale['dealer_code'], sale['dealer_name']))
        for collection in collections + cumulative_collections:
            all_dealers.add((collection['dealer_code'], collection['dealer_name']))
        
        for dealer_code, dealer_name in all_dealers:
            key = f"{dealer_code}_{dealer_name}"
            opening_balances.append({
                'dealer_code': dealer_code,
                'dealer_name': dealer_name,
                'opening_balance': round(opening_balances_map.get(key, 0), 2)
            })
        
        db.close()
        
        return jsonify({
            'success': True,
            'sales': sales,
            'collections': collections,
            'cumulative_sales': cumulative_sales,
            'cumulative_collections': cumulative_collections,
            'opening_balances': opening_balances,
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
        
        if not dealer_code or not billing_date:
            return jsonify({'success': False, 'message': 'Dealer code and date are required'})
        
        # Generate the WhatsApp message
        message = generate_whatsapp_message(int(dealer_code), billing_date, truck_numbers)
        
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
            ppc_bags = int(ppc * 20)
            premium_bags = int(premium * 20)
            opc_bags = int(opc * 20)
            total_bags = int(total_qty * 20)
            
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
        total_unloaded_bags = int((total_ppc_unloaded + total_premium_unloaded + total_opc_unloaded) * 20)
        message_lines.append(f"*Total Unloaded: {total_unloaded_bags} bags*")
        message_lines.append("")
        
        # Material Balance section
        message_lines.append("─" * 25)
        message_lines.append("*📊 Material Balance:*")
        message_lines.append("")
        
        # Opening balance (in bags) - show even if negative
        opening_ppc_bags = int(opening['ppc'] * 20)
        opening_premium_bags = int(opening['premium'] * 20)
        opening_opc_bags = int(opening['opc'] * 20)
        
        message_lines.append("*Opening Balance:*")
        balance_parts = []
        if opening_ppc_bags != 0:
            balance_parts.append(f"PPC: {opening_ppc_bags}")
        if opening_premium_bags != 0:
            balance_parts.append(f"Premium: {opening_premium_bags}")
        if opening_opc_bags != 0:
            balance_parts.append(f"OPC: {opening_opc_bags}")
        
        if balance_parts:
            message_lines.append(f"  {', '.join(balance_parts)} bags")
        else:
            message_lines.append("  No opening balance")
        
        message_lines.append("")
        
        # Today's billing (in bags) - show total only in material balance
        billed_ppc_bags = int(total_ppc_billed * 20)
        billed_premium_bags = int(total_premium_billed * 20)
        billed_opc_bags = int(total_opc_billed * 20)
        
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
        unloaded_ppc_bags = int(total_ppc_unloaded * 20)
        unloaded_premium_bags = int(total_premium_unloaded * 20)
        unloaded_opc_bags = int(total_opc_unloaded * 20)
        
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
        closing_ppc_bags = int(closing_ppc * 20)
        closing_premium_bags = int(closing_premium * 20)
        closing_opc_bags = int(closing_opc * 20)
        
        message_lines.append("*Closing Balance:*")
        closing_parts = []
        if closing_ppc_bags != 0:
            closing_parts.append(f"PPC: {closing_ppc_bags}")
        if closing_premium_bags != 0:
            closing_parts.append(f"Premium: {closing_premium_bags}")
        if closing_opc_bags != 0:
            closing_parts.append(f"OPC: {closing_opc_bags}")
        
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
    """Calculate opening balance for a dealer using manual opening balance + cumulative transactions"""
    
    # Get the month of the selected date
    month_year = before_date[:7]  # Extract YYYY-MM from date
    month_start = month_year + '-01'  # First day of the month
    
    # Start with manual opening balance from admin page (for 1st of month)
    manual_opening = {'ppc': 0, 'premium': 0, 'opc': 0}
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
    except Exception as e:
        # Table might not exist yet
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
        
        # 1. Get dealers from opening_material_balance for this month
        try:
            cursor.execute('''
                SELECT dealer_code, dealer_name, dealer_type
                FROM opening_material_balance
                WHERE month_year = ?
            ''', (month_year,))
            for row in cursor.fetchall():
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
        
        # Get all unloading aggregated by truck
        cursor.execute('''
            SELECT truck_number,
                   SUM(ppc_unloaded) as unloaded_ppc,
                   SUM(premium_unloaded) as unloaded_premium,
                   SUM(opc_unloaded) as unloaded_opc
            FROM vehicle_unloading
            WHERE unloading_date <= ?
            GROUP BY truck_number
        ''', (selected_date,))
        
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
        try:
            cursor.execute('''
                SELECT vehicle_number, ppc_qty, premium_qty, opc_qty
                FROM pending_vehicle_unloading
                WHERE month_year = ?
            ''', (month_year,))
            for orow in cursor.fetchall():
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
                    WHERE unloading_date <= ?
                    GROUP BY truck_number
                ) u ON p.vehicle_number = u.truck_number
                WHERE p.month_year = ?
            ''', (selected_date, month_year))
            
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
    """Get all unique dealers from the database"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name 
            FROM sales_data 
            WHERE dealer_name IS NOT NULL AND dealer_name != ''
            ORDER BY dealer_name
        ''')
        
        dealers = []
        for row in cursor.fetchall():
            dealers.append({
                'dealer_code': row[0],
                'dealer_name': row[1]
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
                   plant_depot, sale_date
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
        cursor.execute('''
            SELECT vehicle_number, billing_date, dealer_code, ppc_qty, premium_qty, opc_qty
            FROM pending_vehicle_unloading
            WHERE month_year = ?
        ''', (month_year,))
        for row in cursor.fetchall():
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
        
        # Get ALL unloading details for trucks billed today (not just today's unloading)
        all_unloading_map = {}
        if truck_numbers_today:
            placeholders = ','.join(['?' for _ in truck_numbers_today])
            cursor.execute(f'''
                SELECT id, truck_number, unloading_dealer, unloading_point, 
                       ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                       notes, dealer_code, is_other_dealer, unloading_date
                FROM vehicle_unloading 
                WHERE truck_number IN ({placeholders}) AND unloading_date <= ?
            ''', (*truck_numbers_today, selected_date))
            
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
                    'unloading_date': row[11]
                })
        
        # Get unloading details for today only (for display)
        cursor.execute('''
            SELECT id, truck_number, unloading_dealer, unloading_point, 
                   ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                   notes, dealer_code, is_other_dealer, unloading_date
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
                'unloading_date': row[11]
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
        
        # Build vehicles list - consolidate same truck on same day, separate different days
        vehicles_list = []
        trucks_today = {}  # Consolidate by truck number for today's billing
        
        for row in invoices_data:
            truck_number = row[0]
            invoice_number = row[1]
            ppc_qty = row[4] or 0
            premium_qty = row[5] or 0
            opc_qty = row[6] or 0
            total_qty = row[7] or 0
            
            if truck_number not in trucks_today:
                trucks_today[truck_number] = {
                    'truck_number': truck_number,
                    'invoices': [],
                    'total_ppc': 0,
                    'total_premium': 0,
                    'total_opc': 0,
                    'total_quantity': 0,
                    'total_value': 0,
                    'billing_date': selected_date,
                    'unloading_details': unloading_map.get(truck_number, []),
                    'other_billing': other_billing_map.get(truck_number, [])
                }
            
            # Add invoice to this truck's list
            trucks_today[truck_number]['invoices'].append({
                'invoice_number': invoice_number,
                'dealer_code': row[2],
                'dealer_name': row[3],
                'ppc_quantity': ppc_qty,
                'premium_quantity': premium_qty,
                'opc_quantity': opc_qty,
                'total_quantity': total_qty,
                'total_value': row[11] or 0,
                'plant_depot': row[12]
            })
            
            # Accumulate totals
            trucks_today[truck_number]['total_ppc'] += ppc_qty
            trucks_today[truck_number]['total_premium'] += premium_qty
            trucks_today[truck_number]['total_opc'] += opc_qty
            trucks_today[truck_number]['total_quantity'] += total_qty
            trucks_today[truck_number]['total_value'] += row[11] or 0
        
        # Add other_billing quantities to truck totals
        for truck_number, truck_data in trucks_today.items():
            other_billings = truck_data.get('other_billing', [])
            for ob in other_billings:
                truck_data['total_ppc'] += ob.get('ppc_quantity', 0) or 0
                truck_data['total_premium'] += ob.get('premium_quantity', 0) or 0
                truck_data['total_opc'] += ob.get('opc_quantity', 0) or 0
                truck_data['total_quantity'] += ob.get('total_quantity', 0) or 0
                truck_data['total_value'] += ob.get('total_value', 0) or 0
        
        # Now check for previous day billings that weren't fully unloaded
        for truck_number, truck_data in trucks_today.items():
            has_pending_previous = False
            previous_pending_qty = 0
            previous_pending_ppc = 0
            previous_pending_premium = 0
            previous_pending_opc = 0
            
            if truck_number in previous_billings:
                total_prev_billed_ppc = sum(p['ppc'] for p in previous_billings[truck_number])
                total_prev_billed_premium = sum(p['premium'] for p in previous_billings[truck_number])
                total_prev_billed_opc = sum(p['opc'] for p in previous_billings[truck_number])
                
                total_prev_unloaded_ppc = 0
                total_prev_unloaded_premium = 0
                total_prev_unloaded_opc = 0
                if truck_number in all_unloading_map:
                    for u in all_unloading_map[truck_number]:
                        if u['unloading_date'] < selected_date:
                            total_prev_unloaded_ppc += u['ppc_unloaded']
                            total_prev_unloaded_premium += u['premium_unloaded']
                            total_prev_unloaded_opc += u['opc_unloaded']
                
                previous_pending_ppc = max(0, total_prev_billed_ppc - total_prev_unloaded_ppc)
                previous_pending_premium = max(0, total_prev_billed_premium - total_prev_unloaded_premium)
                previous_pending_opc = max(0, total_prev_billed_opc - total_prev_unloaded_opc)
                previous_pending_qty = previous_pending_ppc + previous_pending_premium + previous_pending_opc
                
                if previous_pending_qty > 0.01:
                    has_pending_previous = True
            
            truck_data['has_pending_previous'] = has_pending_previous
            truck_data['previous_pending_qty'] = round(previous_pending_qty, 2)
            truck_data['previous_pending_ppc'] = round(previous_pending_ppc, 2)
            truck_data['previous_pending_premium'] = round(previous_pending_premium, 2)
            truck_data['previous_pending_opc'] = round(previous_pending_opc, 2)
            truck_data['previous_billings'] = previous_billings.get(truck_number, [])
            truck_data['is_rebilled'] = has_pending_previous
            truck_data['is_opening_vehicle'] = False
            
            # Calculate if today's billing is fully unloaded
            # Total billed = previous pending + today's billing
            total_billed_ppc = previous_pending_ppc + truck_data['total_ppc']
            total_billed_premium = previous_pending_premium + truck_data['total_premium']
            total_billed_opc = previous_pending_opc + truck_data['total_opc']
            
            # Check if this truck has an opening balance (previous month carryover)
            # Opening balance unloading should NOT count against today's billing
            opening_balance_ppc = 0
            opening_balance_premium = 0
            opening_balance_opc = 0
            cursor.execute('''
                SELECT ppc_qty, premium_qty, opc_qty 
                FROM pending_vehicle_unloading 
                WHERE vehicle_number = ?
            ''', (truck_number,))
            opening_row = cursor.fetchone()
            if opening_row:
                opening_balance_ppc = opening_row[0] or 0
                opening_balance_premium = opening_row[1] or 0
                opening_balance_opc = opening_row[2] or 0
            
            # Get total unloaded for this truck (all dates up to today)
            total_unloaded_ppc = 0
            total_unloaded_premium = 0
            total_unloaded_opc = 0
            if truck_number in all_unloading_map:
                for u in all_unloading_map[truck_number]:
                    total_unloaded_ppc += u['ppc_unloaded']
                    total_unloaded_premium += u['premium_unloaded']
                    total_unloaded_opc += u['opc_unloaded']
            
            # Unloading that applies to today's billing = total unloaded - opening balance consumed - previous billings consumed
            # FIFO: Opening balance first, then previous billings, then today's billing
            unloaded_for_opening_ppc = min(opening_balance_ppc, total_unloaded_ppc)
            unloaded_for_opening_premium = min(opening_balance_premium, total_unloaded_premium)
            unloaded_for_opening_opc = min(opening_balance_opc, total_unloaded_opc)
            
            # Get total previous billings (not just pending - ALL previous billings in the month)
            # EXCLUDE opening balance since it's already handled separately above
            total_prev_billed_ppc = 0
            total_prev_billed_premium = 0
            total_prev_billed_opc = 0
            if truck_number in previous_billings:
                for p in previous_billings[truck_number]:
                    # Skip opening balance entries - they're already counted in opening_balance_*
                    if p.get('dealers') == 'Opening Balance':
                        continue
                    total_prev_billed_ppc += p['ppc']
                    total_prev_billed_premium += p['premium']
                    total_prev_billed_opc += p['opc']
            
            # Unloading consumed by previous billings (after opening)
            remaining_after_opening_ppc = total_unloaded_ppc - unloaded_for_opening_ppc
            remaining_after_opening_premium = total_unloaded_premium - unloaded_for_opening_premium
            remaining_after_opening_opc = total_unloaded_opc - unloaded_for_opening_opc
            
            unloaded_for_prev_ppc = min(total_prev_billed_ppc, remaining_after_opening_ppc)
            unloaded_for_prev_premium = min(total_prev_billed_premium, remaining_after_opening_premium)
            unloaded_for_prev_opc = min(total_prev_billed_opc, remaining_after_opening_opc)
            
            # Unloading available for today's billing (after opening AND previous billings consumed)
            unloaded_for_today_ppc = remaining_after_opening_ppc - unloaded_for_prev_ppc
            unloaded_for_today_premium = remaining_after_opening_premium - unloaded_for_prev_premium
            unloaded_for_today_opc = remaining_after_opening_opc - unloaded_for_prev_opc
            
            # Calculate remaining pending (today's billing only - previous pending already accounted for separately)
            # Today's billing remaining = today's billed - unloading for today
            remaining_ppc = max(0, truck_data['total_ppc'] - unloaded_for_today_ppc)
            remaining_premium = max(0, truck_data['total_premium'] - unloaded_for_today_premium)
            remaining_opc = max(0, truck_data['total_opc'] - unloaded_for_today_opc)
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
            today_has_unloading = (unloaded_for_today_ppc > 0.01 or 
                                   unloaded_for_today_premium > 0.01 or 
                                   unloaded_for_today_opc > 0.01)
            
            if not today_has_unloading:
                # All unloading went to previous billings, not today's
                truck_data['unloading_details'] = []
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
                            'unloading_dealer': u['unloading_dealer'],
                            'unloading_point': u['unloading_point'],
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
            cursor.execute('''
                SELECT p.vehicle_number, p.billing_date, p.dealer_code, 
                       p.ppc_qty, p.premium_qty, p.opc_qty
                FROM pending_vehicle_unloading p
                WHERE p.month_year = ?
            ''', (month_year,))
            
            pending_data = cursor.fetchall()
            
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
                
                # Get unloading details for this truck (any date up to selected date)
                cursor.execute('''
                    SELECT id, truck_number, unloading_dealer, unloading_point, 
                           ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                           notes, dealer_code, is_other_dealer, unloading_date
                    FROM vehicle_unloading 
                    WHERE truck_number = ? AND unloading_date <= ?
                ''', (truck_number, selected_date))
                
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
                truck_already_billed_today = truck_number in trucks_today
                
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
            cursor.execute('''
                SELECT truck_number, billing_date,
                       GROUP_CONCAT(DISTINCT dealers) as dealers,
                       SUM(ppc) as ppc,
                       SUM(premium) as premium,
                       SUM(opc) as opc
                FROM (
                    SELECT truck_number, sale_date as billing_date, dealer_name as dealers,
                           ppc_quantity as ppc, premium_quantity as premium, opc_quantity as opc
                    FROM sales_data
                    WHERE sale_date >= ? AND sale_date < ?
                      AND truck_number IS NOT NULL AND truck_number != ''
                    UNION ALL
                    SELECT truck_number, sale_date as billing_date, dealer_name as dealers,
                           ppc_quantity as ppc, premium_quantity as premium, opc_quantity as opc
                    FROM other_dealers_billing
                    WHERE sale_date >= ? AND sale_date < ?
                      AND truck_number IS NOT NULL AND truck_number != ''
                )
                GROUP BY truck_number, billing_date
                ORDER BY billing_date, truck_number
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
                truck_total_billed[truck]['ppc'] += row[3] or 0
                truck_total_billed[truck]['premium'] += row[4] or 0
                truck_total_billed[truck]['opc'] += row[5] or 0
            
            # Get unloading for all trucks up to selected date
            cursor.execute('''
                SELECT truck_number,
                       SUM(ppc_unloaded) as ppc,
                       SUM(premium_unloaded) as premium,
                       SUM(opc_unloaded) as opc
                FROM vehicle_unloading
                WHERE unloading_date <= ?
                GROUP BY truck_number
            ''', (selected_date,))
            
            truck_unloaded = {}
            for row in cursor.fetchall():
                truck_unloaded[row[0]] = {
                    'ppc': row[1] or 0,
                    'premium': row[2] or 0,
                    'opc': row[3] or 0
                }
            
            # Find trucks with pending material - show each billing date separately
            # Track which truck+date combinations we've added
            added_truck_dates = set()
            for v in vehicles_list:
                display_truck = v.get('display_truck_number', v.get('truck_number', ''))
                if display_truck.endswith('_OPENING'):
                    display_truck = display_truck[:-8]
                if display_truck.endswith('_PREV'):
                    display_truck = display_truck[:-5]
                added_truck_dates.add(f"{display_truck}_{v.get('billing_date', '')}")
            
            # Track cumulative unloading consumed per truck (FIFO)
            truck_unloading_consumed = {}
            
            # First, consume unloading for opening balance vehicles
            for truck, ob in opening_balance_map.items():
                unloaded = truck_unloaded.get(truck, {'ppc': 0, 'premium': 0, 'opc': 0})
                consumed_ppc = min(ob['ppc'], unloaded['ppc'])
                consumed_premium = min(ob['premium'], unloaded['premium'])
                consumed_opc = min(ob['opc'], unloaded['opc'])
                truck_unloading_consumed[truck] = {
                    'ppc': consumed_ppc,
                    'premium': consumed_premium,
                    'opc': consumed_opc
                }
            
            for row in prev_day_billings:
                truck_number = row[0]
                billing_date = row[1]
                dealers = row[2]
                billed_ppc = row[3] or 0
                billed_premium = row[4] or 0
                billed_opc = row[5] or 0
                
                # Skip if this specific truck+date is already in list
                truck_date_key = f"{truck_number}_{billing_date}"
                if truck_date_key in added_truck_dates:
                    continue
                
                # Initialize consumed tracking if needed
                if truck_number not in truck_unloading_consumed:
                    truck_unloading_consumed[truck_number] = {'ppc': 0, 'premium': 0, 'opc': 0}
                
                # Get total unloading for this truck
                total_unloaded = truck_unloaded.get(truck_number, {'ppc': 0, 'premium': 0, 'opc': 0})
                
                # Calculate remaining unloading available (FIFO - after previous billings consumed)
                remaining_unloaded_ppc = max(0, total_unloaded['ppc'] - truck_unloading_consumed[truck_number]['ppc'])
                remaining_unloaded_premium = max(0, total_unloaded['premium'] - truck_unloading_consumed[truck_number]['premium'])
                remaining_unloaded_opc = max(0, total_unloaded['opc'] - truck_unloading_consumed[truck_number]['opc'])
                
                # Calculate how much of THIS billing is unloaded vs pending
                this_unloaded_ppc = min(billed_ppc, remaining_unloaded_ppc)
                this_unloaded_premium = min(billed_premium, remaining_unloaded_premium)
                this_unloaded_opc = min(billed_opc, remaining_unloaded_opc)
                
                # Update consumed
                truck_unloading_consumed[truck_number]['ppc'] += this_unloaded_ppc
                truck_unloading_consumed[truck_number]['premium'] += this_unloaded_premium
                truck_unloading_consumed[truck_number]['opc'] += this_unloaded_opc
                
                # Calculate pending for this specific billing
                pending_ppc = billed_ppc - this_unloaded_ppc
                pending_premium = billed_premium - this_unloaded_premium
                pending_opc = billed_opc - this_unloaded_opc
                pending_total = pending_ppc + pending_premium + pending_opc
                this_total_unloaded = this_unloaded_ppc + this_unloaded_premium + this_unloaded_opc
                
                # Check if THIS BILLING received any unloading on the selected date via FIFO
                # We need to calculate how much of today's unloading was attributed to this billing
                unloaded_on_selected_date = False
                
                # Get unloading BEFORE selected date for this truck
                cursor.execute('''
                    SELECT SUM(ppc_unloaded), SUM(premium_unloaded), SUM(opc_unloaded)
                    FROM vehicle_unloading 
                    WHERE truck_number = ? AND unloading_date < ?
                ''', (truck_number, selected_date))
                before_today = cursor.fetchone()
                before_ppc = (before_today[0] or 0) if before_today else 0
                before_premium = (before_today[1] or 0) if before_today else 0
                before_opc = (before_today[2] or 0) if before_today else 0
                
                # Calculate how much unloading before today was consumed by opening + previous billings
                # This tells us how much of today's unloading applies to this billing
                consumed_before_ppc = truck_unloading_consumed[truck_number]['ppc'] - this_unloaded_ppc
                consumed_before_premium = truck_unloading_consumed[truck_number]['premium'] - this_unloaded_premium
                consumed_before_opc = truck_unloading_consumed[truck_number]['opc'] - this_unloaded_opc
                
                # Unloading before today that was available for this billing
                available_before_ppc = max(0, before_ppc - consumed_before_ppc)
                available_before_premium = max(0, before_premium - consumed_before_premium)
                available_before_opc = max(0, before_opc - consumed_before_opc)
                
                # How much of this billing was unloaded before today
                unloaded_before_ppc = min(billed_ppc, available_before_ppc)
                unloaded_before_premium = min(billed_premium, available_before_premium)
                unloaded_before_opc = min(billed_opc, available_before_opc)
                
                # How much of this billing was unloaded TODAY
                unloaded_today_ppc = this_unloaded_ppc - unloaded_before_ppc
                unloaded_today_premium = this_unloaded_premium - unloaded_before_premium
                unloaded_today_opc = this_unloaded_opc - unloaded_before_opc
                
                if unloaded_today_ppc > 0.01 or unloaded_today_premium > 0.01 or unloaded_today_opc > 0.01:
                    unloaded_on_selected_date = True
                
                # Show previous day vehicles if:
                # 1. They have pending material, OR
                # 2. They received unloading on the selected date (via FIFO)
                if pending_total > 0.01 or unloaded_on_selected_date:
                    # For previous day pending vehicles, only show unloading that applies to THIS billing
                    prev_unloading = []
                    
                    # Only include unloading details if some unloading TODAY applies to this billing
                    # Use unloaded_today_* which is the FIFO-calculated amount for this billing
                    if unloaded_today_ppc > 0.01 or unloaded_today_premium > 0.01 or unloaded_today_opc > 0.01:
                        cursor.execute('''
                            SELECT id, truck_number, unloading_dealer, unloading_point, 
                                   ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, 
                                   notes, dealer_code, is_other_dealer, unloading_date
                            FROM vehicle_unloading 
                            WHERE truck_number = ? AND unloading_date = ?
                        ''', (truck_number, selected_date))
                        
                        # Track remaining unloading to attribute to this billing
                        remaining_to_show_ppc = unloaded_today_ppc
                        remaining_to_show_premium = unloaded_today_premium
                        remaining_to_show_opc = unloaded_today_opc
                        
                        for urow in cursor.fetchall():
                            # Calculate how much of this unloading record applies to this billing
                            record_ppc = urow[4] or 0
                            record_premium = urow[5] or 0
                            record_opc = urow[6] or 0
                            
                            # Only take up to what's remaining for this billing
                            show_ppc = min(record_ppc, remaining_to_show_ppc) if billed_ppc > 0 else 0
                            show_premium = min(record_premium, remaining_to_show_premium) if billed_premium > 0 else 0
                            show_opc = min(record_opc, remaining_to_show_opc) if billed_opc > 0 else 0
                            
                            # Update remaining
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
                    vehicles_list.append({
                        'truck_number': truck_number + '_PREV',
                        'display_truck_number': truck_number,
                        'billing_date': billing_date,
                        'invoices': [{
                            'invoice_number': 'PREVIOUS',
                            'dealer_code': '',
                            'dealer_name': dealers,
                            'ppc_quantity': billed_ppc,
                            'premium_quantity': billed_premium,
                            'opc_quantity': billed_opc,
                            'total_quantity': billed_ppc + billed_premium + billed_opc,
                            'total_value': 0,
                            'plant_depot': f'Billed {billing_date}'
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
                        'truck_also_billed_today': truck_number in trucks_today,
                        'remaining_ppc': round(pending_ppc, 2),
                        'remaining_premium': round(pending_premium, 2),
                        'remaining_opc': round(pending_opc, 2),
                        'remaining_total': round(pending_total, 2)
                    })
                    # Add to set so we don't add duplicates
                    trucks_in_list.add(truck_number)
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
        unloading_dealer = data.get('unloading_dealer')
        unloading_point = data.get('unloading_point')
        ppc_unloaded = data.get('ppc_unloaded', 0)
        premium_unloaded = data.get('premium_unloaded', 0)
        opc_unloaded = data.get('opc_unloaded', 0)
        notes = data.get('notes', '')
        is_other_dealer = 1 if data.get('is_other_dealer', False) else 0
        
        if not truck_number or not unloading_date or not unloading_dealer or not unloading_point:
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        # Calculate total unloaded quantity
        total_unloaded = (ppc_unloaded or 0) + (premium_unloaded or 0) + (opc_unloaded or 0)
        
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        # Insert new unloading record (allows multiple unloadings per vehicle)
        cursor.execute('''
            INSERT INTO vehicle_unloading 
            (truck_number, unloading_date, invoice_number, dealer_code, unloading_dealer, 
             unloading_point, ppc_unloaded, premium_unloaded, opc_unloaded, unloaded_quantity, notes, is_other_dealer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (truck_number, unloading_date, invoice_number, dealer_code, unloading_dealer,
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
    """Get list of all dealers"""
    try:
        db = SalesCollectionsDatabase(DB_PATH)
        cursor = db.conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name 
            FROM sales_data 
            WHERE dealer_code IS NOT NULL AND dealer_name IS NOT NULL
            ORDER BY dealer_name
        ''')
        
        dealers = [{'dealer_code': row[0], 'dealer_name': row[1]} for row in cursor.fetchall()]
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

@app.route('/dealer_financial_balance')
def dealer_financial_balance():
    """Dealer financial balance management page"""
    return render_template('dealer_financial_balance.html')

@app.route('/api/dealer_financial_balance', methods=['POST'])
def get_dealer_financial_balance():
    """Get dealer financial balance data for a month"""
    try:
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
        
        # Get all dealers from sales_data for this month
        cursor.execute('''
            SELECT DISTINCT dealer_code, dealer_name
            FROM sales_data
            WHERE sale_date >= ? AND sale_date < ?
            ORDER BY dealer_name
        ''', (month_start, next_month_start))
        
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
                'debit_note': 0
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
        
        # Get opening balances
        cursor.execute('''
            SELECT dealer_code, opening_balance
            FROM opening_balances
            WHERE month_year = ?
        ''', (month_year,))
        
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            if dealer_code in dealers_map:
                dealers_map[dealer_code]['opening_balance'] = row[1] or 0
        
        # Get credit notes
        cursor.execute('''
            SELECT dealer_code, credit_discount
            FROM credit_discounts
            WHERE month_year = ?
        ''', (month_year,))
        
        for row in cursor.fetchall():
            dealer_code = str(row[0])
            if dealer_code in dealers_map:
                dealers_map[dealer_code]['credit_note'] = row[1] or 0
        
        # Get debit notes
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
            
            # Upsert opening balance
            cursor.execute('''
                INSERT INTO opening_balances (dealer_code, dealer_name, opening_balance, month_year, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET opening_balance = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, opening_balance, month_year, opening_balance, dealer_name))
            
            # Upsert credit note
            cursor.execute('''
                INSERT INTO credit_discounts (dealer_code, dealer_name, credit_discount, month_year, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dealer_code, month_year) 
                DO UPDATE SET credit_discount = ?, dealer_name = ?, updated_at = CURRENT_TIMESTAMP
            ''', (dealer_code, dealer_name, credit_note, month_year, credit_note, dealer_name))
            
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

if __name__ == '__main__':
    # Ensure upload directory exists
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    
    # Run the app
    app.run(debug=True, host='0.0.0.0', port=5001)

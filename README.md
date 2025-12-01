# Sales & Collections Management System

A comprehensive web application for managing cement sales, collections, and dealer billing operations.

## Features

- **Sales File Upload**: Import sales data from Excel files
- **Collections File Upload**: Import collection data with duplicate detection
- **Unified Reports**: Daily and month-to-date sales/collections reports
- **WhatsApp Message Generator**: Generate billing messages for dealers
- **Vehicle Details Management**: Track truck unloading and billing
- **Dealer Balance Reports**: View dealer-wise material and financial balances
- **Auto-calculated Opening Balances**: Previous month's closing becomes next month's opening

## Tech Stack

- **Backend**: Python Flask
- **Database**: SQLite
- **Frontend**: HTML, CSS, JavaScript, Bootstrap 5
- **Charts**: Chart.js

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd CascadeProjects
```

2. Install dependencies:
```bash
pip install flask pandas openpyxl
```

3. Run the application:
```bash
python sales_webapp.py
```

4. Open browser and navigate to `http://localhost:5000`

## Project Structure

```
├── sales_webapp.py              # Main Flask application
├── sales_collections_database.py # Database schema and setup
├── whatsapp_message_generator.py # WhatsApp message generation
├── clear_database.py            # Database cleanup utility
├── templates/
│   ├── index.html               # Main dashboard
│   ├── vehicle_details.html     # Vehicle management
│   ├── dealer_balance.html      # Dealer balance reports
│   ├── opening_material_balance.html # Material balance
│   └── whatsapp_generator.html  # WhatsApp message interface
└── uploads/                     # Uploaded Excel files
```

## Usage

### Upload Sales Data
1. Navigate to the homepage
2. Use the "Upload Sales File" section
3. Select an Excel file with sales data
4. Click Upload

### Generate Reports
1. Select a date from the calendar
2. Click "Generate Report"
3. View unified table with today's and month-to-date data

### WhatsApp Messages
1. Go to WhatsApp Generator
2. Select billing date
3. Choose dealer
4. Enter truck numbers (optional)
5. Generate and copy message

## License

Private - All rights reserved

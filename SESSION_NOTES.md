# DSR Session Notes - Dec 4-5, 2025

## Issues Fixed

### 1. Vehicle Unloading Display - Dealer Material Balance Page
**Problem:** Vehicles were showing as "unloaded complete" even when no unloading details existed for the current month.

**Root Cause:** The unloading query used `WHERE unloading_date <= ?` which included ALL historical unloading from previous months.

**Fix:** Changed to `WHERE unloading_date >= ? AND unloading_date <= ?` to restrict unloading to current month only.

**File:** `sales_webapp.py` (line ~2190)

---

### 2. Vehicle Details Page - Historical Unloading Issue
**Problem:** All vehicles showing as "complete" in December even with no December unloading data.

**Root Cause:** Multiple unloading queries were using `unloading_date <= ?` instead of restricting to current month.

**Fixes Applied:**
- Line ~2541: `all_unloading_map` query - restricted to current month
- Line ~2901: Opening vehicle unloading query - restricted to current month  
- Line ~3070: Previous day vehicles unloading query - restricted to current month
- Line ~3158: Before-today unloading query - restricted to current month

**File:** `sales_webapp.py`

---

### 3. Previous Month Pending Vehicles Not Showing
**Problem:** Vehicles pending from November were not appearing in December's vehicle_details page.

**Root Cause:** 
1. No `pending_vehicle_unloading` entries existed for December
2. Code only queried current month's `pending_vehicle_unloading` table

**Fix:** Added logic to calculate previous month's closing pending vehicles when no current month entry exists:
- Calculate from November's `pending_vehicle_unloading` + November billing - November unloading
- Also check vehicles billed in November that weren't in pending table
- Use calculated `opening_balance_map` instead of re-querying the table

**File:** `sales_webapp.py` (lines ~2494-2628, ~3011-3022)

---

## Key Database Tables

| Table | Purpose |
|-------|---------|
| `sales_data` | Vehicle billing records |
| `vehicle_unloading` | Unloading records by truck |
| `pending_vehicle_unloading` | Opening balance for pending vehicles per month |
| `other_dealers_billing` | Billing to other dealers |
| `opening_balances` | Financial opening balances |
| `credit_discounts` | Credit notes |
| `debit_notes` | Debit notes |

---

## Key Formulas

### Vehicle Pending Calculation (FIFO)
```
Opening Balance = Previous Month Closing
Closing = Opening + Billed - Unloaded
Pending = Billed - Unloaded (current month only)
```

### Financial Balance Calculation
```
Closing = Opening + Sales - Collections - Credit Notes + Debit Notes
```

---

## Files Modified
- `sales_webapp.py` - Main Flask application with all API endpoints

## Deployment
- **Local:** `/Users/akhiltayal/CascadeProjects/DSR/`
- **AWS:** `ec2-user@3.25.160.229:/var/www/dsr/`
- **Deploy Command:** `git push origin main && ssh -i ~/Downloads/dsr-key.pem ec2-user@3.25.160.229 "cd /var/www/dsr && git pull origin main && sudo systemctl restart dsr"`

# DSR Vehicle Pending Quantity Fix - Session Summary
**Date:** January 4, 2026

## Issues Fixed

### 1. Incorrect Pending Quantities on Vehicle Details Page
**Problem:** HR38AB5491 showing 12 MT instead of 7 MT remaining
**Root Cause:** FIFO calculation was using `last_billing_date` as unloading end date for all vehicles, even when there was no later billing
**Fix:** Modified FIFO logic in `get_consolidated_vehicles` to:
- Check if there are any billings after `last_billing_date`
- If no later billing exists, use `selected_date` as unloading end date
- If later billing exists, use `last_billing_date` as unloading end date
**Location:** `sales_webapp.py` lines 3465-3488

### 2. HR38AB3916 Showing Incorrect Remaining (430 bags instead of 240 bags)
**Problem:** Vehicle showing 430 bags remaining instead of actual 240 bags
**Root Cause:** `daily_vehicle_pending` table had stale/incorrect data
**Fix:** Ran `build_daily_vehicle_map.py` to rebuild the table with correct FIFO calculations
**Result:** Now correctly shows 240 bags (12 MT)

### 3. Dealer Balance Not Matching Pending Vehicles Total
**Problem:** 
- Jan 2: Working correctly
- Jan 3: Dealer Balance = 3,785 bags, Pending Vehicles = 6,827 bags (mismatch!)
**Root Cause:** `daily_vehicle_pending` table had incorrect data for Jan 3
**Fix:** 
1. Deleted Jan 3 data from `daily_vehicle_pending`
2. Re-ran `build_daily_vehicle_map.py` to rebuild with correct calculations
3. Verified: Jan 3 Closing = 3,785 bags (matches dealer balance)

### 4. Incorrect Logic for Filtering Pending Vehicles
**Problem:** Added filter to exclude same-day billed and unloaded vehicles, but this was wrong
**Root Cause:** Misunderstanding of what "pending" means - it's total pending from all billings, not just from last billing date
**Fix:** Removed the incorrect same-day filter logic
**Current Logic:** Show all vehicles with `pending > 0` from `daily_vehicle_pending` table
**Location:** `sales_webapp.py` lines 2563-2565

### 5. Build Script Not Running Automatically
**Problem:** `build_daily_vehicle_map.py` was not running automatically, causing stale data
**Initial Approach:** Created systemd timer to run daily at 11:59 PM
**User Requirement:** Script should run when balance summary button is clicked, not on a timer
**Final Solution:** 
- Removed systemd timer
- Added automatic execution at the start of `get_dealer_balance` endpoint
- Script now runs every time balance summary is opened
**Location:** `sales_webapp.py` lines 960-970

## Key Learnings

### Daily Vehicle Pending Table
- `daily_vehicle_pending` stores **end-of-day pending** for each vehicle for each date
- Calculation: `Opening + Today's Billing - Today's Unloading = Closing Pending`
- This is a simple running balance, NOT per-billing-date FIFO
- The table is authoritative for dealer balance calculations

### FIFO Logic on Vehicle Details Page
- Vehicle details page shows separate cards for different billing dates
- Each card attempts to calculate FIFO attribution of unloading
- This is for display purposes only - the total pending should match `daily_vehicle_pending`
- Unloading attribution must be limited to the billing period for each card

### Dealer Balance Calculation
- Should equal sum of all pending vehicles from `daily_vehicle_pending`
- No additional filtering needed beyond `pending > 0`
- The `build_daily_vehicle_map.py` script handles all FIFO logic

## Files Modified

1. **`sales_webapp.py`**
   - Lines 3465-3488: Fixed FIFO unloading date range logic
   - Lines 2563-2565: Simplified pending vehicle filtering
   - Lines 960-970: Added automatic execution of build script

2. **`build_daily_vehicle_map.py`**
   - No code changes, but executed multiple times to rebuild data

## Deployment Steps

1. Modified code locally
2. Committed and pushed to GitHub
3. SSH to production server (3.25.160.229)
4. Stashed local database changes
5. Pulled latest code
6. Restarted `dsr.service`
7. Synced corrected database to production

## Production Server Details

- **Host:** 3.25.160.229
- **SSH Key:** ~/Downloads/dsr-key.pem
- **Project Path:** /var/www/dsr
- **Service:** dsr.service (gunicorn)
- **Database:** /var/www/dsr/webapp_sales_collections.db
- **Log:** /var/log/dsr_build_daily.log

## Verification Commands

```bash
# Check dealer balance vs pending vehicles
sqlite3 webapp_sales_collections.db "
SELECT 'Pending Vehicles Jan 3' as metric, SUM(ppc_qty) * 20 as bags
FROM daily_vehicle_pending WHERE date = '2026-01-03';
"

# Verify database integrity
sqlite3 webapp_sales_collections.db "PRAGMA integrity_check;"

# Check service status
ssh -i ~/Downloads/dsr-key.pem ec2-user@3.25.160.229 "sudo systemctl status dsr"

# View build script logs
ssh -i ~/Downloads/dsr-key.pem ec2-user@3.25.160.229 "cat /var/log/dsr_build_daily.log"
```

## Current Status

✅ All pending quantity issues resolved
✅ Dealer balance matches pending vehicles total
✅ Build script runs automatically on balance summary click
✅ Database synced and verified
✅ Production deployment successful

## Next Steps

- Monitor balance summary page for any discrepancies
- If issues arise, check `/var/log/dsr_build_daily.log` for build script execution
- Database is now synced locally for backup

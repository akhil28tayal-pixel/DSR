# Daily Vehicle Pending Map Implementation

## Overview
Implemented a daily pending vehicle map that maintains accurate running balances by processing transactions day-by-day, eliminating complex month-switching auto-save calculations.

## Database Schema

### New Table: `daily_vehicle_pending`
```sql
CREATE TABLE daily_vehicle_pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    vehicle_number TEXT NOT NULL,
    ppc_qty REAL DEFAULT 0,
    premium_qty REAL DEFAULT 0,
    opc_qty REAL DEFAULT 0,
    dealer_code TEXT,
    last_billing_date TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, vehicle_number)
);

CREATE INDEX idx_daily_vehicle_pending_date ON daily_vehicle_pending(date);
CREATE INDEX idx_daily_vehicle_pending_vehicle ON daily_vehicle_pending(vehicle_number);
```

## Implementation

### 1. Build Script: `build_daily_vehicle_map.py`
- Initializes with November 1, 2025 opening balances (20 vehicles)
- Processes all transactions day-by-day from Nov 1 to current date
- For each date:
  - Starts with previous day's balances
  - Adds today's billing (sales_data + other_dealers_billing)
  - Subtracts today's unloading
  - Saves vehicles with pending > 0

### 2. Results
- **Nov 30, 2025:** 180 vehicles, 1,625.25 MT (32,505 bags)
- **Dec 31, 2025:** 243 vehicles, 2,274.1 MT (45,482 bags)
- **Jan 1, 2026:** 244 vehicles, 2,338.6 MT (46,772 bags)

Verification: Jan 1 = Dec 31 + Jan 1 billing = 2,274.1 + 64.5 = 2,338.6 MT ✓

### 3. Specific Vehicles Verified
- **DL01MA6784:** 12.0 MT on both Dec 31 and Jan 1 (no Jan 1 transactions) ✓
- **HR38AB7994:** 13.95 MT on both Dec 31 and Jan 1 (1.95 PPC + 12.0 Premium) ✓

## Next Steps

### TODO: Update Vehicle Display Endpoint
The `get_vehicles_for_date` endpoint needs to be updated to:
1. Query `daily_vehicle_pending` for the selected date
2. Show all vehicles with pending balances
3. Include vehicles billed on the selected date
4. Display unloading details as before

### TODO: Auto-Update on New Transactions
When new billing or unloading is added:
1. Update the daily map for that date
2. Recalculate all subsequent dates (or use incremental update)

### TODO: Remove Old Auto-Save Logic
Once the daily map is integrated:
- Remove vehicle auto-save from `get_vehicles_for_date`
- Keep dealer auto-save (separate concern)
- Clean up `pending_vehicle_unloading` table usage

## Benefits
1. **Accurate:** Day-by-day processing eliminates calculation errors
2. **Fast:** Direct query instead of complex joins and calculations
3. **Consistent:** Same data regardless of when viewed
4. **Debuggable:** Can inspect any date's balances
5. **Scalable:** Indexed for fast queries

## Files Modified
- `webapp_sales_collections.db` - Added `daily_vehicle_pending` table
- `build_daily_vehicle_map.py` - Script to build/rebuild the map
- `sales_webapp.py` - (Pending) Update vehicle display endpoint

## Deployment Notes
1. Run `build_daily_vehicle_map.py` on production to initialize
2. Deploy updated `sales_webapp.py` with new endpoint logic
3. Test vehicle display for various dates
4. Monitor for any discrepancies

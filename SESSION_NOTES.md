# Session Notes - Dec 5, 2025

## Issues Fixed

### 1. Duplicate Dealer Names in Vehicle Unloading
- **Problem:** 4 "ARORA TRADERS" entries showing in dropdown (2 with dealer code suffix, 2 without)
- **Fix:** Standardized dealer names in `sales_data` table:
  - `11018632` → `ARORA TRADERS (8632)`
  - `11039117` → `ARORA TRADERS (9117)`

### 2. Vehicle Unloading Not Consuming Previous Month Pending (HR55AL3684)
- **Problem:** Vehicle HR55AL3684 unloading on 01-12 was consuming 01-12 billing instead of November pending balance
- **Root Cause:** Code was querying `pending_vehicle_unloading` table directly for opening balance, but HR55AL3684 had no entry there. The opening balance was calculated from previous month's closing and stored in `opening_balance_map`, but that wasn't being used.
- **Fix:** Changed code at line ~2873-2883 in `sales_webapp.py` to use `opening_balance_map` instead of querying `pending_vehicle_unloading` directly

**Before:**
```python
cursor.execute('''
    SELECT ppc_qty, premium_qty, opc_qty 
    FROM pending_vehicle_unloading 
    WHERE vehicle_number = ?
''', (truck_number,))
opening_row = cursor.fetchone()
if opening_row:
    opening_balance_ppc = opening_row[0] or 0
    ...
```

**After:**
```python
if truck_number in opening_balance_map:
    opening_balance_ppc = opening_balance_map[truck_number].get('ppc', 0)
    opening_balance_premium = opening_balance_map[truck_number].get('premium', 0)
    opening_balance_opc = opening_balance_map[truck_number].get('opc', 0)
```

## Key Commands
- **Sync DB from AWS:** `scp -i ~/Downloads/dsr-key.pem ec2-user@3.25.160.229:/var/www/dsr/webapp_sales_collections.db ./webapp_sales_collections.db`
- **Push DB to AWS:** `scp -i ~/Downloads/dsr-key.pem ./webapp_sales_collections.db ec2-user@3.25.160.229:/var/www/dsr/webapp_sales_collections.db`
- **Deploy:** `git push origin main && ssh -i ~/Downloads/dsr-key.pem ec2-user@3.25.160.229 "cd /var/www/dsr && git stash && git pull origin main && sudo systemctl restart dsr"`

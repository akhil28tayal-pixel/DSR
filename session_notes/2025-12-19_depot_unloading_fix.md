# Session Notes: DEPOT Unloading Assignment Fix
**Date:** December 19, 2025

## Issue
Vehicle unloading of 25 MT from KARHANA BUILDERS (dealer_code: 11037442) was not being assigned to the DEPOT-billed vehicle card for HR58D1569 on 2025-12-18.

## Investigation

### Data State
- **Truck:** HR58D1569
- **Date:** 2025-12-18
- **DEPOT billing:** 25 MT to ISHIKA TRADERS (dealer_code: 11020993)
- **PLANT billing:** 25 MT to YADAV CEMENT STORE (dealer_code: 11018540)
- **Unloading:** 25 MT from KARHANA BUILDERS (dealer_code: 11037442)

### Root Cause
The condition at line 3285 in `sales_webapp.py` was:
```python
if plant_depot_count > 1 and card_dealer_codes and not has_opening and global_pending_ppc + global_pending_premium + global_pending_opc > 0.01:
```

The `not has_opening` condition prevented DEPOT-specific unloading calculation when there was an opening balance from the previous month (25 PPC from November 2025). This caused the code to skip the logic that assigns unloading to DEPOT cards when the unloading dealer_code doesn't match any PLANT card's dealer_codes.

## Fix Applied
Removed the `not has_opening` and `global_pending > 0.01` conditions:

**Before:**
```python
has_opening = (opening_ppc > 0 or opening_premium > 0 or opening_opc > 0)
if plant_depot_count > 1 and card_dealer_codes and not has_opening and global_pending_ppc + global_pending_premium + global_pending_opc > 0.01:
```

**After:**
```python
if plant_depot_count > 1 and card_dealer_codes:
```

## Result
- DEPOT card now shows 25.0 PPC unloaded (KARHANA BUILDERS)
- DEPOT remaining: 0.00 (fully unloaded)
- PLANT card remains at 25.0 PPC pending (no matching unloading)

## Commits
1. `f5951e1` - Add debug logging for HR58D1569 unloading issue
2. `2a1f32f` - Fix: Remove has_opening condition so DEPOT cards always get proper unloading attribution

## Key Learning
DEPOT cards should always receive unloading that doesn't match any PLANT card's dealer_codes, regardless of whether there's an opening balance from the previous month. The opening balance condition was incorrectly preventing this logic from executing.

## Files Modified
- `/Users/akhiltayal/CascadeProjects/DSR/sales_webapp.py` (lines ~3281-3285)

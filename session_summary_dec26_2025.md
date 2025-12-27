# Vehicle Unloading Display Fixes - December 26, 2025

## Session Summary

Fixed multiple issues with vehicle unloading card display on the Vehicle Unloading page, specifically for vehicles with pending quantities from previous days and unloading on the current day.

## Issues Fixed

### 1. HR58D1569 - Incorrect Unloading Assignment (4:00 PM)
**Problem:** Only 5 MT of 20 MT unloading was being assigned to the card, showing 20 MT remaining instead of 5 MT.

**Root Cause:** When extra invoices were added to show complete billing for a date (total_ppc > card_pending_ppc), the unloading assignment was still limited by card_pending_ppc instead of total_ppc.

**Solution:** 
- Added `limit_ppc` calculation based on whether extra invoices were added
- If `total_ppc > card_pending_ppc`: use `total_ppc` as limit
- Otherwise: use `card_pending_ppc` (normal FIFO)
- Updated unloading assignment loop to use `limit_ppc` instead of `card_pending_ppc`

**Result:** HR58D1569 now shows 25 MT billed, 20 MT unloaded, 5 MT remaining ✅

**Commit:** `40d9fe2` - "Fix: Use total_ppc as unloading limit when extra invoices added"

---

### 2. Old Billing Cards Showing Incorrect Remaining (4:06 PM)
**Problem:** Old billing cards (12/12, 14/12, 16/12) showing with positive remaining amounts even though FIFO calculation determined 0 pending.

**Examples:**
- HR38AB5259: card_pending=0, remaining=5.0 (should be 0)
- DL01MA3651: card_pending=0, remaining=2.5 (should be 0)

**Root Cause:** When `card_pending_ppc = 0` but extra invoices were added (making `total_ppc > 0`), the remaining calculation used `total_ppc - unloading`, giving positive remaining even though FIFO determined no pending.

**Solution:**
- Added check before remaining calculation at line 3694
- If `card_pending_ppc = 0` AND `card_pending_premium = 0` AND `card_pending_opc = 0`:
  - Set `remaining = 0` (FIFO determined no pending)
- Otherwise: Use existing logic (total_ppc or card_pending_ppc based on condition)

**Result:** Old billing cards with 0 FIFO pending now show 0 remaining ✅

**Commit:** `48f962d` - "Fix: Set remaining to 0 when card_pending_ppc is 0 (FIFO determined no pending)"

---

### 3. DEPOT Vehicles Showing as Pending When Fully Unloaded (4:09 PM)
**Problem:** DEPOT vehicles billed on 25/12 showing as pending even though fully unloaded.

**Examples:**
- DL01LA1230, DL01LAA0453, DL01LAF4202, DL01LAH5623
- Today's card: Shows pending with 0 unloaded
- Prev Day card: Shows complete with full unloading

**Root Cause:** When vehicle is billed today AND has pending from earlier dates, two cards are created (Prev Day + Today). The unloading assignment logic was assigning today's unloading to the Prev Day card, leaving today's card with 0 unloading.

**Solution:**
- Changed condition at line 3366 from `if is_billed_today or has_unloading_today`
- To: `if not is_billed_today and has_unloading_today`
- This prevented Prev Day cards from stealing unloading when vehicle is billed today

**Result:** DEPOT vehicles billed today now show correct unloading on today's card ✅

**Commit:** `7385f68` - "Fix: Don't assign today's unloading to Prev Day card when vehicle is billed today"

---

### 4. Prev Day Cards with Pending Showing 0 Unloaded (4:15 PM)
**Problem:** Vehicles with unloading on 25/12 showing as pending with 0 unloaded.

**Examples:**
- HR58C0501, HR58C8562, HR38AB3916 (Prev Day cards with pending)

**Root Cause:** Previous fix (issue #3) broke FIFO logic for vehicles with actual Prev Day pending. Two conflicting scenarios:
1. Vehicles with Prev Day pending + Today billing: Unloading should go to Prev Day (FIFO)
2. Vehicles with Prev Day (0 pending) + Today billing: Unloading should go to Today

**Solution:**
- Combined condition at line 3367:
  ```python
  if has_unloading_today and (has_any_pending or not is_billed_today)
  ```
- Assigns unloading to Prev Day card when:
  - `has_any_pending = True` (FIFO: assign to pending card first)
  - OR `not is_billed_today` (no today's card exists)
- But NOT when:
  - `has_any_pending = False` AND `is_billed_today = True` (let today's card get it)

**Result:** All vehicles with unloading on 25/12 now show correct unloading assignment ✅

**Commit:** `655f44d` - "Fix: Assign unloading to Prev Day card only if has pending OR not billed today"

---

## Key Code Changes

### File: `/Users/akhiltayal/CascadeProjects/DSR/sales_webapp.py`

1. **Lines 3377-3381:** Added `limit_ppc` calculation for unloading assignment
2. **Lines 3400-3420:** Updated unloading assignment to use `limit_ppc` instead of `card_pending_ppc`
3. **Lines 3693-3697:** Added check to set remaining=0 when FIFO determined no pending
4. **Line 3367:** Combined condition for unloading assignment to Prev Day cards

---

## Testing Results

### Verified Vehicles:
- **HR58D1569:** 25 MT billed, 20 MT unloaded, 5 MT remaining ✅
- **DL01LA1230:** 1.25 MT billed, 1.25 MT unloaded, 0 remaining ✅
- **HR58C0501:** 20 MT unloaded on Prev Day card, 0 remaining ✅
- **HR58C8562:** 20 MT unloaded on Prev Day card, 0 remaining ✅
- **HR38AB3916:** 12 MT unloaded on Prev Day card, 0 remaining ✅

### All Test Cases Passing:
- Vehicles with multiple invoices from same date show correct total and unloading
- Old billing cards with 0 pending show 0 remaining
- DEPOT vehicles billed today show unloading on today's card
- Prev Day cards with pending show correct FIFO unloading
- Remaining calculations respect FIFO pending amounts

---

## Deployment

All fixes deployed to AWS production:
```bash
ssh -i ~/Downloads/dsr-key.pem ec2-user@3.25.160.229 "cd /var/www/dsr && git pull origin main && sudo systemctl restart dsr"
```

**Production URL:** http://3.25.160.229

---

## Summary

Successfully resolved all vehicle unloading display issues by:
1. Aligning unloading assignment limits with billing display (total_ppc vs card_pending_ppc)
2. Ensuring remaining calculations respect FIFO pending amounts
3. Correctly routing unloading to appropriate cards based on pending status and billing dates
4. Maintaining FIFO logic while handling edge cases for vehicles billed on multiple dates

All changes maintain backward compatibility and don't affect other functionality.

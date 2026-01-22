# Data Quality Analysis - Customer 360

## Executive Summary

**Overall Quality Score:** 93/100 ⚠️  **Good** (needs improvement)

**Assessment Date:** 2024-01-22  
**Total Records:** 5,437 (raw) → 4,501 (deduplicated)  
**Data Age:** 371 days (oldest record from 2023-01-15)

---

## Quality Dimensions

### 1. ✅ Completeness: 99.78% - Excellent

**Findings:**
- **Email:** Only 12 missing (0.22%) → Excellent ✅
- **Phone:** 234 missing (4.30%) → Acceptable (not mandatory field)
- **Names:** < 0.1% missing → Excellent ✅

**Action Required:**
- ✅ No action needed (completeness is excellent)
- 💡 Consider validating the 12 customers without email (may be test data)

---

### 2. ⚠️  Validity: 98.62% - Excellent (but 63 invalid emails)

**Findings:**
- **Valid emails:** 5,362 (98.62%) ✅
- **Invalid format:** 63 (1.16%) ⚠️
- **NULL emails:** 12 (0.22%)

**Invalid Email Examples:**
```
john@              ← Missing domain
@gmail.com         ← Missing local part
test.email         ← Missing @ symbol
marie@@yahoo.fr    ← Double @
```

**Root Cause:**
- Manual data entry errors
- Copy-paste issues
- No validation at source

**Action Required:**
1. ✅ **Flag invalid emails** in staging layer (`is_valid_email = FALSE`)
2. ⚠️  **Alert business team** → Re-contact these 63 customers for correct email
3. 🔧 **Add validation** at data entry point (prevent future issues)

---

### 3. ⚠️  Uniqueness: 82.79% - Good (but 936 duplicates!)

**Findings:**
- **Raw records:** 5,437
- **Unique customers:** 4,501
- **Duplicates removed:** 936 (17.21%) ⚠️

**Top Duplicates:**
```
jean.dupont@gmail.com  → 3 occurrences
marie.martin@free.fr   → 3 occurrences
pierre.durand@sfr.fr   → 2 occurrences
```

**Root Cause:**
- Same customer signed up multiple times
- Different customer_id but same email
- No uniqueness constraint on email in raw layer

**Action Required:**
1. ✅ **Deduplication working** (warehouse layer has 4,501 unique)
2. 💡 **Investigate top duplicates** → Merge accounts if needed
3. 🔧 **Add UNIQUE constraint** on email in production system

**Business Impact:**
- 17% inflated customer count → Misleading metrics
- Possible multiple accounts for same person → Fragmented customer view
- **Solution:** Identity resolution (already implemented in dbt)

---

### 4. ⚠️  Date Format Consistency: Mixed formats detected

**Findings:**
```
ISO (YYYY-MM-DD)       → 62.93% (3,421 records)
European (DD/MM/YYYY)  → 32.91% (1,789 records)
US (MM-DD-YYYY)        → 3.70% (201 records)
Unknown format         → 0.48% (26 records) ⚠️
```

**Root Cause:**
- Multiple data sources with different date formats
- Manual data entry
- CSV import from Excel (auto-formatting dates)

**Action Required:**
1. ✅ **Parsing macro exists** (`parse_mixed_dates`) → Handles 3 formats
2. ⚠️  **Investigate 26 unknown formats** → May fail parsing
3. 🔧 **Standardize at source** → Require ISO format in future

**Migration Risk:** 
- **Medium** → Macro must be correctly translated to BigQuery
- Test thoroughly on all 3 formats

---

### 5. 📞 Phone Format: 78.89% inconsistent

**Findings:**
```
Contains digits (any format) → 78.89%
Local valid (0612345678)     → 14.94%
International (+33612...)    → 1.80%
Invalid                      → 0.07%
NULL                         → 4.30%
```

**Root Cause:**
- No standardization (spaces, dashes, parentheses)
- Examples: "06 12 34 56 78", "06-12-34-56-78", "+33 6 12 34 56 78"

**Action Required:**
1. ✅ **Cleaning logic exists** (remove non-digits in staging)
2. 💡 **Consider phone validation** (length, country code)
3. 🔧 **Standardize format** → Store as international format (+33...)

**Business Impact:**
- Low (phone is secondary contact method)
- May affect SMS campaigns (need clean format)

---

### 6. 🌍 Country Distribution: 90% France

**Findings:**
```
FR  → 89.98% (4,892 customers)
BE  → 5.74% (312 customers)
CH  → 2.67% (145 customers)
NULL → 1.62% (88 customers)
```

**Observations:**
- France-heavy customer base ✅ (expected)
- 88 customers without country ⚠️

**Action Required:**
- ✅ No action (distribution looks normal)
- 💡 Investigate NULL countries → May be test data

---

## Data Quality Issues Summary

| Issue | Severity | Count | % of Data | Action |
|-------|----------|-------|-----------|--------|
| **Invalid emails** | 🔴 High | 63 | 1.16% | Alert business team |
| **Duplicate customers** | 🟡 Medium | 936 | 17.21% | ✅ Deduplicated in warehouse |
| **Mixed date formats** | 🟡 Medium | 3 formats | 100% | ✅ Macro handles it |
| **Unknown date formats** | 🟡 Medium | 26 | 0.48% | Investigate & fix |
| **Inconsistent phone** | 🟢 Low | 4,289 | 78.89% | ✅ Cleaned in staging |
| **Missing phone** | 🟢 Low | 234 | 4.30% | Acceptable (optional field) |
| **Missing country** | 🟢 Low | 88 | 1.62% | Investigate test data |

---

## Migration Readiness

### ✅ Ready to Migrate

1. **Deduplication logic** → Working (936 duplicates removed)
2. **Email validation** → Flags invalid formats
3. **Date parsing** → Handles 3 formats
4. **Phone cleaning** → Removes special chars

### ⚠️  Requires Attention

1. **63 invalid emails** → Business team needs to fix at source
2. **26 unknown date formats** → May fail parsing in BigQuery
3. **Test thoroughly** → Especially date parsing macro

### 🎯 Acceptance Criteria for Migration

**Migration is successful if:**

| Metric | PostgreSQL (Before) | BigQuery (After) | Status |
|--------|---------------------|------------------|--------|
| Total raw records | 5,437 | 5,437 | Must match exactly |
| Unique customers | 4,501 | 4,501 | Must match exactly |
| Valid emails | 5,362 | 5,362 | Must match exactly |
| VIP customers | ~450 | ~450 | ±5% acceptable |
| Total revenue | €1.8M | €1.8M | ±0.1% acceptable |

**If any metric doesn't match → Migration failed, rollback and debug!**

---

## Recommendations

### Short-term (Pre-Migration)

1. 🔴 **Critical:** Validate the 26 records with unknown date formats
```sql
   SELECT customer_id, created_at
   FROM raw.csv_customers
   WHERE created_at !~ '^\d{4}-\d{2}-\d{2}'
     AND created_at !~ '^\d{2}/\d{2}/\d{4}'
     AND created_at !~ '^\d{2}-\d{2}-\d{4}';
```

2. 🟡 **Important:** Alert business about 63 invalid emails
   - Export list for customer service
   - Re-contact customers for correct email

3. 🟢 **Nice-to-have:** Investigate top duplicate emails
   - Are they legitimate duplicates or data entry errors?

### Long-term (Post-Migration)

1. **Add validation at source:**
   - Email format validation on signup form
   - Date picker (prevent manual entry)
   - Phone number formatter

2. **Add UNIQUE constraint:**
```sql
   ALTER TABLE raw.csv_customers
   ADD CONSTRAINT unique_email UNIQUE (email);
```

3. **Monitoring:**
   - Daily data quality checks
   - Alert if invalid email % > 2%
   - Alert if duplicate rate > 20%

---

## Next Steps

1. ✅ Day 5 complete → Baseline documented
2. ⏭️  Day 6: Install BigQuery Python dependencies
3. ⏭️  Day 7: Build extraction script (PostgreSQL → CSV)
4. ⏭️  Day 8: Build load script (CSV → BigQuery)
5. ⏭️  Day 9: Create BigQuery DDL with proper types
6. ⏭️  Day 10: Reconciliation (validate row counts match)

**Quality gate:** All metrics must match PostgreSQL exactly ✅


# CRM Account Health Signals — Design Document

## Status: Approved — Ready for implementation

---

## Philosophy

Don't build a composite health score (0-100 or green/yellow/red). These rot because they're opaque, hide the "why," and become visual noise. Instead, build **specific, transparent signals** — each self-explanatory, each implying a clear action.

Signals are:
- Computed from **spots/revenue data only** (objective, always current after imports)
- **Not** dependent on activity log data (essentially unused — 1 note on 1 entity total)
- **Exception-based** — no signal = clean card; signals only appear when something needs attention
- **Revenue-gated** — small accounts don't generate noise

---

## Data Profile (as of 2026-02-11)

### Entity Counts
| Type | Total | Active |
|------|-------|--------|
| Customers | 557 | 257 |
| Agencies | 108 | 68 |

### Revenue Distribution (All-Time, Excl. Trade)
| Bucket | Accounts | Revenue |
|--------|----------|---------|
| $500K+ | 7 | $5.3M |
| $100K-500K | 53 | $10.3M |
| $50K-100K | 26 | $1.8M |
| $10K-50K | 99 | $2.3M |
| $1K-10K | 108 | $441K |
| Under $1K | 54 | -$128K |

**Pareto**: 60 accounts ($100K+) drive 78% of total revenue (~$15.6M of ~$20M).

### Customer Recency
| Recency | Accounts |
|---------|----------|
| Last 30 days | 55 |
| 31-90 days | 28 |
| 91-180 days | 15 |
| 181-365 days | 28 |
| 365+ days | 221 |

221 customers (64%) haven't aired in over a year — massive lapsed pool. Signals must NOT flag all of these or the system is useless.

### Agency Recency
| Recency | Accounts |
|---------|----------|
| Last 30 days | 22 |
| 31-90 days | 5 |
| 91-180 days | 4 |
| 181-365 days | 8 |
| 365+ days | 40 |

### Year-over-Year Trend (2024 → 2025)
| Trend | Accounts | 2024 Rev | 2025 Rev |
|-------|----------|----------|----------|
| Churned (no 2025 rev) | 76 | $2.2M | $0 |
| Declining 30%+ | 16 | $916K | $187K |
| Declining 0-30% | 17 | $536K | $439K |
| Growing 0-30% | 6 | $270K | $303K |
| Growing 30%+ | 19 | $171K | $529K |

76 customers churned entirely. Only 25 grew.

### Booking Frequency (Since 2024-01-01)
| Pattern | Accounts |
|---------|----------|
| Year-round (10+ months) | 60 |
| Regular (6-9 months) | 26 |
| Seasonal (3-5 months) | 51 |
| Occasional (1-2 months) | 80 |

### Spots Data Range
- Earliest: 2020-12-28
- Latest: 2026-12-27 (forward bookings exist)
- Total spots: 1,245,646

### Key Implications
1. **Activity log is unusable as a signal source** — signals are 100% spots/revenue-derived
2. **Lapsed pool is too large for a blanket flag** — need to distinguish "was meaningful and recently stopped" from "was always small and drifted away"
3. **Revenue-weighting is essential** — losing a $200K account vs a $800 account are different events
4. **Forward bookings are a critical false-positive filter** — accounts with future spots aren't at risk
5. **Booking cadence varies wildly** — universal day thresholds don't work; need account-relative or tier-based logic

---

## Signal Definitions

### Signal 1: Revenue Declining

**Detects**: Accounts still active but spending meaningfully less.

**Logic**: Trailing 12 months revenue vs prior 12 months. Flag when drop exceeds 30%.

**Revenue gate**: Prior period $10K+. Below that, fluctuations are normal.

**False-positive filter**: If future spots (air_date > today) total at least 50% of the gap, suppress.

**Severity**:
- Prior $100K+ and declined 30%+ → **high**
- Prior $10K-100K and declined 50%+ → **medium**

**Display**: `↓ Revenue · $145K → $82K (-43%)`

**Implied action**: Retention conversation — understand why they're spending less.

---

### Signal 2: Gone Quiet

**Detects**: Accounts that recently broke their own booking pattern.

**Logic (tier-based approach)**:
- Year-round (10+ months/yr): flag at **90 days** since last spot
- Regular (6-9 months/yr): flag at **120 days**
- Seasonal (3-5 months/yr): flag at **8 months**
- Occasional (1-2 months/yr): **don't flag** — pattern is inherently irregular

**Alternative (account-relative approach)**: Compute median inter-booking interval over past 2 years, flag when current gap exceeds 2x median. Requires 3+ booking periods to establish pattern.

**Revenue gate**: Lifetime $10K+.

**False-positive filter**: Suppress if future spots are booked.

**Display**: `Quiet 94d · typically books monthly`

**Implied action**: Proactive check-in before the relationship goes cold.

---

### Signal 3: Churned (Year-over-Year)

**Detects**: Accounts with meaningful prior-year revenue and zero current-year activity.

**Logic**: $10K+ in prior calendar year, $0 in current year including future bookings.

**Distinct from Gone Quiet**: Gone Quiet catches accounts *in the process* of going silent. Churned catches accounts that have fully lapsed. Different urgency and action.

**Severity**:
- Prior $100K+ → **high**
- Prior $10K-100K → **medium**

**Display**: `No 2026 activity · $87K in 2025`

**Implied action**: Win-back outreach. Large accounts may need manager involvement.

---

### Signal 4: Growing

**Detects**: Accounts with positive revenue momentum.

**Logic**: Trailing 12 months vs prior 12 months, up 30%+, current period $10K+.

**Why a positive signal**: Growing accounts need attention too — nurture, upsell, protect from competitors. AEs should know which accounts are hot, not just which are at risk.

**Tiers**:
- Current $100K+ and growing 30%+ → "top mover"
- Current $10K-100K and growing 30%+ → "emerging"

**Display**: `↑ Growing · $42K → $78K (+86%)`

**Implied action**: Nurture. Explore expansion, protect the momentum.

---

### Signal 5: New Account

**Detects**: Recently acquired accounts in their critical first year.

**Logic**: First-ever spot within last 12 months, $5K+ in revenue.

**Rationale**: New accounts are fragile. First year determines retention trajectory. Industry data shows first 90 days are make-or-break.

**Display**: `New · first booked Oct 2025 · $23K`

**Implied action**: Strong onboarding cadence. Regular check-ins. Don't let them fall through cracks.

---

## What We Explicitly Don't Signal

- **Long-lapsed accounts (365+ days)**: 221 accounts — too many to flag. These are historical, not "needs attention." They belong in the existing inactive filter, not the signal system.
- **Small accounts under $10K lifetime**: Fluctuations at this level are normal and not worth CRM energy.
- **Trade-only revenue**: Already excluded from all metrics.

---

## Thresholds Summary

| Signal | Revenue Gate | Trigger | Suppress If |
|--------|-------------|---------|-------------|
| Revenue Declining | Prior 12mo $10K+ | Down 30%+ | Future bookings cover 50%+ gap |
| Gone Quiet | Lifetime $10K+ | Exceeded tier gap threshold | Future spots exist |
| Churned | Prior year $10K+ | $0 current year + future | Any current/future spots |
| Growing | Current 12mo $10K+ | Up 30%+ | — (always show) |
| New Account | Current $5K+ | First spot within 12mo | — (always show) |

---

## UI Integration Plan

### On Entity Cards (Grid View)
- Small badge, only when signal present. No signal = clean card.
- Max 1 signal shown (highest priority): Churned > Declining > Gone Quiet > New > Growing
- Badge is the signal text itself (e.g., `↓ Revenue -43%`), not a colored dot

### In Detail Modal
- Signals section near top, expanding with specifics:
  - "Revenue down 43% year-over-year: $145K trailing 12mo vs $254K prior 12mo"
  - Transparent, no mystery about methodology

### "Needs Attention" View
- Filter preset or toggle that shows only accounts with active signals
- Sortable by signal type — AE can work through dormant accounts, then declining, etc.
- This is the daily workflow tool: open address book → Needs Attention → work the list

---

## Implementation Shape

### Data Layer
- Extend `entity_metrics` table (or add sibling `entity_signals` table) computed during `refresh_entity_metrics()`
- Columns: `signal_type`, `signal_severity`, `signal_detail` (JSON), `computed_at`
- Refreshed on every spots import — signals are always current

### Queries Needed
- Trailing 12mo vs prior 12mo revenue per entity (for Declining/Growing)
- Booking frequency classification per entity (for Gone Quiet tier)
- Days since last spot + future spot check (for Gone Quiet suppression)
- Prior year vs current year totals (for Churned)
- First-ever spot date per entity (for New Account)

### Performance
- All computed at import time, not query time
- Address book list query joins signals like it joins metrics — no runtime cost
- Expected signal count: ~30-80 active signals (manageable, not overwhelming)

---

## Decisions (Resolved 2026-02-11)

1. **Rolling 12 months** for comparison periods — smoother, no January weirdness
2. **Tier-based** Gone Quiet thresholds (90/120/240 days by booking frequency) — simpler to implement, explain, and debug
3. **$10K revenue gate** to start — can adjust after seeing results in practice

---

## Technical References

- Entity metrics cache: `sql/migrations/013_entity_metrics_cache.sql`
- Refresh function: `src/web/routes/address_book.py` lines 24-49
- Import trigger: `src/services/broadcast_month_import_service.py`
- Existing stale detection: `src/web/routes/stale_customers.py`
- Address book template: `src/web/templates/address_book.html`
- Spots schema: `src/database/schema.py` lines 99-163

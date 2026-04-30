# Workbook AE Drift Tracker (`tblKnownAEs`)

**Audience:** the client-side LLM (Excel / Power Query) building the
`Revenue Master.xlsx` workbook.
**Date:** 2026-04-21
**Status:** Proposed. Purely workbook-side — **zero server changes required.**

Companion to `tblKnownRows` (row-level tuple acknowledgment). Catches
AE-level identity changes that the row tracker misses, or flags with
300 row-level noise events when one person renamed.

---

## 1. Why this exists

`tblKnownRows` fires when a new `(customer, market, revenue_class, ae1,
agency_flag, sector)` tuple appears. If `ae1` changes, every tuple
that AE touches gets a fresh `row_hash` → Kurt sees hundreds of `New
Rows` entries instead of "one person was renamed, confirm and move on."

`tblKnownAEs` fires one level up: on the AE identity itself. It's a
**coarse pre-filter** that lets Kurt confirm the AE change in one
keystroke, at which point the 300 row-level flags can be batch-
acknowledged or ignored.

It also surfaces AEs that the server's casing-drift normalization has
quietly collapsed, so Kurt can decide whether the drift reflects a
genuine new person or a DB hygiene issue the importer should catch.

## 2. What it catches

Three scenarios, decreasing frequency:

| Scenario | Example | Row-level symptom | AE-level symptom |
|---|---|---|---|
| **New AE** | Kurt hires "Alex Kim" | 50+ new `row_hash` entries on `New Rows` | One new `ae_key = alex kim` on `New AEs`. Kurt acknowledges once. |
| **Rename** | DB cleanup: `Riley van Patten` → `Riley Van Patten` (all spots) | All Riley tuples re-hash; 300+ `New Rows` entries | `ae_key = riley van patten` unchanged — **nothing fires** because the key is already acknowledged. Display may update silently. |
| **Partial-cleanup drift** | Half of Riley's spots get fixed, half don't | Server MIN picks one canonical casing; drift invisible at row level unless other identity fields also differ | Tracker compares `ae_key` against `tblKnownAEs`; if the canonical display the server returns doesn't match the acknowledged display, flag as "Casing drift — probably harmless, DB cleanup happened" |

The rename case is where this feature earns its keep. The server's
casing-normalization (committed 2026-04-21) already handles the common
per-spot casing drift, but a genuine rename (`Jane Smith` → `Jane
Smith-Lopez` after marriage) produces a new `ae_key` and a dead old
one. The tracker surfaces both.

## 3. Key design decisions

### 3.1 Key on normalized form, not display

```
ae_key = Text.Lower(Text.Trim(Text.From(ae1)), "en-US")
```

Same algorithm as the `row_hash` applies to the `ae1` field. Stable
across server-side canonical-display changes and DB cleanups. Display
casing is **derived from the latest refresh** — never stored in
`tblKnownAEs`.

If you key on display, every DB cleanup becomes a re-acknowledge pass
for Kurt. Don't do that.

### 3.2 Union source across both endpoints

Build the AE universe from **sheet-export's `ae1`** union
**planning-export's `ae1`**:

- Sheet-export covers AEs with booking activity (all-history).
- Planning-export covers AEs with budget or forecast rows, even with
  no bookings (placeholder entities like `ZTBD`, new hires in
  planning but not yet in spots).

Either source alone misses legitimate AEs.

### 3.3 `tblKnownAEs` schema

Hidden sheet, single table:

| Column | Type | Notes |
|---|---|---|
| `ae_key` | Text | Normalized form. Primary key. |
| `acknowledged_display` | Text | The display casing at time of ack. Used only for drift-detection reasons on `New AEs`. |
| `acknowledged_date` | Date | When Kurt added it. Optional but useful for audit. |
| `notes` | Text | Freeform. Optional. |

Kurt acknowledges by copying a row from `New AEs` → `tblKnownAEs`.
Same manual workflow as `tblKnownRows` for v1. A "Ack Selected"
Office Script can follow in v2 if it gets annoying.

### 3.4 `New AEs` tab schema

Shown to Kurt. Read-only output of the PQ anti-join.

| Column | Purpose |
|---|---|
| `ae_key` | The lookup key. Kurt copies this to `tblKnownAEs` to acknowledge. |
| `ae1_display` | Current server casing. What Kurt sees on the Data tab. |
| `reason` | Auto-computed: `"New AE"`, `"Casing drift from acknowledged '<old>'"`, or `"Rename sibling — possible match for '<old>'"`. |
| `first_seen_month` | Earliest `broadcast_month` across both endpoints. Helps Kurt gauge how long this has been around. |
| `latest_month` | Latest `broadcast_month`. Helps spot stale vs active. |
| `total_gross` | Lifetime `SUM(gross_rate)` from sheet-export. `null` for planning-only AEs with no activity. |
| `source` | `"sheet-export"` / `"planning-export"` / `"both"`. Flags planning-only placeholders (`ZTBD`) vs data-only AEs (historical Riley). |

Sort by `total_gross` descending — biggest unacknowledged AEs surface first.

## 4. `reason` auto-computation

Applied row-by-row on the `New AEs` output:

1. **`"Casing drift from acknowledged '<ack_display>'"`** — if
   `ae_key` is found in `tblKnownAEs` but the current `ae1_display`
   doesn't match `acknowledged_display`. This is almost always
   benign (a DB cleanup happened, server's canonical-casing MIN()
   picked differently). Kurt can either re-acknowledge the new
   display (update the row in `tblKnownAEs`) or ignore.

2. **`"Rename sibling — possible match for '<old>'"`** — if `ae_key`
   is new AND there's an acknowledged AE whose display differs only
   in known name-mutation patterns (married name suffix, space
   variations). Heuristic, not load-bearing — false positives fine,
   false negatives fine. One simple check: fuzzy match on the first
   token of the display name. E.g., new `jane smith-lopez` flagged
   as possible rename of acknowledged `jane smith`.

3. **`"New AE"`** — everything else. Default reason.

Kurt reads these and decides. The tracker doesn't auto-acknowledge
anything; it just saves Kurt the "wait, is this actually a new
person?" cognitive pass.

## 5. PQ implementation sketch

```
qAEs_raw =
    let
        from_sheet = Table.SelectColumns(qRevenueActuals,
            {"ae1", "broadcast_month", "gross_rate"}),
        from_sheet_typed = Table.AddColumn(from_sheet, "source",
            each "sheet-export"),
        from_plan = Table.SelectColumns(qPlanningExport,
            {"ae1", "broadcast_month"}),
        from_plan_null_gross = Table.AddColumn(
            Table.AddColumn(from_plan, "gross_rate", each null),
            "source", each "planning-export"),
        combined = Table.Combine({from_sheet_typed, from_plan_null_gross}),
        with_key = Table.AddColumn(combined, "ae_key",
            each Text.Lower(Text.Trim(Text.From([ae1])), "en-US"))
    in
        with_key

qAEs_grouped =
    let
        source = qAEs_raw,
        grouped = Table.Group(source, {"ae_key"}, {
            {"ae1_display", each List.First([ae1]), type text},
            {"first_seen_month", each List.Min([broadcast_month])},
            {"latest_month", each List.Max([broadcast_month])},
            {"total_gross", each List.Sum([gross_rate])},
            {"sources", each Text.Combine(List.Distinct([source]), ",")}
        }),
        with_source_label = Table.TransformColumns(grouped, {{"sources", each
            if _ = "sheet-export,planning-export" or _ = "planning-export,sheet-export"
            then "both" else _}})
    in
        with_source_label

qNewAEs =
    let
        all_aes = qAEs_grouped,
        known = Excel.CurrentWorkbook(){[Name="tblKnownAEs"]}[Content],
        -- Anti-join to filter out acknowledged AEs (match on ae_key,
        -- but preserve drift case via separate logic).
        with_ack = Table.NestedJoin(all_aes, {"ae_key"}, known, {"ae_key"},
            "ack_match", JoinKind.LeftOuter),
        with_reason = Table.AddColumn(with_ack, "reason", each
            if Table.RowCount([ack_match]) = 0 then
                "New AE"    -- fuzzy-rename heuristic can refine this
            else if [ack_match]{0}[acknowledged_display] <> [ae1_display] then
                "Casing drift from acknowledged '" & [ack_match]{0}[acknowledged_display] & "'"
            else
                null),  -- fully acknowledged, drop from output
        filtered = Table.SelectRows(with_reason, each [reason] <> null),
        final = Table.RemoveColumns(filtered, {"ack_match"})
    in
        Table.Sort(final, {{"total_gross", Order.Descending}})
```

The fuzzy rename heuristic (first-token match) can be layered on as a
second pass that adds a `rename_candidate` column, then the `reason`
computation prefers the rename case over the plain "New AE". Simple
to bolt on after the base tracker ships.

## 6. Acknowledgment workflow

Same muscle memory as `tblKnownRows`:

1. Refresh.
2. Open `New AEs` tab, scan top-to-bottom (sorted by total_gross).
3. For each row:
   - **"New AE"** → copy `ae_key` + current `ae1_display` to
     `tblKnownAEs`. Fill in `acknowledged_date = TODAY()`.
   - **"Casing drift from acknowledged '<old>'"** → either
     (a) update the `acknowledged_display` in the matching
     `tblKnownAEs` row (explicit re-ack) or (b) ignore and live with
     the stale display (the `reason` will keep firing until you do
     one or the other).
   - **"Rename sibling — possible match for '<old>'"** → Kurt's
     judgment. If it's really a rename, update the old
     `tblKnownAEs` row's `ae_key` + display; if it's a coincidence,
     treat as "New AE" and add a new `tblKnownAEs` row.
4. Refresh again. `New AEs` should now be empty (or close to it).

## 7. What this doesn't do (scope boundaries)

- **Doesn't auto-acknowledge anything.** All entries require Kurt
  action. The tracker is informational.
- **Doesn't affect `tblKnownRows`.** A fresh row_hash from an AE
  rename still surfaces on `New Rows`. If that's annoying after
  v1, a v2 enhancement could let Kurt select "AE rename — auto-
  acknowledge all row_hashes touching this AE" from the `New AEs`
  tab. Not v1.
- **Doesn't prevent the drift.** The server's casing-
  normalization (committed 2026-04-21) already collapses the common
  per-spot casing drift. The tracker catches what the server can't
  see — genuine renames, placeholder-to-active transitions,
  display-vs-ack-casing mismatches after DB cleanups.
- **Doesn't need a server endpoint.** Purely workbook-side. See §8.

## 8. No server changes needed

The workbook already pulls `ae1` from both endpoints. `ae_key`
normalization is three Text functions in PQ. Group, anti-join,
render. That's the whole feature.

A server-side `/api/revenue/ae-directory` endpoint pre-computing
`{ae_key, ae1_display, first_seen, last_seen, total_gross}` per AE
was considered and **rejected for v1** — the workbook aggregates
5,344 rows in PQ in milliseconds, and the endpoint adds one more
auth surface and one more doc. Revisit if perf ever bites (unlikely).

## 9. Testing / validation (informal)

After implementing, verify three cases:

1. **Baseline.** First refresh after adding `tblKnownAEs` (empty) →
   `New AEs` shows every AE, sorted by total_gross. Kurt bulk-
   acknowledges the known-good ones.
2. **Stable refresh.** Immediately refresh again without DB
   changes → `New AEs` is empty.
3. **Synthetic rename.** Temporarily rename one AE in `tblKnownAEs`
   (change `acknowledged_display`, leave `ae_key` alone) → next
   refresh flags "Casing drift from acknowledged '<renamed>'".
   Revert the manual edit.

No automated test harness — this is workbook-side behavior, tested
by Kurt exercising the UI.

## 10. Related docs

- `docs/sheet-export-client-contract.md` — row-level contract (`row_hash`, `tblKnownRows`).
- `docs/planning-export-client-contract.md` — planning rollup used as the second AE source.
- `docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md` §6 — the Power Query architecture the tracker plugs into.

# The Might 29

This document describes each column in the Excel data structure and their positions.

## Quick Reference Grid

| Pos | Field Name | Display Name | Brief Description |
|-----|------------|--------------|-------------------|
| 0 | `bill_code` | Bill Code | Agency:customer identifier with colon separators |
| 1 | `air_date` | Start Date | Calendar date when spot aired |
| 2 | `end_date` | End Date | Calendar date when spot aired (same as air_date) |
| 3 | `day_of_week` | Day(s) | Full day name (Monday, Tuesday, etc.) |
| 4 | `time_in` | Time In | Programming block start time (12-hour format) |
| 5 | `time_out` | Time Out | Programming block end time (12-hour format) |
| 6 | `length_seconds` | Length | Spot duration in MM:SS format |
| 7 | `media` | Media/Name/Program | Spot name for master control |
| 8 | `comments` | Comments | Actual air time when spot broadcast |
| 9 | `language_code` | Language | Single-letter language codes (C, V, H, J, E, T, P) |
| 10 | `format` | Format | Program name where spot actually aired |
| 11 | `sequence_number` | Units-Spot count | Unknown purpose |
| 12 | `line_number` | Line | Unknown purpose |
| 13 | `spot_type` | Type | Spot category (BB, Com, BNS, SVC, PRD, PKG, CRD, PRG) |
| 14 | `estimate` | Agency/Episode# or cut number | Estimate or contract numbers |
| 15 | `gross_rate` | Unit rate Gross | Rate before deductions |
| 16 | `make_good` | Make Good | Notes for compensatory re-airings |
| 17 | `spot_value` | Spot Value | Same as gross_rate |
| 18 | `broadcast_month` | Month | TV industry month format (Nov-24) |
| 19 | `broker_fees` | Broker Fees | Broker commission amounts |
| 20 | `priority` | Sales/rep com: revenue sharing | Priority/revenue sharing (unclear) |
| 21 | `station_net` | Station Net | Final revenue after all deductions |
| 22 | `sales_person` | Sales Person | Sales rep name or ID |
| 23 | `revenue_type` | Revenue Type | Business model category |
| 24 | `billing_type` | Billing Type | Calendar or Broadcast month system |
| 25 | `agency_flag` | Agency? | Agency or Non-agency |
| 26 | `affidavit_flag` | Affidavit? | Repurposed field (original use: affidavit required) |
| 27 | `contract` | Notarize? | Contract number (original use: notarization flag) |
| 28 | `market_name` | Market | Geographic market code (SEA, SFO, LAX, etc.) |

## Column Definitions

### Column 0: `bill_code` (Bill Code)
**Purpose:** Identifies the billing entity structure for the record  
**Format:** Uses colon (`:`) as separator between agency and customer  
**Structure:** 
- `customer` - Direct customer with no agency
- `agency:customer` - Single agency with customer
- `agency1:agency2:customer` - Multiple agencies (what follows final colon is always customer name)  
**Examples:** 
- `ACME_Corp` (direct customer)
- `MediaBuyers:ACME_Corp` (single agency)
- `HoldingCo:MediaBuyers:ACME_Corp` (multiple agencies)

### Column 1: `air_date` (Start Date)
**Purpose:** The calendar date when the spot aired  
**Format:** Date value representing the actual broadcast date

### Column 2: `end_date` (End Date)
**Purpose:** The calendar date when the spot aired (same as air_date)  
**Format:** Date value representing the actual broadcast date  
**Note:** Typically equal to air_date since each record represents a single spot airing

### Column 3: `day_of_week` (Day(s))
**Purpose:** The day of the week when the spot aired  
**Format:** Fully spelled out day name (e.g., "Monday", "Tuesday", "Saturday")  
**Usage:** Helpful for categorizing spots as weekday vs. weekend for analysis and pricing

### Column 4: `time_in` (Time In)
**Purpose:** Defines the start of the programming block/time window where the spot can air  
**Format:** 12-hour time format with seconds (e.g., "7:00:00 AM", "2:30:00 PM")  
**Usage:** Customer programming preferences (e.g., Hindi news block, children's Mandarin block, Japanese language programming)

### Column 5: `time_out` (Time Out)
**Purpose:** Defines the end of the programming block/time window where the spot can air  
**Format:** 12-hour time format with seconds (e.g., "7:00:00 AM", "2:30:00 PM")  
**Usage:** Combined with time_in to specify target programming blocks  
**Special case:** When time_in/time_out spans most/all of the day, indicates a "run of schedule" (ROS) or rotator spot, commonly used for bonus spots

### Column 6: `length_seconds` (Length)
**Purpose:** Duration of the actual spot/ad in seconds  
**Format:** Time duration format (MM:SS or H:MM:SS) like "0:00:30", "0:02:00"  
**Common lengths:**
- 30 seconds (most common - standard commercial)
- 15 seconds (short commercial)  
- 2 minutes (long-form ad/infomercial segment)
- 60 seconds, 45 seconds (standard variations)
**Special cases:** Longer durations (6+ hours) may represent infomercials or programming content

### Column 7: `media` (Media/Name/Program)
**Purpose:** Spot name identifier for master control operations  
**Format:** Text string representing the spot/ad name  
**Note:** This spot name may differ from how it's stored in cloud systems or playout systems, but provides useful reference for master control staff  
**Examples:** "Gary Sadlon", "Gary Sadlon 2nd Issue" (as seen in data)

### Column 8: `comments` (Comments)
**Purpose:** Captures the actual air time when the commercial spot was broadcast  
**Format:** Free-form text field  
**Primary use:** Records the precise time the spot actually aired (vs. the programming block window defined by time_in/time_out)

### Column 9: `language_code` (Language)
**Purpose:** Identifies the language of the spot content  
**Format:** Single-letter codes for different languages  
**Common codes:**
- `C` - Chinese (Mandarin/Cantonese)
- `V` - Vietnamese  
- `H` - Hindi (also used for Hmong)
- `J` - Japanese
- `E` - English
- `T` - Tagalog
- `P` - Punjabi
**Usage:** Helps categorize content by target language audience

### Column 10: `format` (Format)
**Purpose:** Identifies the program that was broadcasting when the spot aired  
**Format:** Text string representing the TV program/show name  
**Usage:** Shows which specific program the spot actually aired during (vs. the intended programming block from time_in/time_out)

### Column 11: `sequence_number` (Units-Spot count)
**Purpose:** Unknown at this time  
**Format:** Numeric value  
**Note:** Field definition and usage patterns need further investigation

### Column 12: `line_number` (Line)
**Purpose:** Unknown at this time  
**Format:** Numeric value (e.g., 1, 2, 3...)  
**Note:** Field definition and usage patterns need further investigation

### Column 13: `spot_type` (Type)
**Purpose:** Categorizes the type of spot/content being aired  
**Format:** Three-letter abbreviation codes  
**Common types:**
- `BB` - Billboard
- `Com` - Commercial (paid advertising)
- `BNS` - Bonus (additional spots provided to client)
- `SVC` - Services
- `PRD` - Production
- `PKG` - Package
- `CRD` - Credit
- `PRG` - Paid Programming (infomercials/long-form content)
**Usage:** Determines billing treatment and scheduling priorities

### Column 14: `estimate` (Agency/Episode# or cut number)
**Purpose:** Contains estimate or contract numbers for business tracking  
**Format:** Alphanumeric identifier  
**Usage:** Important reference numbers for linking spots to estimates or contracts  
**Note:** Uncertain whether these are specifically estimate numbers or contract numbers, but they are significant for business operations

### Column 15: `gross_rate` (Unit rate Gross)
**Purpose:** The gross rate charged per spot before deductions  
**Format:** Currency amount (typically dollars)  
**Usage:** Base rate before agency commissions or broker fees are subtracted  
**Note:** Represents the published or contracted rate before any percentage-based deductions

### Column 16: `make_good` (Make Good)
**Purpose:** Indicates a spot that's being re-aired due to a previous issue  
**Format:** Text field (varchar) - contains notes about the make good situation  
**Usage:** Empty for normal spots, contains descriptive notes when spot is a compensatory re-airing  
**Note:** These spots are typically provided at no additional charge to make up for previous scheduling or technical issues

### Column 17: `spot_value` (Spot Value)
**Purpose:** The value of the spot (equal to gross_rate)  
**Format:** Currency amount (typically dollars)  
**Note:** This field contains the same value as gross_rate

### Column 18: `broadcast_month` (Month)
**Purpose:** Converts air_date to broadcast month using TV industry standards  
**Format:** Month abbreviation and 2-digit year (e.g., "Nov-24")  
**Calculation:** Uses week-based boundaries (Sunday to Saturday) rather than calendar month boundaries  
**Formula logic:** Takes the air_date and shifts it to align with broadcast industry month definitions, where months typically run from Sunday to Saturday rather than calendar dates  
**Usage:** Essential for TV industry financial reporting and ratings periods

### Column 19: `broker_fees` (Broker Fees)
**Purpose:** The fees charged by a broker for their services  
**Format:** Currency amount (typically dollars)  
**Usage:** Represents broker commissions or fees that may be deducted from station revenue

### Column 20: `priority` (Sales/rep com: revenue sharing)
**Purpose:** Possibly a priority order for spot precedence decisions  
**Format:** Numeric value  
**Hypothesis:** May determine which spot takes precedence during collision situations (when multiple spots compete for the same time slot)  
**Note:** Also related to sales/rep commission and revenue sharing, but exact usage needs further investigation

### Column 21: `station_net` (Station Net)
**Purpose:** The net amount the station receives after all deductions  
**Format:** Currency amount (typically dollars)  
**Calculation:** Gross rate minus agency commissions, broker fees, and other deductions  
**Usage:** Represents the final revenue the station actually receives from this spot

### Column 22: `sales_person` (Sales Person)
**Purpose:** Identifies the sales rep who sold this spot  
**Format:** Name or ID of the sales representative  
**Usage:** Used for commission tracking, sales reporting, and performance analysis

### Column 23: `revenue_type` (Revenue Type)
**Purpose:** Categorizes the type of revenue/business model for the spot  
**Format:** Text description of revenue category  
**Common types:**
- Internal Ad Sales
- Branded Content
- Paid Programming  
- Direct Response
- Other
- Services
- Production (replaced by Branded Content)
**Usage:** Affects reporting categories, commission structures, and revenue recognition

### Column 24: `billing_type` (Billing Type)
**Purpose:** Defines how months are interpreted for billing purposes  
**Format:** Text value - either "Calendar" or "Broadcast"  
**Options:**
- `Calendar` - Uses traditional calendar months (January 1-31, February 1-28/29, etc.)
- `Broadcast` - Uses broadcast industry month definitions (week-based boundaries, Sunday to Saturday)
**Usage:** Determines which month system is applied in the broadcast_month calculation and billing cycles

### Column 25: `agency_flag` (Agency?)
**Purpose:** Indicates whether the spot was purchased through an agency  
**Format:** Text value  
**Options:**
- `Agency` - Spot purchased through an advertising agency
- `Non-agency` - Spot purchased directly by the client
**Usage:** Affects commission calculations, billing processes, and sales reporting categories

### Column 26: `affidavit_flag` (Affidavit?)
**Purpose:** Previously indicated whether an affidavit was required for the spot  
**Historical use:** Used to flag spots that needed affidavit documentation (proof of airing)  
**Current status:** Field has been repurposed for another use that is not yet documented  
**Note:** Field definition and current usage need further investigation

### Column 27: `contract` (Notarize?)
**Purpose:** Currently contains the contract number for the spot  
**Format:** Alphanumeric contract identifier  
**Historical use:** Originally indicated whether the customer required the affidavit to be notarized  
**Current use:** Stores contract number for business tracking and reference  
**Note:** Notarization requirements are now captured in billing books rather than this field

### Column 28: `market_name` (Market)
**Purpose:** Identifies the geographic market where the spot aired  
**Format:** Three-letter market abbreviation codes  
**Common markets:**
- `SEA` - Seattle
- `SFO` - San Francisco  
- `LAX` - Los Angeles
- `MMT` - Multimarket coverage
- `DAL` - Dallas
- `CVC` - Central Valley
- `NYC` - New York
- `CMP` - Chicago Minneapolis St Paul  
- `HOU` - Houston
**Usage:** Used for market-specific analysis, reporting, and operational tracking

---

## Summary

This Excel column structure contains 29 columns (0-28) that capture comprehensive information about broadcast spots, including scheduling details, financial data, client information, and operational metadata. The data supports billing, scheduling, reporting, and operational requirements for a multi-market broadcasting operation.
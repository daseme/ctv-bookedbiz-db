# In Testing Navigation Section

## Problem

AE My Accounts (`/ae/my-accounts`) and Manager Dashboard (`/manager/dashboard`) are live but not discoverable from the home page or main navigation. They need visibility for testing without mixing into the established nav categories.

## Solution

Add an amber-colored "In Testing" section to both the home page card grid and the base template dropdown nav. Visible to AE, Management, and Admin roles only.

## Changes

### Home Page (`src/web/templates/index.html`)

New card category between "Budget & Planning" and "Planned Reports/Coming Soon":
- **My Accounts** — `/ae/my-accounts` — "CRM-style view of your book of business"
- **Manager Dashboard** — `/manager/dashboard` — "Team performance and account oversight"

Amber card styling (nord13 `#ebcb8b`). Wrapped in role check:
```jinja
{% if current_user.role.value in ('ae', 'management', 'admin') %}
```

### Base Template Nav (`src/web/templates/base.html`)

New 5th dropdown section after "Budgeting", before "Admin":
- Label: `🧪 In Testing` with amber/nord13 accent color
- Two featured dropdown items: My Accounts, Manager Dashboard
- Same hover dropdown pattern as existing sections
- Same role check as home page

### Visibility

Only AE, Management, and Admin roles see the section. Other roles see no change.

### What does NOT change

- Existing nav sections untouched
- Routes and pages unchanged
- "Planned Reports/Coming Soon" section stays as-is

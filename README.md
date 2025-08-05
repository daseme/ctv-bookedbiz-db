# Revenue Analysis System

A comprehensive revenue categorization and analysis system built with BaseQueryBuilder.

## Quick Start

```bash
# Run summary analysis
python revenue_analysis.py --year 2024

# Generate markdown report
python revenue_analysis.py --year 2024 --format markdown --output reports/revenue_2024.md

# Generate JSON report  
python revenue_analysis.py --year 2024 --format json --output reports/revenue_2024.json
```

## System Architecture

- `src/query_builders.py` - Core BaseQueryBuilder classes
- `src/revenue_analysis.py` - Main business logic engine
- `tests/migration_tests/` - Migration validation tests
- `reports/` - Generated reports

## Revenue Categories

1. **Individual Language Blocks** (59.5%) - Single language targeting
2. **Chinese Prime Time** (17.2%) - Cross-audience during Chinese prime hours
3. **Multi-Language (Cross-Audience)** (10.0%) - Filipino-led cross-cultural strategy
4. **Direct Response** (8.7%) - WorldLink agency advertising
5. **Other Non-Language** (1.4%) - Miscellaneous spots needing investigation
6. **Overnight Shopping** (1.6%) - NKB dedicated shopping programming
7. **Branded Content (PRD)** (1.3%) - Internal production work
8. **Services (SVC)** (0.3%) - Station services and announcements

## Perfect Reconciliation

The system achieves 0.000000% error rate with perfect revenue reconciliation across all categories.

## Strategic Insights

- **Chinese Market Dominance**: $1.35M+ combined strategy
- **Filipino Cross-Audience Leadership**: 60.3% of cross-audience revenue
- **Cross-Audience Strategy**: $1.1M+ total revenue
- **Weekend Programming**: Strong cross-audience weekend performance



%%{init: {"theme": "default"}}%%
flowchart TD
    subgraph Spot Flow [Spot Assignment Swimlanes]
        direction LR

        subgraph A [📥 Unassigned Spot]
            A1[Spot enters system]
        end

        subgraph B [🧠 Business Rules]
            B1[Check WorldLink]
            B2[Check Paid Programming]
            B3[Check Duration > 6 hrs]
            B4[Check Tagalog Pattern]
            B5[Check Chinese Evening Pattern]
        end

        subgraph C [📅 Schedule Lookup]
            C1[Find Schedule for Market + Date]
            C2[Find Overlapping Blocks]
        end

        subgraph D [🧬 Language Analysis]
            D1[Single Block → Language-Specific]
            D2[Multi-Block → Same Language?]
            D3[→ Same Family?]
            D4[→ Else → Multi-Language or ROS]
        end

        subgraph E [💾 Final Assignment]
            E1[Save to spot_language_blocks]
        end

        A1 --> B1
        B1 -->|Yes| E1
        B1 -->|No| B2
        B2 -->|Yes| E1
        B2 -->|No| B3
        B3 -->|Yes| E1
        B3 -->|No| B4
        B4 -->|Yes| E1
        B4 -->|No| B5
        B5 -->|Yes| E1
        B5 -->|No| C1
        C1 --> C2
        C2 --> D1
        D1 -->|Yes| E1
        D1 -->|No| D2
        D2 -->|Yes| E1
        D2 -->|No| D3
        D3 -->|Yes| E1
        D3 -->|No| D4
        D4 --> E1
    end

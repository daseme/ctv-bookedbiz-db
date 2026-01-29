"""
Repository for sector-level planning data.
Provides booked revenue broken down by sector for planning session drill-down.
"""

from dataclasses import dataclass
from typing import Dict, List
from decimal import Decimal

from src.services.base_service import BaseService
from src.database.connection import DatabaseConnection


@dataclass
class SectorBookedData:
    """Booked revenue for a single sector/month combination."""

    sector_id: int
    sector_code: str
    sector_name: str
    month: int
    booked_amount: Decimal
    spot_count: int


@dataclass
class EntitySectorBooked:
    """All sector booked data for an entity/year."""

    ae_name: str
    year: int
    records: List[SectorBookedData]

    def by_sector_month(self) -> Dict[int, Dict[int, SectorBookedData]]:
        """Returns {sector_id: {month: SectorBookedData}}"""
        result: Dict[int, Dict[int, SectorBookedData]] = {}
        for r in self.records:
            if r.sector_id not in result:
                result[r.sector_id] = {}
            result[r.sector_id][r.month] = r
        return result

    def sectors_with_activity(self) -> List[tuple]:
        """Returns [(sector_id, sector_code, sector_name), ...] with any booked revenue."""
        seen = {}
        for r in self.records:
            if r.sector_id not in seen:
                seen[r.sector_id] = (r.sector_id, r.sector_code, r.sector_name)
        return sorted(seen.values(), key=lambda x: x[2])  # Sort by name

    def total_for_sector(self, sector_id: int) -> Decimal:
        """Annual total booked for a sector."""
        return sum(r.booked_amount for r in self.records if r.sector_id == sector_id)


class SectorPlanningRepository(BaseService):
    """Repository for sector-level planning queries."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)

    def get_booked_by_sector(self, ae_name: str, year: int) -> EntitySectorBooked:
        """
        Get booked revenue by sector for an entity.

        For House: excludes WorldLink spots (identified by bill_code prefix 'WL').
        For AEs: straightforward sales_person match.
        """
        # Base query joins spots to customers to sectors
        query = """
            SELECT 
                COALESCE(c.sector_id, 0) as sector_id,
                COALESCE(s.sector_code, 'UNK') as sector_code,
                COALESCE(s.sector_name, 'Unassigned') as sector_name,
                substr(sp.broadcast_month, 1, 3) as month_abbr,
                SUM(COALESCE(sp.gross_rate, 0)) as booked_amount,
                COUNT(sp.spot_id) as spot_count
            FROM spots sp
            LEFT JOIN customers c ON sp.customer_id = c.customer_id
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE sp.sales_person = ?
              AND sp.broadcast_month LIKE ?
              AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        """

        params = [ae_name, f"%-{str(year)[2:]}"]

        # For House, exclude WorldLink spots
        if ae_name == "House":
            query += " AND (sp.bill_code NOT LIKE 'WL%' OR sp.bill_code IS NULL)"

        query += """
            GROUP BY 
                COALESCE(c.sector_id, 0),
                COALESCE(s.sector_code, 'UNK'),
                COALESCE(s.sector_name, 'Unassigned'),
                substr(sp.broadcast_month, 1, 3)
            ORDER BY sector_name, month_abbr
        """

        with self.safe_connection() as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

        # Parse month from broadcast_month format (e.g., "Jan-26")
        month_map = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
        }

        records = []
        for row in rows:
            # Row indices: 0=sector_id, 1=sector_code, 2=sector_name, 3=month_abbr, 4=booked, 5=count
            month_abbr = row[3] if row[3] else ""
            month_num = month_map.get(month_abbr[:3], 0) if month_abbr else 0
            records.append(
                SectorBookedData(
                    sector_id=row[0],
                    sector_code=row[1],
                    sector_name=row[2],
                    month=month_num,
                    booked_amount=Decimal(str(row[4])),
                    spot_count=row[5],
                )
            )

        return EntitySectorBooked(ae_name=ae_name, year=year, records=records)

    def get_booked_by_sector_all_entities(
        self, year: int
    ) -> Dict[str, EntitySectorBooked]:
        """
        Get sector booked data for all entities at once.
        More efficient than calling get_booked_by_sector repeatedly.
        """
        year_suffix = f"%-{str(year)[2:]}"

        query = """
            SELECT 
                sp.sales_person,
                COALESCE(c.sector_id, 0) as sector_id,
                COALESCE(s.sector_code, 'UNK') as sector_code,
                COALESCE(s.sector_name, 'Unassigned') as sector_name,
                substr(sp.broadcast_month, 1, 3) as month_abbr,
                SUM(COALESCE(sp.gross_rate, 0)) as booked_amount,
                COUNT(sp.spot_id) as spot_count
            FROM spots sp
            LEFT JOIN customers c ON sp.customer_id = c.customer_id
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE sp.broadcast_month LIKE ?
              AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
              AND sp.sales_person != 'House'
            GROUP BY 
                sp.sales_person,
                COALESCE(c.sector_id, 0),
                COALESCE(s.sector_code, 'UNK'),
                COALESCE(s.sector_name, 'Unassigned'),
                substr(sp.broadcast_month, 1, 3)
            
            UNION ALL
            
            -- House excluding WorldLink
            SELECT 
                'House' as sales_person,
                COALESCE(c.sector_id, 0) as sector_id,
                COALESCE(s.sector_code, 'UNK') as sector_code,
                COALESCE(s.sector_name, 'Unassigned') as sector_name,
                substr(sp.broadcast_month, 1, 3) as month_abbr,
                SUM(COALESCE(sp.gross_rate, 0)) as booked_amount,
                COUNT(sp.spot_id) as spot_count
            FROM spots sp
            LEFT JOIN customers c ON sp.customer_id = c.customer_id
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE sp.broadcast_month LIKE ?
              AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
              AND sp.sales_person = 'House'
              AND (sp.bill_code NOT LIKE 'WL%' OR sp.bill_code IS NULL)
            GROUP BY 
                COALESCE(c.sector_id, 0),
                COALESCE(s.sector_code, 'UNK'),
                COALESCE(s.sector_name, 'Unassigned'),
                substr(sp.broadcast_month, 1, 3)
            
            ORDER BY sales_person, sector_name, month_abbr
        """

        month_map = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
        }

        with self.safe_connection() as conn:
            cursor = conn.execute(query, [year_suffix, year_suffix])
            rows = cursor.fetchall()

        # Group by entity
        # Row indices: 0=sales_person, 1=sector_id, 2=sector_code, 3=sector_name, 4=month_abbr, 5=booked, 6=count
        result: Dict[str, List[SectorBookedData]] = {}
        for row in rows:
            ae = row[0]
            if ae not in result:
                result[ae] = []
            month_abbr = row[4] if row[4] else ""
            month_num = month_map.get(month_abbr[:3], 0) if month_abbr else 0
            result[ae].append(
                SectorBookedData(
                    sector_id=row[1],
                    sector_code=row[2],
                    sector_name=row[3],
                    month=month_num,
                    booked_amount=Decimal(str(row[5])),
                    spot_count=row[6],
                )
            )

        return {
            ae: EntitySectorBooked(ae_name=ae, year=year, records=records)
            for ae, records in result.items()
        }

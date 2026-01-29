"""
Service for sector-level planning detail.
Combines sector expectations with booked actuals.
"""

from typing import Dict, List, Optional

from src.models.sector_planning_models import (
    EntitySectorPlanningDetail,
    SectorPlanningRow,
)
from src.repositories.sector_planning_repository import SectorPlanningRepository
from src.repositories.sector_expectation_repository import SectorExpectationRepository


class SectorPlanningService:
    """Service for sector-level planning drill-down."""

    def __init__(
        self,
        sector_planning_repo: SectorPlanningRepository,
        sector_expectation_repo: SectorExpectationRepository,
    ):
        self.sector_planning_repo = sector_planning_repo
        self.sector_expectation_repo = sector_expectation_repo

    def get_sector_detail(self, ae_name: str, year: int) -> EntitySectorPlanningDetail:
        """
        Get complete sector planning detail for an entity.
        Combines expectations (budget theory) with booked actuals.
        """
        # Get expectations - returns List[SectorExpectation]
        expectations = self.sector_expectation_repo.get_by_entity_year(ae_name, year)

        # Get booked by sector
        booked_data = self.sector_planning_repo.get_booked_by_sector(ae_name, year)
        booked_by_sector = booked_data.by_sector_month()

        # Build unified sector list from both sources
        all_sectors: Dict[int, SectorPlanningRow] = {}

        # Add sectors from expectations (List[SectorExpectation])
        for exp in expectations:
            if exp.sector_id not in all_sectors:
                all_sectors[exp.sector_id] = SectorPlanningRow(
                    sector_id=exp.sector_id,
                    sector_code=exp.sector_code,
                    sector_name=exp.sector_name,
                )
            row = all_sectors[exp.sector_id]
            detail = row.for_month(exp.month)
            detail.expected = exp.expected_amount

        # Add/merge booked data
        for sector_id, month_data in booked_by_sector.items():
            if sector_id not in all_sectors:
                # Sector has booked revenue but no expectation
                first_record = list(month_data.values())[0]
                all_sectors[sector_id] = SectorPlanningRow(
                    sector_id=sector_id,
                    sector_code=first_record.sector_code,
                    sector_name=first_record.sector_name,
                )

            row = all_sectors[sector_id]
            for month, booked in month_data.items():
                detail = row.for_month(month)
                detail.booked = booked.booked_amount

        # Sort sectors by name
        sorted_sectors = sorted(all_sectors.values(), key=lambda s: s.sector_name)

        return EntitySectorPlanningDetail(
            ae_name=ae_name,
            year=year,
            sectors=sorted_sectors,
            has_sector_expectations=len(expectations) > 0,
        )

    def get_sector_detail_all_entities(
        self, year: int, entity_names: List[str]
    ) -> Dict[str, EntitySectorPlanningDetail]:
        """
        Get sector detail for multiple entities efficiently.
        Useful for pre-loading all data for the planning session.
        """
        # Batch fetch booked data
        all_booked = self.sector_planning_repo.get_booked_by_sector_all_entities(year)

        result = {}
        for ae_name in entity_names:
            # Skip WorldLink - no sector breakdown
            if ae_name == "WorldLink":
                continue

            # Get expectations for this entity
            expectations = self.sector_expectation_repo.get_by_entity_year(
                ae_name, year
            )

            # Get booked (from batch fetch, empty if not present)
            booked_data = all_booked.get(ae_name)
            booked_by_sector = booked_data.by_sector_month() if booked_data else {}

            # Build unified sector list
            all_sectors: Dict[int, SectorPlanningRow] = {}

            for exp in expectations:
                if exp.sector_id not in all_sectors:
                    all_sectors[exp.sector_id] = SectorPlanningRow(
                        sector_id=exp.sector_id,
                        sector_code=exp.sector_code,
                        sector_name=exp.sector_name,
                    )
                row = all_sectors[exp.sector_id]
                detail = row.for_month(exp.month)
                detail.expected = exp.expected_amount

            for sector_id, month_data in booked_by_sector.items():
                if sector_id not in all_sectors:
                    first_record = list(month_data.values())[0]
                    all_sectors[sector_id] = SectorPlanningRow(
                        sector_id=sector_id,
                        sector_code=first_record.sector_code,
                        sector_name=first_record.sector_name,
                    )

                row = all_sectors[sector_id]
                for month, booked in month_data.items():
                    detail = row.for_month(month)
                    detail.booked = booked.booked_amount

            sorted_sectors = sorted(all_sectors.values(), key=lambda s: s.sector_name)

            result[ae_name] = EntitySectorPlanningDetail(
                ae_name=ae_name,
                year=year,
                sectors=sorted_sectors,
                has_sector_expectations=len(expectations) > 0,
            )

        return result

    def get_gap_summary(self, ae_name: str, year: int) -> Optional[str]:
        """
        Get a one-line summary of sector gaps for display in the main planning grid.
        Returns None if entity has no sector expectations.
        """
        detail = self.get_sector_detail(ae_name, year)
        return detail.summary_message()

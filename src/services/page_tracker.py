from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from src.database.models import Base
from src.database.models import PageIndexTracker



class PageTrackerService:
    def __init__(self, session: Session):
        self.session = session

    def get_last_page_index(self, company_id: int, company_name: str) -> int:
        """
        Get the last processed page index for a company.
        Creates a new tracker if one doesn't exist.
        """
        try:
            tracker = self.session.query(PageIndexTracker).filter_by(
                company_id=company_id
            ).one()
            
            if tracker.last_page_index == 1:
                return 1
                
            return tracker.last_page_index - 1
            
        except NoResultFound:
            # Create new tracker if doesn't exist
            new_tracker = PageIndexTracker(
                company_id=company_id,
                company_name=company_name,
                last_page_index=1,
                last_updated=datetime.now()
            )
            self.session.add(new_tracker)
            self.session.commit()
            return 1

    def update_page_index(self, company_id: int, page_index: int) -> None:
        """Update the last processed page index for a company"""
        try:
            tracker = self.session.query(PageIndexTracker).filter_by(
                company_id=company_id
            ).one()
            
            tracker.last_page_index = page_index
            tracker.last_updated = datetime.now()
            self.session.commit()
            
        except NoResultFound:
            raise ValueError(f"No tracker found for company_id: {company_id}")
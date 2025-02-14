# File location: src/services/page_tracker.py
from datetime import datetime
import logging
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from src.database.models import Base
from src.database.models import PageIndexTracker



class PageTrackerService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def get_last_page_index(self, restaurant_id: int, restaurant_name: str) -> int:
        """
        Get the last processed page index for a company.
        Creates a new tracker if one doesn't exist.
        """
        self.logger.debug("get_last_page_index...")
        try:
            tracker = self.session.query(PageIndexTracker).filter_by(
                restaurant_id=restaurant_id
            ).one()
            
            if tracker.last_page_index == 1:
                return 1
                
            return tracker.last_page_index - 1
            
        except NoResultFound:
            # Create new tracker if doesn't exist
            new_tracker = PageIndexTracker(
                restaurant_id=restaurant_id,
                restaurant_name=restaurant_name,
                last_page_index=1,
                last_updated=datetime.now()
            )
            self.session.add(new_tracker)
            self.session.commit()
            return 1

    def update_page_index(self, restaurant_id: int, page_index: int) -> None:
        """Update the last processed page index for a company"""
        self.logger.debug("update_page_index...")
        try:
            tracker = self.session.query(PageIndexTracker).filter_by(
                restaurant_id=restaurant_id
            ).one()
            
            tracker.last_page_index = page_index
            tracker.last_updated = datetime.now()
            self.session.commit()
            
        except NoResultFound:
            raise ValueError(f"No tracker found for restaurant_id: {restaurant_id}")
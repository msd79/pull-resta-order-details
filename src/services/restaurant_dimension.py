# File: src/services/restaurant_dimension.py
from datetime import datetime
from sqlalchemy.orm import Session
from src.database.dimentional_models import DimRestaurant
from src.database.models import Restaurant
import logging
from typing import Optional

class RestaurantDimensionService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def update_restaurant_dimension(self, restaurant: Restaurant) -> int:
        try:
            current_record = self.session.query(DimRestaurant)\
                .filter(
                    DimRestaurant.restaurant_id == restaurant.id,
                    DimRestaurant.is_current == True
                ).first()

            # Create new record
            new_record = DimRestaurant(
                restaurant_id=restaurant.id,
                restaurant_name=restaurant.name,
                effective_date=datetime.now(),
                expiration_date=None,
                is_current=True
            )

            if current_record:
                current_record.expiration_date = datetime.now()
                current_record.is_current = False
                
            self.session.add(new_record)
            self.session.flush()  # Get the key before commit
            
            # Return the new key
            return new_record.restaurant_key

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating restaurant dimension: {str(e)}")
            raise






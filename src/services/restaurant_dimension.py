from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.orm import Session
from src.database.dimentional_models import DimRestaurant
from src.database.models import Restaurant, Order
import logging

class RestaurantDimensionService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def update_restaurant_dimension(self, restaurant: Restaurant) -> int:
        try:
            self.logger.info(f"Processing restaurant dimension for ID {restaurant.id}")
            
            # Get the current record, if it exists.
            self.logger.debug(f"Checking if dimension record exists for restaurant ID {restaurant.id}")
            current_record = self.session.query(DimRestaurant)\
                .filter(DimRestaurant.restaurant_id == restaurant.id).first()

            if current_record:
                self.logger.debug(f"Found existing record with key {current_record.restaurant_key}")
                self.logger.debug(f"Current name: '{current_record.restaurant_name}', New name: '{restaurant.name}'")
                
                if current_record.restaurant_name != restaurant.name:
                    self.logger.info(f"Updating restaurant {restaurant.id} name from '{current_record.restaurant_name}' to '{restaurant.name}'")
                    current_record.restaurant_name = restaurant.name
                    self.logger.debug("Flushing session to persist name change")
                    self.session.flush()
                    return current_record.restaurant_key
                else:
                    self.logger.debug("No name change detected, using existing record")
                    return current_record.restaurant_key

            # Create a new record if no record exists.
            self.logger.info(f"Creating new dimension record for restaurant '{restaurant.name}'")
            self.logger.debug(f"Restaurant details - ID: {restaurant.id}, Company ID: {getattr(restaurant, 'company_id', None)}")
            
            new_record = DimRestaurant(
                restaurant_id=restaurant.id,
                restaurant_name=restaurant.name,
                company_id=getattr(restaurant, 'company_id', None),
                company_name=getattr(restaurant, 'company_name', None)
            )
            
            self.session.add(new_record)
            self.logger.debug("Flushing session to generate restaurant_key")
            self.session.flush()
            
            self.logger.info(f"Created restaurant dimension with key {new_record.restaurant_key}")
            return new_record.restaurant_key

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Failed to update restaurant dimension for ID {restaurant.id}: {str(e)}")
            self.logger.debug(f"Restaurant data at failure: {vars(restaurant)}")
            raise
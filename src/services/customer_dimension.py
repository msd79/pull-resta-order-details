# File: src/services/customer_dimension.py
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging
from src.database.dimentional_models import DimCustomer
from src.database.models import Customer, Order

class CustomerDimensionService:
    """
    Service for managing customer dimensions.
    
    Note: While the table structure supports Type 2 SCD with history tracking,
    we currently maintain only current records (is_current=True) and update
    them in place. Historical records (is_current=False) are preserved but
    not actively maintained or created.
    """
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def transform_customer(self, customer: Customer, metrics: Dict[str, Any], restaurant_key: int) -> DimCustomer:
        """Transform a Customer record into a DimCustomer record."""
        try:
            self.logger.debug(f"Starting transformation for customer ID: {customer.id}")
            
            # Calculate age group
            age_group = self._calculate_age_group(customer.birth_date) if customer.birth_date else 'Unknown'
            self.logger.debug(f"Calculated age group for customer {customer.id}: {age_group}")
            
            # Calculate customer segment based on metrics
            customer_segment = self._determine_customer_segment(metrics)
            self.logger.debug(f"Determined customer segment for customer {customer.id}: {customer_segment}")
            
            # Calculate customer tenure
            customer_tenure_days = self._calculate_tenure_days(
                metrics.get('first_order_date'),
                metrics.get('last_order_date')
            )
            self.logger.debug(f"Calculated tenure for customer {customer.id}: {customer_tenure_days} days")
            
            dim_customer = DimCustomer(
                customer_id=customer.id,
                full_name=customer.full_name,
                email=customer.email,
                mobile=customer.mobile,
                birth_date=customer.birth_date,
                age_group=age_group,
                
                # Type 2 SCD fields
                effective_date=datetime.now(),
                expiration_date=None,
                is_current=True,
                
                # Status and preferences
                is_email_marketing_allowed=customer.is_email_marketing_allowed,
                is_sms_marketing_allowed=customer.is_sms_marketing_allowed,
                
                # Pre-calculated metrics
                lifetime_order_count=metrics.get('total_orders', 0),
                lifetime_order_value=round(metrics.get('total_spent', 0.0), 2),
                average_order_value = round(metrics.get('avg_order_value', 0.0), 2),
                first_order_date=metrics.get('first_order_date'),
                last_order_date=metrics.get('last_order_date'),
                customer_segment=customer_segment,
                customer_tenure_days=customer_tenure_days,
                restaurant_key=restaurant_key,
            )
            self.logger.debug(f"Successfully created dimension record for customer {customer.id}")
            return dim_customer
        except Exception as e:
            self.logger.error(f"Error transforming customer {customer.id}: {str(e)}", exc_info=True)
            raise

    def _calculate_age_group(self, birth_date: datetime) -> str:
        """Calculate age group based on birth date."""
        if not birth_date:
            return 'Unknown'
            
        age = (datetime.now() - birth_date).days // 365
        self.logger.debug(f"Calculated age: {age} years from birth date: {birth_date}")
        
        if age < 18:
            return 'Under 18'
        elif age < 25:
            return '18-24'
        elif age < 35:
            return '25-34'
        elif age < 45:
            return '35-44'
        elif age < 55:
            return '45-54'
        else:
            return '55+'

    def _determine_customer_segment(self, metrics: Dict[str, Any]) -> str:
        """Determine customer segment based on order history and value."""
        total_orders = metrics.get('total_orders', 0)
        avg_order_value = round(metrics.get('avg_order_value', 0.0), 2)
        
        self.logger.debug(f"Determining segment with total_orders={total_orders}, avg_order_value=${avg_order_value}")
        
        if total_orders >= 24 and avg_order_value >= 50:  # 2 orders per month and high value
            return 'VIP'
        elif total_orders >= 12:  # 1 order per month
            return 'Regular'
        elif total_orders >= 3:   # Quarterly orders
            return 'Occasional'
        else:
            return 'New'

    def _calculate_tenure_days(self, first_order_date: Optional[datetime], 
                             last_order_date: Optional[datetime]) -> int:
        """Calculate customer tenure in days."""
        if not first_order_date:
            self.logger.debug("No first order date found, returning tenure of 0 days")
            return 0
            
        end_date = last_order_date or datetime.now()
        tenure_days = (end_date - first_order_date).days
        self.logger.debug(f"Calculated tenure from {first_order_date} to {end_date}: {tenure_days} days")
        return tenure_days

    def get_customer_metrics(self, customer_id: int) -> Dict[str, Any]:
        """Calculate customer metrics from order history."""
        try:
            self.logger.info(f"Retrieving order metrics for customer ID: {customer_id}")
            self.logger.debug(f"Executing query to calculate metrics for customer {customer_id}")
            
            metrics = self.session.query(
                func.count(Order.id).label('total_orders'),
                func.sum(Order.total).label('total_spent'),
                func.min(Order.creation_date).label('first_order_date'),
                func.max(Order.creation_date).label('last_order_date')
            ).filter(
                Order.customer_id == customer_id
            ).first()
            
            result = {
                'total_orders': metrics[0] or 0,
                'total_spent': metrics[1] or 0.0,
                'first_order_date': metrics[2],
                'last_order_date': metrics[3]
            }
            
            # Calculate average order value
            if result['total_orders'] > 0:
                result['avg_order_value'] = result['total_spent'] / result['total_orders']
            else:
                result['avg_order_value'] = 0.0
            
            self.logger.debug(f"Customer {customer_id} metrics: {result}")
            self.logger.info(f"Successfully retrieved metrics for customer ID: {customer_id}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating customer metrics for ID {customer_id}: {str(e)}", exc_info=True)
            raise


    def update_customer_dimension(self, customer: Customer, restaurant_key: int) -> None:
        """Update customer dimension with simple in-place updates."""
        try:
            self.logger.info(f"Updating customer dimension for customer ID: {customer.id}")
            
            # Get current metrics
            metrics = self.get_customer_metrics(customer.id)
            
            # Get existing record
            existing_record = self.session.query(DimCustomer)\
                .filter(DimCustomer.customer_id == customer.id, DimCustomer.is_current == True).first()
            
            #  current_record = self.session.query(DimCustomer)\
            #     .filter(
            #         DimCustomer.customer_id == customer.id,
            #         DimCustomer.is_current == True
            #     ).first()
            
            if existing_record:
                # Update ALL attributes in place
                existing_record.full_name = customer.full_name
                existing_record.email = customer.email
                existing_record.mobile = customer.mobile
                existing_record.birth_date = customer.birth_date
                existing_record.age_group = self._calculate_age_group(customer.birth_date)
                existing_record.is_email_marketing_allowed = customer.is_email_marketing_allowed
                existing_record.is_sms_marketing_allowed = customer.is_sms_marketing_allowed
                
                # Update metrics
                existing_record.lifetime_order_count = metrics.get('total_orders', 0)
                existing_record.lifetime_order_value = round(metrics.get('total_spent', 0.0), 2)
                existing_record.average_order_value = round(metrics.get('avg_order_value', 0.0), 2)
                existing_record.first_order_date = metrics.get('first_order_date')
                existing_record.last_order_date = metrics.get('last_order_date')
                existing_record.customer_segment = self._determine_customer_segment(metrics)
                existing_record.customer_tenure_days = self._calculate_tenure_days(
                    metrics.get('first_order_date'),
                    metrics.get('last_order_date')
                )
                
                self.logger.debug(f"Updated existing dimension record for customer {customer.id}")
            else:
                # Create new record
                new_record = self.transform_customer(customer, metrics, restaurant_key)
                self.session.add(new_record)
                self.logger.debug(f"Created new dimension record for customer {customer.id}")
            
            self.session.commit()
            self.logger.info(f"Successfully updated customer dimension for customer ID: {customer.id}")
            
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating customer dimension for ID {customer.id}: {str(e)}")
            raise
    
    
    
    
    # def _has_tracked_changes(self, current: DimCustomer, new: DimCustomer) -> bool:
    #     """Check if any tracked attributes have changed (for Type 2 SCD)."""
    #     tracked_attributes = [
    #         'full_name',
    #         'email',
    #         'mobile',
    #         'birth_date',
    #         'is_email_marketing_allowed',
    #         'is_sms_marketing_allowed'
    #     ]
        
    #     for attr in tracked_attributes:
    #         current_val = getattr(current, attr)
    #         new_val = getattr(new, attr)
    #         if current_val != new_val:
    #             self.logger.debug(f"Tracked attribute '{attr}' changed: {current_val} -> {new_val}")
    #             return True
                
    #     return False

    # def _update_non_tracked_attributes(self, current: DimCustomer, new: DimCustomer) -> None:
    #     """Update non-tracked attributes (Type 1 changes)."""
    #     non_tracked_attributes = [
    #         'lifetime_order_count',
    #         'lifetime_order_value',
    #         'average_order_value',
    #         'last_order_date',
    #         'customer_segment',
    #         'customer_tenure_days'
    #     ]
        
    #     for attr in non_tracked_attributes:
    #         old_val = getattr(current, attr)
    #         new_val = getattr(new, attr)
    #         if old_val != new_val:
    #             self.logger.debug(f"Updating non-tracked attribute '{attr}': {old_val} -> {new_val}")
    #             setattr(current, attr, new_val)
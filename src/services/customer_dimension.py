# File: src/services/customer_dimension.py
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func
import logging
from src.database.dimentional_models import DimCustomer
from src.database.models import Customer, Order

class CustomerDimensionService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def transform_customer(self, customer: Customer, metrics: Dict[str, Any], restaurant_id: int) -> DimCustomer:
        """Transform a Customer record into a DimCustomer record."""
        try:
            # Calculate age group
            age_group = self._calculate_age_group(customer.birth_date) if customer.birth_date else 'Unknown'
            
            # Calculate customer segment based on metrics
            customer_segment = self._determine_customer_segment(metrics)
            
            # Calculate customer tenure
            customer_tenure_days = self._calculate_tenure_days(
                metrics.get('first_order_date'),
                metrics.get('last_order_date')
            )
            
            return DimCustomer(
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
                is_active=bool(customer.status == 1),  # Assuming status 1 is active
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
                restaurant_id=restaurant_id
            )
        except Exception as e:
            self.logger.error(f"Error transforming customer {customer.id}: {str(e)}")
            raise

    def _calculate_age_group(self, birth_date: datetime) -> str:
        """Calculate age group based on birth date."""
        if not birth_date:
            return 'Unknown'
            
        age = (datetime.now() - birth_date).days // 365
        
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
        
        if total_orders >= 24 and avg_order_value >= 50:  # 2 orders per month and high value
            return 'VIP'
        elif total_orders >= 12:  # 1 order per month
            return 'Regular'
        elif total_orders >= 4:   # Quarterly orders
            return 'Occasional'
        else:
            return 'New'

    def _calculate_tenure_days(self, first_order_date: Optional[datetime], 
                             last_order_date: Optional[datetime]) -> int:
        """Calculate customer tenure in days."""
        if not first_order_date:
            return 0
            
        end_date = last_order_date or datetime.now()
        return (end_date - first_order_date).days

    def get_customer_metrics(self, customer_id: int) -> Dict[str, Any]:
        """Calculate customer metrics from order history."""
        try:
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
                
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating customer metrics: {str(e)}")
            raise

    def update_customer_dimension(self, customer: Customer) -> None:
        """Update customer dimension with change tracking (Type 2 SCD)."""
        try:
            self.logger.info(f"Starting customer dimension update for customer ID: {customer.id}")
            
            # Get current metrics
            metrics = self.get_customer_metrics(customer.id)
            
            # Get current customer dimension record
            current_record = self.session.query(DimCustomer)\
                .filter(
                    DimCustomer.customer_id == customer.id,
                    DimCustomer.is_current == True
                ).first()
            
            # Create new dimension record
            new_record = self.transform_customer(customer, metrics, customer.restaurant_id)
            
            if current_record:
                # Check if any tracked attributes have changed
                if self._has_tracked_changes(current_record, new_record):
                    # Expire current record
                    current_record.expiration_date = datetime.now()
                    current_record.is_current = False
                    
                    # Add new record
                    self.session.add(new_record)
                else:
                    # Update non-tracked attributes of current record
                    self._update_non_tracked_attributes(current_record, new_record)
            else:
                # Add first record for this customer
                self.session.add(new_record)
            
            self.session.commit()
            self.logger.info(f"Successfully updated customer dimension for customer ID: {customer.id}")
            
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating customer dimension: {str(e)}")
            raise

    def _has_tracked_changes(self, current: DimCustomer, new: DimCustomer) -> bool:
        """Check if any tracked attributes have changed (for Type 2 SCD)."""
        tracked_attributes = [
            'full_name',
            'email',
            'mobile',
            'birth_date',
            'is_email_marketing_allowed',
            'is_sms_marketing_allowed'
        ]
        
        return any(
            getattr(current, attr) != getattr(new, attr)
            for attr in tracked_attributes
        )

    def _update_non_tracked_attributes(self, current: DimCustomer, new: DimCustomer) -> None:
        """Update non-tracked attributes (Type 1 changes)."""
        non_tracked_attributes = [
            'lifetime_order_count',
            'lifetime_order_value',
            'average_order_value',
            'last_order_date',
            'customer_segment',
            'customer_tenure_days'
        ]
        
        for attr in non_tracked_attributes:
            setattr(current, attr, getattr(new, attr))
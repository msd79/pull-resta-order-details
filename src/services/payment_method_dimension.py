# File: src/services/payment_method_dimension.py
from sqlalchemy.orm import Session
from src.database.dimentional_models import DimPaymentMethod
from src.database.models import Payment
import logging

class PaymentMethodDimensionService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def update_payment_method_dimension(self, payment: Payment, restaurant_id: int) -> None:
        try:
            dim_payment_method = self.session.query(DimPaymentMethod)\
                .filter(
                    DimPaymentMethod.payment_method_id == payment.payment_method_id,
                    DimPaymentMethod.restaurant_id == restaurant_id  # NEW
                ).first()

            if not dim_payment_method:
                dim_payment_method = DimPaymentMethod(
                    payment_method_id=payment.payment_method_id,
                    payment_method_name=payment.payment_method_name,
                    payment_method_type=payment.payment_method_type,
                    is_digital=payment.payment_method_type in [1, 2],
                    is_card=payment.payment_method_type == 1,
                    is_cash=payment.payment_method_type == 3,
                    requires_extra_charge=payment.extra_charge > 0,
                    restaurant_id=restaurant_id  # NEW
                )
                
                self.session.add(dim_payment_method)
                self.session.flush()  # Get the key before commit
            return dim_payment_method.payment_method_key

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating payment method dimension: {str(e)}")
            raise

# File: src/services/promotion_dimension.py
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from src.database.dimentional_models import DimPromotion
from src.database.models import Promotion
import logging

class PromotionDimensionService:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def update_promotion_dimension(self, promotion: Promotion) -> Optional[int]:
            """
            Update promotion dimension and return the promotion key.
            
            Args:
                promotion: The promotion record to process
                
            Returns:
                int: The promotion dimension key if successful
                None: If an error occurs
            """
            try:
                dim_promotion = self.session.query(DimPromotion)\
                    .filter(DimPromotion.promotion_id == promotion.id)\
                    .first()

                if not dim_promotion:
                    dim_promotion = DimPromotion(
                        promotion_id=promotion.id,
                        promotion_name=promotion.name,
                        promotion_description=promotion.description,
                        promotion_type=promotion.promotionType,
                        benefit_type=promotion.benefitType,
                        discount_type=promotion.discountType,
                        discount_amount=promotion.discountAmount,
                        min_subtotal=promotion.minSubTotal,
                        coupon_code=promotion.couponCode,
                        is_first_order_only=promotion.onlyFirstOrder,
                        is_once_per_customer=promotion.oncePerCustomer,
                        company_id=promotion.companyID,
                        restaurant_id=promotion.restaurant_id
                    )
                    self.session.add(dim_promotion)
                    self.session.flush()  # This ensures the key is generated
                
                return dim_promotion.promotion_key

            except Exception as e:
                self.session.rollback()
                self.logger.error(f"Error updating promotion dimension: {str(e)}")
                raise
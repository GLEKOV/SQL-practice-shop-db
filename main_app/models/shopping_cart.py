from datetime import datetime, timezone
from sqlalchemy import ForeignKey, Integer, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from .base import Base


class ShoppingCart(Base):
    """
    Stores temporary user-selected products before order completion.

    Linked to user_account and products.

    Fields: quantity, added_at.

    Optional unique constraint to prevent duplicate product entries in a user’s cart.

    паттерн «корзина покупок» через отдельную таблицу.
    """

    __tablename__ = "shopping_cart"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign keys
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)

    # Cart info
    quantity: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    added_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    user = relationship("User", back_populates="cart_items")
    product = relationship("Product")

    __table_args__ = (
        UniqueConstraint("user_id", "product_id", name="uix_user_product_cart"),
    )

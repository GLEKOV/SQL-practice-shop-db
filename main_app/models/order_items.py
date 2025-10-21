from datetime import datetime, timezone
from sqlalchemy import ForeignKey, Integer, Numeric
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from .base import Base


class OrderItem(Base):
    """
    Stores individual products within an order.

    Linked to orders and products.

    Fields: quantity, unit_price (price at time of order).

    Relationships allow retrieving all items of an order or all orders containing a product.

    Timestamps track creation and updates.

    Relationship structure: 1 - m - 1

    orders (1) ──< order_items >── (1) products
    """

    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign keys
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)

    # Item details
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    unit_price: Mapped[float] = mapped_column(
        Numeric(10, 2), nullable=False
    )  # price at the time of order

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
    )

    # Relationships
    order = relationship("Order", back_populates="items")
    product = relationship("Product", back_populates="order_items")

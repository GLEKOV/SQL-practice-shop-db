from datetime import datetime, timezone
from sqlalchemy import String, Integer, Numeric, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from .base import Base


class Payment(Base):
    """
    Stores payment history for orders.

    Linked to orders and user_account.

    Fields: payment_method, amount, status, transaction_id (external reference).

    Timestamps track creation and updates.
    """

    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign keys
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)

    # Payment info
    payment_method: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # e.g., credit_card, paypal
    amount: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    status: Mapped[str] = mapped_column(
        String(50), default="pending", nullable=False
    )  # pending, completed, failed
    transaction_id: Mapped[str | None] = mapped_column(
        String(100), unique=True
    )  # external payment ID

    # Audit timestamps
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    order = relationship("Order", back_populates="payments")
    user = relationship("User", back_populates="payments")

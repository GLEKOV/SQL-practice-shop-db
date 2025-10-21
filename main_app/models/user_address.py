from datetime import datetime, timezone
from sqlalchemy import String, Integer, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from .base import Base


class UserAddress(Base):
    """
    Stores addresses for users; one user can have multiple addresses.

    Fields: line1, line2, city, state, postal_code, country.

    Flags is_default_shipping and is_default_billing for default addresses.

    Timestamps created_at and updated_at track creation and updates.

    Linked to orders via shipping_address_id.
    """

    __tablename__ = "user_address"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to user
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)

    # Address fields
    line1: Mapped[str] = mapped_column(
        String(255), nullable=False
    )  # street, house number
    line2: Mapped[str | None] = mapped_column(String(255))  # apartment, suite, optional
    city: Mapped[str] = mapped_column(String(100), nullable=False)
    state: Mapped[str | None] = mapped_column(String(100))
    postal_code: Mapped[str] = mapped_column(String(20), nullable=False)
    country: Mapped[str] = mapped_column(
        String(2), nullable=False
    )  # ISO 3166-1 alpha-2

    # Metadata
    is_default_shipping: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    is_default_billing: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # Audit timestamps
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
    )
    # используется lambda чтобы было актуальное время, а не просто то время которое было на момент загрузки модуля

    # Relationships
    user = relationship("User", back_populates="addresses")
    orders = relationship("Order", back_populates="shipping_address")

from datetime import datetime, timezone
import enum

from sqlalchemy import (
    String,
    DateTime,
    Boolean,
    Integer,
    Enum as SQLAlchemyEnum,
    CheckConstraint,
    Index,
    UniqueConstraint,
    text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


# region User Status ENUM
class UserStatus(enum.Enum):
    active = "active"
    banned = "banned"
    deleted = "deleted"


# endregion


class User(Base):
    """
    Stores unique users of the system.

    Fields for authentication: email, password_hash.

    Optional fields for GDPR, last login, password change tracking.

    is_active indicates if account is enabled.

    Timestamps created_at and updated_at track account creation and updates.

    Relationships: orders, reviews, payments, wishlist, cart_items.
    """

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # contacts
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    phone: Mapped[str | None] = mapped_column(String(32), unique=True, nullable=True)

    # authentication
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    password_algo: Mapped[str] = mapped_column(String(50), nullable=True)
    password_changed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    # nullable=True is default for mapped_column

    # localisation profile
    preferred_locale: Mapped[str] = mapped_column(
        String(10), server_default=text("'en'"), nullable=False
    )
    timezone: Mapped[str] = mapped_column(
        String(50), server_default=text("'UTC'"), nullable=False
    )
    default_currency: Mapped[str] = mapped_column(
        String(3), server_default=text("'USD'"), nullable=False
    )

    # terms and policies accepted
    marketing_opt_in: Mapped[bool] = mapped_column(
        Boolean, server_default=text("false"), nullable=False
    )
    terms_accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # account status and security
    status: Mapped[UserStatus] = mapped_column(
        SQLAlchemyEnum(UserStatus, name="user_status", native_enum=False),
        server_default=text("'active'"),
        nullable=False,
    )
    failed_login_attempts: Mapped[int] = mapped_column(
        Integer, server_default=text("0"), nullable=False
    )
    lockout_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # GDPR - General Data Protection Regulation
    gdpr_erasure_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )

    # region Relationships
    # 1-n
    orders = relationship("Order", back_populates="user", cascade="all, delete-orphan")
    reviews = relationship(
        "Review", back_populates="user", cascade="all, delete-orphan"
    )
    payments = relationship(
        "Payment", back_populates="user", cascade="all, delete-orphan"
    )
    wishlist = relationship(
        "Wishlist", back_populates="user", cascade="all, delete-orphan"
    )
    cart_items = relationship(
        "ShoppingCart", back_populates="user", cascade="all, delete-orphan"
    )
    addresses = relationship(
        "UserAddress", back_populates="user", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint("email <> ''", name="ck_user_account_email_non_empty"),
        Index("ix_user_account_created_at", "created_at"),
        Index("ix_user_account_last_login_at", "last_login_at"),
        Index("ix_user_account_status_created_at", "status", "created_at"),
    )
    # endregion

    def __repr__(self) -> str:
        return f"<UserAccount id={self.id} email={self.email} status={self.status}>"

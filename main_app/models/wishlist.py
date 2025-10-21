from datetime import datetime, timezone
from sqlalchemy import String, Integer, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from .base import Base
from .m2m_wishlist_items import WishlistItem
from .user import User

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


class Wishlist(Base):
    """
    Contains name, timestamps and link to m2m table wishlist_items
    """

    __tablename__ = "wishlists"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to user
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)

    name: Mapped[str] = mapped_column()

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

    items: Mapped[list["WishlistItem"]] = relationship(
        "WishlistItem", back_populates="wishlist", cascade="all, delete-orphan"
    )
    user: Mapped[list["User"]] = relationship("User", back_populates="wishlist")

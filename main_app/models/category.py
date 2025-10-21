from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from .base import Base
from .product import Product


class Category(Base):
    """
    Stores product categories with optional hierarchical structure.

    Fields: name, slug (unique), description.

    is_active indicates if category is visible.

    Self-referential parent_id allows parent-child relationships.

    Relationships: children (subcategories), products via product_categories.

    Timestamps track creation and updates.
    """

    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Hierarchy
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("categories.id"), nullable=True
    )

    # Category info
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(500))

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Audit timestamps
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    parent: Mapped[Optional["Category"]] = relationship(
        "Category", remote_side=[id], backref="children"
    )
    products: Mapped[list["Product"]] = relationship(
        "Product", secondary="products_categories", back_populates="categories"
    )

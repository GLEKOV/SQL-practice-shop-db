from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, Numeric, DateTime, Boolean, Text
from .base import Base

if TYPE_CHECKING:
    from .category import Category
from .m2m_wishlist_items import WishlistItem


class Product(Base):
    """
    Stores all products available in the store.

    Fields: name, sku, description, price, stock_quantity.

    is_active indicates product visibility.

    Supports category relation via product_categories.

    created_at and updated_at track product creation and updates.

    Relationships: order_items, reviews, wishlisted_by.
    """

    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # main info
    sku: Mapped[str] = mapped_column(
        String(50), unique=True, nullable=False
    )  # unique prodict code
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # product name
    description: Mapped[str | None] = mapped_column(Text)
    brand: Mapped[str | None] = mapped_column(String(100))

    # price and stock
    price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    discount_price: Mapped[float | None] = mapped_column(Numeric(10, 2))
    stock: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )  # is the product active for sale

    # SEO & publishing
    slug: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False
    )  # URL slug
    meta_title: Mapped[str | None] = mapped_column(String(255))
    meta_description: Mapped[str | None] = mapped_column(String(500))

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # region Relationships
    # 1-N
    order_items = relationship(
        "OrderItem", back_populates="product", cascade="all, delete-orphan"
    )
    reviews = relationship(
        "Review", back_populates="product", cascade="all, delete-orphan"
    )

    # M2M
    wishlists: Mapped[list["WishlistItem"]] = relationship(
        "WishlistItem", back_populates="product"
    )
    categories: Mapped[list["Category"]] = relationship(
        "Category", secondary="products_categories", back_populates="products"
    )
    # endregion

    def __repr__(self) -> str:
        return f"<Product id={self.id} sku={self.sku} name={self.name}>"

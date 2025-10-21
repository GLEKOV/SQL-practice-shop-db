from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase
from .base import Base


class ProductCategory(Base):
    """
    Association table for many-to-many relationship between products and categories.

    Fields: product_id, category_id.

    Optional unique constraint to prevent duplicate product-category pairs.
    """

    __tablename__ = "products_categories"

    # id is not needed and just takes space on disc

    # Foreign keys
    product_id: Mapped[int] = mapped_column(
        ForeignKey("products.id"),
        nullable=False,
        primary_key=True,
    )
    category_id: Mapped[int] = mapped_column(
        ForeignKey("categories.id"), nullable=False, primary_key=True
    )

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from main_app.models.fake_data_gen.fake_data import FakeData


DATABASE_URL = "postgresql+asyncpg://glekov:StrongPass123!@localhost:5433/sql_shop"


engine = create_async_engine(
    DATABASE_URL, echo=True, echo_pool=False, pool_size=5, max_overflow=10
)
session_factory = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autocommit=False,
    class_=AsyncSession,
    autoflush=False,
)


fake_data = FakeData()


async def fill_users(how_many: int = 50):

    users = fake_data.create_fake_users(how_many)

    async with session_factory() as session:
        async with session.begin():
            session.add_all(users)
        # commit произойдёт автоматически из-за session.begin()


async def fill_categories():
    async with session_factory() as session:
        async with session.begin():
            categories = fake_data.create_fake_categories()
            session.add_all(categories)
        # commit произойдёт автоматически из-за session.begin()


async def fill_products(how_many: int = 100):

    products = await fake_data.create_fake_products(how_many)

    async with session_factory() as session:
        async with session.begin():
            session.add_all(products)
        # commit произойдёт автоматически из-за session.begin()


async def fill_addresses(how_many=5000):
    await fake_data.create_fake_addresses(how_many=how_many)


async def fill_orders(how_many=100):
    await fake_data.create_fake_orders(how_many=how_many, max_items_in_order=10)


async def main():
    async with engine.begin() as conn:
        # region добавляем данные

        # await fill_users(10000)     # 1
        # await fill_categories()     # 2
        # await fill_products(100)    # 3
        # await fill_addresses(9500)  # 4
        await fill_orders(5000)  # 5
        # endregion


if __name__ == "__main__":
    asyncio.run(main())

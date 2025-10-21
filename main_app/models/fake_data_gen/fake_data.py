import random
import hashlib
from datetime import datetime, timedelta, timezone
from typing import List
from faker import Faker
from sqlalchemy import select, func, exists
from sqlalchemy.orm import selectinload

from main_app.db_helper import db_helper
from main_app.models.user import User, UserStatus

from .storage import get_categories, DataLists, OrderStatusENUM
from .. import Category, Product, Order, OrderItem, UserAddress


class FakeData:
    """Class for generating fake data for database seeding."""

    def __init__(self, seed: int | None = None):
        """Initialisation of faker object
        without seed(full random data)
        or with seed (same random data every generation)"""

        # limit on how many objects can be created per function call
        self.limitation = 10000
        self.batch_size = 500

        self.faker = Faker()
        if seed:
            self.faker.seed_instance(seed)
            random.seed(seed)

    # region Utils
    @staticmethod
    def fake_password_hash(password: str) -> str:
        """Generate fake password hash."""
        return hashlib.sha256(password.encode()).hexdigest()

    @staticmethod
    async def check_user_exists(user_id: int) -> bool:
        async with db_helper.session_factory() as session:
            result = await session.execute(select(User).where(User.id == user_id))
            return result.scalar_one_or_none() is not None

    @staticmethod
    async def check_product_exists(product_id: int) -> bool:
        async with db_helper.session_factory() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            return result.scalar_one_or_none() is not None

    @staticmethod
    async def get_min_max_user_id() -> tuple[int | None, int | None]:
        async with db_helper.session_factory() as session:
            result = await session.execute(select(func.min(User.id), func.max(User.id)))
            min_id, max_id = result.one()
            return min_id, max_id

    @classmethod
    async def get_random_existing_user_id(cls) -> int | None:
        """Возвращает случайный ID пользователя в БД или None"""
        min_id, max_id = await cls.get_min_max_user_id()

        if min_id is None or max_id is None:
            return None

        for _ in range(10000):
            random_id = random.randint(min_id, max_id)
            if await cls.check_user_exists(user_id=random_id):
                return random_id

    # endregion

    def create_fake_users(self, how_many: int) -> List[User]:
        """Return list of fake User objects."""
        users: List[User] = []
        # limitation for
        how_many = min(how_many, self.limitation)
        for _ in range(how_many):
            email = self.faker.unique.email()
            phone = self.faker.unique.msisdn()[:12]
            password = self.faker.password(length=10)
            password_hash = self.fake_password_hash(password=password)

            user = User(
                email=email,
                phone=phone,
                password_hash=password_hash,
                password_algo="sha256",
                password_changed_at=datetime.now(timezone.utc)
                - timedelta(days=random.randint(0, 365)),
                preferred_locale=random.choice(["en", "de", "fr", "ru"]),
                timezone="UTC",
                default_currency=random.choice(["USD", "EUR", "GBP", "JPY"]),
                marketing_opt_in=random.choice([True, False]),
                terms_accepted_at=datetime.now(timezone.utc)
                - timedelta(days=random.randint(1, 1000)),
                status=random.choice(list(UserStatus)),
                failed_login_attempts=random.randint(0, 5),
                lockout_until=None,
                created_at=datetime.now(timezone.utc)
                - timedelta(days=random.randint(1, 1000)),
                updated_at=datetime.now(timezone.utc),
                last_login_at=datetime.now(timezone.utc)
                - timedelta(days=random.randint(1, 300)),
                gdpr_erasure_requested_at=None,
            )
            users.append(user)

        return users

    @staticmethod
    def create_fake_categories():
        return get_categories()

    async def create_fake_products(self, how_many: int):
        """Creates fake products in DB"""
        how_many = min(how_many, self.limitation)

        async def get_categories_from_db():
            async with db_helper.session_factory() as session:
                result = await session.execute(select(Category))
                categories_from_db = result.scalars().all()
                return categories_from_db

        def choose_categories(categories: list[Category]) -> list[Category]:
            chosen_c = []
            for _ in range(3):
                chosen_c.append(random.choice(categories))
            return chosen_c

        categories_from_db = await get_categories_from_db()

        products: list[Product] = []
        for _ in range(how_many):
            category_list = choose_categories(categories_from_db)
            name = self.faker.catch_phrase()
            sku = self.faker.unique.bothify(text="???-########")
            brand = self.faker.company()
            price = round(random.uniform(5, 2000), 2)
            discount_price = (
                round(price * random.uniform(0.7, 0.95), 2)
                if random.random() < 0.3
                else None
            )
            stock = random.randint(0, 500)
            slug = self.faker.unique.slug()
            description = self.faker.paragraph(nb_sentences=3)
            meta_title = name
            meta_description = description[:100]

            product = Product(
                name=name,
                sku=sku,
                description=description,
                brand=brand,
                price=price,
                discount_price=discount_price,
                stock=stock,
                is_active=True,
                slug=slug,
                meta_title=meta_title,
                meta_description=meta_description,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                categories=category_list,
            )
            products.append(product)
        return products

    async def create_fake_reviews(self, how_many: int):
        pass

    async def create_fake_addresses(self, how_many: int):
        """Creates fake addresses for random users who don't have addresses in DB in batches"""
        how_many = min(how_many, self.limitation)
        data_lists = DataLists()

        #  одна сессия на большую транзакцию
        async with db_helper.session_factory() as session:
            # объекты адресов будут добавляться порционно - батчами
            batch = []

            for _ in range(how_many):
                # рандомные адреса будут добавляться для рандомных пользователей просто потому что
                random_user_id = await self.get_random_existing_user_id()
                if random_user_id is None:
                    continue
                # проверка на существование адреса у выбранного пользователя
                stmt = select(exists().where(UserAddress.user_id == random_user_id))
                # если адрес у выбранного случайного пользователя в БД уже есть - идем дальше по циклу
                # делаем еще проверку на дупликат юзера внутри батча
                exists_in_batch = any(
                    address.user_id == random_user_id for address in batch
                )
                # await нужно ставить ДО вызова scalar() потому что scalar применяется к результату, а не корутине
                if (await session.execute(stmt)).scalar() or exists_in_batch:
                    continue
                user_address: UserAddress = UserAddress(
                    user_id=random_user_id,
                    line1=random.choice(data_lists.streets)
                    + " "
                    + str(random.randint(1, 144))
                    + (random.choice("ABCDEFG") if (random.randint(1, 4) == 3) else ""),
                    line2=(
                        str(random.randint(1, 88))
                        if random.randint(0, 1) == 0
                        else None
                    ),
                    city=random.choice(data_lists.cities),
                    state=random.choice(data_lists.states),
                    postal_code=str(random.randint(100000, 10000000)),
                    country=random.choice(data_lists.countries),
                    is_default_shipping=(random.randint(0, 1) == 1),
                    is_default_billing=(random.randint(0, 1) == 1),
                )

                batch.append(user_address)
                # порционное добавление объектов в сессию
                if len(batch) >= self.batch_size:
                    session.add_all(batch)
                    await session.commit()
                    batch.clear()

            if batch:  # добавляем оставшиеся объекты
                session.add_all(batch)

            await session.commit()

    async def create_fake_orders(self, how_many: int, max_items_in_order: int):
        """Creates orders data in DB"""
        how_many = min(how_many, self.limitation)
        max_items_in_order = min(max_items_in_order, self.limitation)

        async with db_helper.session_factory() as session:
            for _ in range(how_many):

                random_user_id = await self.get_random_existing_user_id()
                if random_user_id is None:
                    continue

                res = await session.execute(
                    select(User)
                    .options(selectinload(User.addresses))
                    .where(User.id == random_user_id)
                )
                current_user: User = res.scalar_one()
                if not current_user.addresses:
                    continue

                order_status_gen = random.choice(
                    [
                        OrderStatusENUM.PENDING,
                        OrderStatusENUM.PAID,
                        OrderStatusENUM.SHIPPING,
                        OrderStatusENUM.DELIVERED,
                        OrderStatusENUM.COMPLETED,
                        OrderStatusENUM.CANCELED,
                    ]
                )
                order_number = f"{datetime.now(timezone.utc).replace(tzinfo=None):%Y%m%d}-{current_user.id}-{random.randint(100000, 999999)}"

                order_items: list[OrderItem] = []

                # генерируем список id всех продуктов
                product_ids = [
                    p for p in (await session.execute(select(Product.id))).scalars()
                ]
                if not product_ids:
                    continue

                for _ in range(random.randint(1, max_items_in_order)):

                    product_id = random.choice(product_ids)

                    stmt = select(Product).where(Product.id == product_id)
                    res = await session.execute(stmt)
                    current_product: Product = res.scalar_one()
                    item = OrderItem(
                        product_id=product_id,
                        quantity=random.randint(1, 100),
                        unit_price=current_product.price,
                    )
                    order_items.append(item)

                order: Order = Order(
                    user_id=random_user_id,
                    shipping_address_id=current_user.addresses[0].id,
                    order_number=order_number,
                    status=order_status_gen,
                    total_amount=sum(
                        (item.unit_price * item.quantity) for item in order_items
                    ),
                    is_paid=(order_status_gen != OrderStatusENUM.PENDING.value),
                    items=order_items,
                )
                session.add(order)

            await session.commit()

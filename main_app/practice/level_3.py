"""Описание задач уровня 3

Задачи этого уровня направлены на развитие уверенного понимания работы JOIN, GROUP BY,
агрегатных функций и связей между таблицами.

Основной упор делается не на простые выборки, а на анализ данных после объединения таблиц
и формирование сводной информации (топы, средние, суммы и т.д.).


Основные цели:
 - Научиться мыслить таблицами — понимать, какую структуру данных формирует каждый JOIN.
 - Освоить группировку и агрегацию: GROUP BY, COUNT, SUM, AVG, MAX, MIN.
 - Разобраться, как получать агрегированные данные по связям M:N и 1:N.
 - Уметь писать как чистые SQL-запросы, так и эквиваленты на ORM (через SQLAlchemy Core).
 - Научиться использовать ORDER BY, LIMIT, COALESCE, и базовую параметризацию в text().


Инструменты и приемы:
 - JOIN / LEFT JOIN — объединение связанных таблиц.
 - GROUP BY — схлопывание строк по ключу.
 - Агрегатные функции — COUNT, SUM, AVG.
 - ORDER BY, LIMIT — сортировка и ограничение выборки.
 - COALESCE() — замена NULL на значение по умолчанию.
 - text() и плейсхолдеры (:param) — безопасная параметризация SQL.
 - func из SQLAlchemy ORM — аналог агрегатов.
 - select().join().group_by().order_by() — конструктор ORM-запросов.


Типичные кейсы уровня 3:
 - Подсчёт заказов или товаров по категориям.
 - Топ-N записей по количеству продаж или сумме.
 - Расчёт средней цены / суммы / количества.
 - Вывод агрегированных данных для конкретного пользователя.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import text, select, asc, desc, func

from models import Product, Category, OrderItem, ProductCategory, Order, User


class Top10ProductsByOrders:
    """Вывести product_id, product_name, order_count — топ-10 товаров, которые встречаются в заказах чаще всего."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко: основная таблица - order_items, в которой нужно посчитать количество повторяющихся товаров,
        соединив с products по id, чтобы получить еще и product_name

        Нам понадобятся таблицы products и order_items
        у каждого заказа может быть несколько позиций (order_items)
        Нужно понять сколько раз встречается каждый товар в заказах

        Делаем JOIN
        FROM products p
        JOIN order_items oi ON p.id = oi.product_id

        После этого получаем таблицу, где каждая строка - это позиция товара в заказе
        Сколько заказов с товаром - столько и строк с этим товаром
        таблица
        product_id | name | order_id | quantity

        Чтобы получить одну строку на товар мы схлопываем таблицу по повторяющимся product_id
        с помощью группировки GROUP BY.
        Агрегат COUNT(*) покажет, сколько раз этот товар встречался в заказах.
        результирующая таблица
        product_id | product_name | order_count
        """
        query = text(
            """
        SELECT
            p.id AS product_id,
            p.name AS product_name,
            COUNT(*) AS order_count
        FROM products p
        JOIN order_items oi ON p.id = oi.product_id
        GROUP BY p.id, p.name
        ORDER BY order_count DESC
        LIMIT 10;        
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        stmt = (
            select(Product.id, Product.name, func.count().label("order_count"))
            .join(OrderItem, Product.id == OrderItem.product_id)
            .group_by(Product.id, Product.name)
            .order_by(desc("order_count"))
            .limit(10)
        )

        res = await self.session.execute(stmt)
        rows = res.mappings().all()
        return rows


class AveragePriceByCategory:
    """Вывести среднюю цену товаров в каждой категории по убыванию: category_id, category_name, avg_price"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко: Соединив M:N таблицы, схлопываем их по категории и применяем агрегат AVG(p.price)

        Таблицы products и categories связаны через M:N ассоциативную таблицу products_categories
        соединяем две таблицы через ассоциативную
        FROM products p
        JOIN products_categories pc ON p.id = pc.product_id
        JOIN categories c ON pc.category_id = c.id

        теперь каждая строка - это товар и его категория
        если товар принадлежит нескольким категориям, он появится несколько раз

        Группируем по category_id, category_name, в каждой группе все товары категории.
        Агрегат AVG(p.price) возвращает среднюю цену по группе
        """
        query = text(
            """
        SELECT
            c.id AS category_id,
            c.name AS category_name,
            AVG(p.price) as avg_price
        FROM products p
        JOIN products_categories pc ON p.id = pc.product_id
        JOIN categories c ON pc.category_id = c.id
        GROUP BY c.id, c.name
        ORDER BY avg_price DESC;
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        stmt = (
            select(
                Category.id, Category.name, func.avg(Product.price).label("avg_price")
            )
            .join(ProductCategory, Category.id == ProductCategory.category_id)
            .join(Product, ProductCategory.product_id == Product.id)
            .group_by(Category.id, Category.name)
            .order_by(desc("avg_price"))
        )

        res = await self.session.execute(stmt)
        rows = res.mappings().all()
        return rows


class UserAllOrdersSumByOrders:
    """Для пользователя user_id = 7 вывести список его заказов с суммой каждого."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self, user_id: int):
        """Мышление-решение
        Кратко: Получаем таблицу с всеми отдельными товарами пользователя по всем заказам
        схлопываем по заказам, высчитывая сумму каждого заказа агрегатом SUM(quantity*price)

        Нам понадобятся таблицы orders, order_items

        FROM orders o
        JOIN order_items ON o.id = oi.order_id
        WHERE o.user_id = :user_id

        Теперь каждая строка — позиция заказа конкретного пользователя.
        То есть если у него 3 заказа, в каждом по 5 товаров — получаем 15 строк.

        Чтобы получить по одной строке на заказ — группируем по order_id.
        Агрегат: SUM(oi.quantity * oi.price) - сумма по заказу.
        """

        # тут используется параметризация через bind-переменные
        # в SQLAlchemy text() можно использовать именованные параметры с двоеточием - плейсхолдеры
        # плейсхолдеры передаются при вызове .execute() словарём
        query = text(
            """
            SELECT
                o.id AS order_id,
                o.created_at,
                SUM(oi.quantity * oi.unit_price) as total_sum
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            WHERE o.user_id = :user_id
            GROUP BY o.id, o.created_at
            ORDER BY o.created_at DESC;
        """
        )
        res = await self.session.execute(query, {"user_id": user_id})
        return res.mappings().all()

    async def orm(self, user_id: int):
        stmt = (
            select(
                Order.id.label("order_id"),
                Order.created_at,
                func.sum(OrderItem.quantity * OrderItem.unit_price).label("total_sum"),
            )
            .join(OrderItem, Order.id == OrderItem.order_id)
            .where(Order.user_id == user_id)
            .group_by(Order.id, Order.created_at)
            .order_by(Order.created_at.desc())
        )

        res = await self.session.execute(stmt)
        rows = res.mappings().all()
        return rows


class Top5SpendingUsers:
    """Вывести топ-5 пользователей по общей сумме всех их заказов.
    Поля для вывода:
    user_id, username, total_spent"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко: получаем все позиции заказов по пользователям, схлопываем по user и считаем сумму + сортировка и лимит

        Сумма заказов - это сумма всех позиций order_items по всем заказам пользователя, значит нужны таблицы:
        users, orders, order_items

        Построение соединенных таблиц начинаем с пользователя, потому что хотим получить пользователей, а не заказы.
        Соединяем их с заказами, потом с позициями

        FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN order_items oi on o.id = oi.order_id

        После этого у нас каждая строка представляет одну позицию конкретного товара
        конкретного заказа конкретного пользователя.

        Чтобы получить одну строку на пользователя, схлопываем, группируя по user_id
        Теперь к группам можно применить агрегатную функцию SUM(oi.quantity * oi.price) as total_spent
        Сортируем и ограничиваем выборку устанавливая лимит
        ORDER BY total_spent DESC
        LIMIT 5
        """
        query = text(
            """
            SELECT
                u.id AS user_id,
                u.username,
                SUM(oi.quantity * oi.unit_price) as total_spent
            FROM users u
            JOIN orders o ON u.id = o.user_id
            JOIN order_items oi ON o.id = oi.order_id
            GROUP BY u.id, u.username
            ORDER BY total_spent DESC
            LIMIT 5;
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        stmt = (
            select(
                User.id.label("user_id"),
                func.sum(OrderItem.quantity * OrderItem.unit_price).label(
                    "total_spent"
                ),
            )
            .join(Order, User.id == Order.user_id)
            .join(OrderItem, Order.id == OrderItem.order_id)
            .group_by(User.id)
            .order_by(desc("total_spent"))
            .limit(5)
        )

        res = await self.session.execute(stmt)
        rows = res.mappings().all()
        return rows


class Top5ProductsSOldByQuantity:
    """
    Вывести топ-5 товаров по общему проданному количеству.
    Вывести: product_id, product_name, total_sold (где total_sold = SUM(order_items.quantity)).
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко: Добавляем к order_items поля из products, группируем по product_id и считаем сумму quantity

        Основная таблица order_items - добавляем к ней данные из products
        FROM products p
        JOIN order_items oi ON p.id = oi.product_id

        Группируем по p.id и p.name чтобы вывести имя товара тоже
        Применяем агрегат SUM(oi.quantity)
        Сортируем и применяем лимит
        """

        # используется LEFT JOIN вместо дефолтного INNER JOIN чтобы товары с нулевыми продажами попали в выборку
        # COALESCE возвращает первое не Null значение из списка - тут оно заменяет Null на 0 когда не было продаж
        query = text(
            """
            SELECT
                p.id AS product_id,
                p.name AS product_name,
                COALESCE(SUM(oi.quantity), 0) AS total_sold
            FROM products p
            LEFT JOIN order_items oi ON p.id = oi.product_id
            GROUP BY p.id, p.name
            ORDER BY total_sold DESC
            LIMIT 5;
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        stmt = (
            select(
                Product.id.label("product_id"),
                Product.name.label("product_name"),
                func.coalesce(func.sum(OrderItem.quantity), 0).label("total_sold"),
            )
            .outerjoin(OrderItem, Product.id == OrderItem.product_id)
            .group_by(Product.id, Product.name)
            .order_by(desc("total_sold"))
            .limit(5)
        )

        res = await self.session.execute(stmt)
        rows = res.mappings().all()
        return rows


class Template:
    """ """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко:

        """
        query = text(
            """
            SELECT
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        stmt = ()

        res = await self.session.execute(stmt)
        rows = res.mappings().all()
        return rows

"""Описание задач уровня 4

Задачи этого уровня направлены на освоение более сложных аналитических запросов,
включающих подзапросы, коррелированные подзапросы и оконные функции.

Главная цель — научиться работать не просто с объединёнными таблицами,
а с производными выборками (результатами других запросов) и вычислять значения
на их основе. Такие приёмы применяются при анализе данных и в сложных отчётах.

---

Основные цели:
 - Освоить подзапросы в SELECT, FROM и WHERE, включая коррелированные подзапросы.
 - Понять, как агрегированные данные использовать в фильтрации или сравнении.
 - Научиться применять оконные функции (AVG OVER, RANK, ROW_NUMBER и др.).
 - Разобраться, как комбинировать агрегаты и оконные выражения.
 - Научиться выделять промежуточные вычисления в подзапросы для читаемости.

---

Инструменты и приёмы:
 - Подзапросы (Subquery, CTE, коррелированные подзапросы).
 - Оконные функции: ROW_NUMBER(), RANK(), DENSE_RANK(), AVG() OVER (...), SUM() OVER (...).
 - HAVING — фильтрация по агрегатам после группировки.
 - WITH (CTE) — создание временных таблиц для многошаговых вычислений.
 - func.over() — вызов оконных функций через SQLAlchemy ORM.
 - select().subquery() — вложенные выборки в ORM.

---

Типичные кейсы уровня 4:
 - Вывести товары, цена которых выше среднего по их категории.
 - Найти категории с наибольшим средним чеком.
 - Определить, какие заказы больше медианного значения.
 - Вывести топ-N товаров с оконной функцией RANK() вместо LIMIT.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import text, select, asc, desc, func, distinct

from models import Product, Category, OrderItem, ProductCategory, Order, User


class ProductsWithPriceHigherThanAVGinCategoryCorrelatedSubQuery:
    """
    Вывести товары, цена которых выше средней по их категории.
    Поля в результате: product_id, product_name, price, category_id, avg_price_in_category.
    (Если товар принадлежит нескольким категориям — он может появиться несколько раз, по каждой категории.)
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко: Для строк товара+категории получаем avg_price_in_category и выводим если price > avg_price_in_category

        Нам нужно знать цену товара (products.price) и среднюю цену в его категории
        — значит, нужно связать товары с категориями.
        Понадобятся таблицы products и products_categories (N:N)

        FROM products p
        JOIN products_categories pc ON p.id = pc.product_id

        После JOIN каждая строка содержит данные по товару и категории
        Если товар в нескольких категориях — он будет в нескольких строках.

        Коррелированный подзапрос: для каждой строки (товар+категория) вычислить AVG(price)
        по товарам той же категории и сравнить цену товара с этим значением.


        Подзапрос SELECT AVG(p2.price) ... WHERE pc2.category_id = pc.category_id
        вычисляет среднюю цену именно для той категории, которая связана с внешней строкой (pc.category_id)
        WHERE сравнивает цену товара с этим средним и оставляет только те, что больше


        Подробно что тут происходит
         - делается JOIN товаров и категорий
         - для каждой строчки получившейся таблицы запускается подзапрос, который делает свой JOIN товаров и категорий
          - фильтром по pc2.category_id = pc.category_id выбирает все товары, что и текущая ВНЕШНЯЯ СТРОКА
          - AVG(p2.price) вычисляет среднее по этой группе товаров (выбранная предыдущем фильтром группа) - число
         - Тут мы получили итоговую таблицу, в которой посчитано поле avg_price_in_category,
         но так как WHERE - динамический фильтр, он не сохраняет значение, вычисленное в SELECT,
         поэтому подзапрос пишется дважды - в SELECT для отображения - в WHERE для фильтрации
        """
        query = text(
            """
            SELECT
                p.id AS product_id,
                p.name AS product_name,
                p.price,
                pc.category_id,
                (
                    SELECT AVG(p2.price)
                    FROM products p2
                    JOIN products_categories pc2 ON p2.id = pc2.product_id
                    WHERE pc2.category_id = pc.category_id
                ) AS avg_price_in_category
            FROM products p
            JOIN products_categories pc ON p.id = pc.product_id
            WHERE p.price > (
                SELECT AVG(p2.price)
                FROM products p2
                JOIN products_categories pc2 ON p2.id = pc2.product_id
                WHERE pc2.category_id = pc.category_id
            )
            ORDER BY pc.category_id, p.price DESC;
        """
        )

        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        pass


class ProductsWithPriceHigherThanAVGinCategoryWindowFunc:
    """
    Вывести товары, цена которых выше средней по их категории.
    Поля в результате: product_id, product_name, price, category_id, avg_price_in_category.
    Если товар принадлежит нескольким категориям — он появляется по каждой категории отдельно.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко: Оконная функция считает среднюю цену категории для каждой строчки товара в CTE,
        после чего внешний SELECT фильтрует и сортирует товары по уже посчитанному полю через WHERE

        Здесь используется оконная функция в промежуточной таблице, созданной через CTE,
        где для каждой строки товара добавляем столбец с вычисленной средней ценой его категории через оконную функцию:

        Оконная функция AVG(p.price) OVER (PARTITION BY pc.category_id) считает среднее по каждой категории
        и добавляет это значение как отдельный столбец к каждой строке товара

        В итоге получаем таблицу с посчитанной средней ценой категории для каждого товара в отдельном столбце

        Главный (внешний) SELECT берёт нужные столбцы и фильтрует товары по условию price > avg_price_in_category,
        затем сортирует по category_id и цене по убыванию
        """
        query = text(
            """
            WITH product_with_avg AS (
                SELECT
                    p.id AS product_id,
                    p.name AS product_name,
                    p.price,
                    pc.category_id,
                    AVG(p.price) OVER (PARTITION BY pc.category_id) AS avg_price_in_category
                FROM products p
                JOIN products_categories pc ON p.id = pc.product_id
            )
            
            SELECT
                product_id,
                product_name,
                price,
                category_id,
                avg_price_in_category
            FROM product_with_avg
            WHERE price > avg_price_in_category
            ORDER BY category_id, price DESC;
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        product_avg_subq = (
            select(
                Product.id.label("product_id"),
                Product.name.label("product_name"),
                Product.price.label("price"),
                ProductCategory.category_id.label("category_id"),
                func.avg(Product.price)
                .over(partition_by=ProductCategory.category_id)
                .label("avg_price_in_category"),
            ).join(ProductCategory, Product.id == ProductCategory.product_id)
        ).subquery("product_with_avg")

        # 2) внешний запрос — фильтрация по рассчитанному avg и вывод нужных полей
        stmt = (
            select(
                product_avg_subq.c.product_id,
                product_avg_subq.c.product_name,
                product_avg_subq.c.price,
                product_avg_subq.c.category_id,
                product_avg_subq.c.avg_price_in_category,
            )
            .where(product_avg_subq.c.price > product_avg_subq.c.avg_price_in_category)
            .order_by(product_avg_subq.c.category_id, product_avg_subq.c.price.desc())
        )

        res = await self.session.execute(stmt)
        rows = res.mappings().all()
        return rows


class CategoriesWithHighestAverageCheck:
    """
    Найти категории с наибольшим средним чеком.
    Вывести топ 10 category_id, category_name, avg_check_for_category
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко:  Для каждой категории находим все заказы, где встречались товары этой категории,
        берём сумму заказа (чек), считаем среднее по всем таким заказам, сортируем по убыванию.

        Связь между категориями и заказами идёт через products → order_items → orders.

        Один заказ может содержать несколько товаров разных категорий, поэтому
        — нужно учитывать заказ в категории только один раз.

        Для этого сначала создадим таблицу (через CTE), где каждая категория сопоставлена с уникальным order_id.

        Затем присоединим таблицу заказов и возьмём AVG(total_sum) по каждой категории.

        Отсортируем по среднему чеку.

         - CTE category_orders
        Формирует таблицу с уникальными парами (category_id, order_id) —
        то есть "какая категория в каких заказах встречалась".
        DISTINCT нужен, чтобы если в заказе несколько товаров из одной категории,
        заказ не учитывался несколько раз.

         - Основной SELECT
        Присоединяет orders по order_id, берёт o.total_amount,
        и считает AVG() по каждой категории.

         - GROUP BY
        Группировка по категории, чтобы агрегировать средний чек.

         - ORDER BY
        Сортировка по среднему чеку в порядке убывания, чтобы увидеть “топ дорогих категорий”.
        """
        query = text(
            """
            WITH category_orders AS (
                SELECT DISTINCT
                    pc.category_id,
                    oi.order_id
                FROM products_categories pc
                JOIN products p ON p.id = pc.product_id
                JOIN order_items oi ON oi.product_id = p.id
            )
            SELECT
                c.id AS category_id,
                c.name AS category_name,
                AVG(o.total_amount) AS avg_check_for_category
            FROM category_orders co
            JOIN orders o ON co.order_id = o.id
            JOIN categories c ON co.category_id = c.id
            GROUP BY c.id, c.name
            ORDER BY avg_order_total_for_category DESC
            LIMIT 10;
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        # 1. CTE с уникальными (category_id, order_id)
        category_orders_cte = (
            select(
                distinct(ProductCategory.category_id),
                OrderItem.order_id,
            )
            .join(Product, Product.id == ProductCategory.product_id)
            .join(OrderItem, OrderItem.product_id == Product.id)
            .cte("category_orders")
        )
        # 2. Основной запрос
        query = (
            select(
                Category.id.label("category_id"),
                Category.name.label("category_name"),
                func.avg(Order.total_amount).label("avg_order_total_for_category"),
            )
            .join(category_orders_cte, category_orders_cte.c.category_id == Category.id)
            .join(Order, category_orders_cte.c.order_id == Order.id)
            .group_by(Category.id, Category.name)
            .order_by(func.avg(Order.total_amount).desc())
        )

        res = await self.session.execute(query)
        rows = res.mappings().all()
        return rows


class OrdersHigherThanMedian:
    """
    Определить, какие заказы больше медианного значения.
    Вывести:
    order_id, total_sum — только те заказы, сумма которых выше медианы всех заказов.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко:
        Сначала нужно вычислить медиану всех total_sum из orders.
        Для этого используем оконную функцию PERCENTILE_CONT(0.5) —
        она вычисляет значение, находящееся посередине отсортированных значений.
        После этого можно выбрать заказы, у которых total_sum выше этой медианы.

        Оконная функция PERCENTILE_CONT(0.5)
        PERCENTILE_CONT — непрерывная функция для вычисления процентиля (в данном случае 0.5 → медиана).
        WITHIN GROUP (ORDER BY total_sum) сортирует orders по total_sum и берет значение посередине.
        OVER () без PARTITION — значит, считаем медиану по всей таблице целиком.

        CTE order_with_median
        Создаёт временную таблицу, где каждой строке (order_id, total_sum) добавлено поле median_total — одно и то же
        значение для всех строк (так как окно без PARTITION).

        То есть на выходе получаем структуру вида:
        order_id	total_sum	median_total
        1	            350	        410
        2	            520	        410
        3	            410	        410
        4	            600	        410

        Основной SELECT
        Фильтруем через WHERE total_sum > median_total —
        оставляем только заказы выше медианы.
        Сортируем по total_sum DESC, чтобы видеть самые дорогие первыми.
        """
        query = text(
            """
            WITH order_with_median AS (
                SELECT
                    id AS order_id,
                    total_sum,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) OVER () AS median_total
                FROM orders
            )
            SELECT
                order_id,
                total_amount,
                median_total
            FROM order_with_median
            WHERE total_sum > median_total
            ORDER BY total_sum DESC;
        """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        order_with_median = select(
            Order.id.label("order_id"),
            Order.total_amount,
            func.percentile_cont(0.5)
            .within_group(Order.total_amount)
            .over()
            .label("median_total"),
        ).cte("order_with_median")

        query = (
            select(
                order_with_median.c.order_id,
                order_with_median.c.total_amount,
                order_with_median.c.median_total,
            )
            .where(order_with_median.c.total_amount > order_with_median.c.median_total)
            .order_by(order_with_median.c.total_amount.desc())
        )

        res = await self.session.execute(query)
        rows = res.mappings().all()
        return rows


class TopFrequentProductsViaWindowRank:
    """
    Вывести топ-N товаров, которые чаще всего встречаются в заказах, используя оконную функцию RANK().
    Поля в результате:
    product_id, product_name, order_count, rank.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self, number: int = 5):
        """Мышление-решение
        Кратко:
        Сначала посчитаем, сколько раз каждый товар встречался в заказах (через JOIN + GROUP BY),
        затем присвоим каждому товару ранг (RANK()) на основе этого количества.
        После этого выберем строки, где rank <= N.

        Таким образом, RANK() выполняет ту же задачу, что и ORDER BY + LIMIT,
        но с преимуществом — если несколько товаров делят одно место (например, 3-е),
        они получат одинаковый ранг, а следующий начнётся с пропуском (4-е → 6-е место и т.д.).

        JOIN
        Соединяем products и order_items, чтобы получить таблицу, где каждая строка — это вхождение товара в заказ.
        GROUP BY
        Считаем количество вхождений COUNT(oi.id) для каждого товара.

        Получаем таблицу:
        product_id	product_name	order_count
        1	            Monitor	        15
        2	            Keyboard	    10
        3	            Mouse	        10
        4	            Headphones	    8

        RANK()
        Применяем оконную функцию:
        RANK() OVER (ORDER BY COUNT(oi.id) DESC)

        Она проходит по результатам, отсортированным по количеству заказов,
        и присваивает позиции (ранги).

        При одинаковых значениях order_count ранги одинаковые.
        product_id	order_count	rank
        1	            15	      1
        2	            10	      2
        3	            10	      2
        4	            8	      4

        CTE product_ranked
        Используется CTE, чтобы результат RANK() можно было фильтровать через WHERE rank <= :N.
        Основной SELECT
        Фильтруем только верхние строки (например, rank <= 5) и сортируем по рангу.
        """
        query = text(
            """
            WITH product_ranked AS (
                SELECT
                    p.id AS product_id,
                    p.name AS product_name,
                    COUNT(oi.id) AS order_count,
                    RANK() OVER (ORDER BY COUNT(oi.id) DESC) AS rank
                FROM products p
                JOIN order_items oi ON p.id = oi.product_id
                GROUP BY p.id, p.name
            )
            SELECT
                product_id,
                product_name,
                order_count,
                rank
            FROM product_ranked
            WHERE rank <= :N
            ORDER BY rank;
        """
        )
        res = await self.session.execute(query, {"N": number})
        return res.mappings().all()

    async def orm(self, number: int = 5):
        product_ranked = (
            select(
                Product.id.label("product_id"),
                Product.name.label("product_name"),
                func.count(OrderItem.id).label("order_count"),
                func.rank()
                .over(order_by=func.count(OrderItem.id).desc())
                .label("rank"),
            )
            .join(OrderItem, Product.id == OrderItem.product_id)
            .group_by(Product.id, Product.name)
            .cte("product_ranked")
        )

        query = (
            select(
                product_ranked.c.product_id,
                product_ranked.c.product_name,
                product_ranked.c.order_count,
                product_ranked.c.rank,
            )
            .where(product_ranked.c.rank <= number)
            .order_by(product_ranked.c.rank)
        )

        res = await self.session.execute(query)
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

"""Описание задач уровня 5

Задачи этого уровня направлены на освоение продвинутой аналитики и оптимизации запросов.
Основное внимание уделяется многошаговым вычислениям, сложным оконным функциям,
анализу динамики данных и выявлению закономерностей.
Цель — научиться строить запросы, которые решают реальные аналитические задачи
и позволяют находить скрытые зависимости в данных.

---

Основные цели:
 - Научиться использовать несколько оконных функций в одном запросе.
 - Понять разницу между PARTITION BY и ORDER BY в оконных выражениях.
 - Освоить многоуровневые подзапросы и комбинации CTE.
 - Использовать оконные агрегаты для анализа тенденций, ранжирования и отклонений.
 - Применять статистические функции (STDDEV, PERCENTILE_CONT, COVAR_POP и др.).
 - Оптимизировать сложные запросы и повышать читаемость с помощью WITH.

---

Инструменты и приёмы:
 - Сложные CTE (многоступенчатые вычисления).
 - Комбинации оконных функций с агрегатами и фильтрами.
 - Статистические и аналитические функции (медиана, стандартное отклонение, процентиль).
 - Самообъединения (self join) и сравнение записей "предыдущая–текущая".
 - Коррелированные подзапросы для персонализированных метрик.
 - Временные аналитические функции (LAG, LEAD, FIRST_VALUE, LAST_VALUE).
 - Оптимизация фильтров и подзапросов (фильтрация до агрегации).

---

Типичные кейсы уровня 5:
 - Найти категории, медианная цена товаров в которых выше общей медианы по всем товарам.
 - Определить заказы, сумма которых превышает среднее значение для данного пользователя.
 - Вывести топ-3 товара в каждой категории по количеству продаж с использованием RANK().
 - Вычислить разницу продаж по месяцам и отобрать товары с падением > 30%.
 - Определить пользователей, которые совершали покупки каждый из последних 6 месяцев.
 - Найти пары товаров, которые чаще всего покупаются вместе.
 - Определить аномальные заказы (сумма > 2 стандартных отклонений от среднего).
 - Для каждого пользователя вычислить время между первым и последним заказом.
 - Посчитать кумулятивную выручку по дням с начала месяца.
 - Найти пользователей, купивших один и тот же товар более одного раза.

---
"""

from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import text, select, asc, desc, func, distinct

from models import Product, Category, OrderItem, ProductCategory, Order, User


class Top3ProductsPerCategory:
    """
    Вывести топ-3 товара в каждой категории по количеству продаж.
    Вывести:
    category_id, product_id, total_sales, rank
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко:
        Считаем, сколько раз каждый товар продавался в каждой категории,
        затем используем оконную функцию RANK() OVER (PARTITION BY category_id ORDER BY total_sales DESC),
        чтобы присвоить место каждому товару внутри своей категории.
        После этого фильтруем по rank <= 3.

        ---
        Шаги мышления:
        1. Из таблицы order_items (или аналогичной) собираем количество продаж каждого товара.
           Это делаем через GROUP BY category_id, product_id.
        2. В CTE добавляем оконную функцию RANK(), которая нумерует товары внутри каждой категории
           по убыванию количества продаж.
        3. Во внешнем SELECT фильтруем только те строки, где rank <= 3 — это и есть топ-3.
        4. Сортируем для наглядности по category_id, rank.

        ---
        Оконная функция RANK() OVER (PARTITION BY category_id ORDER BY total_sales DESC)
        PARTITION BY — делит выборку на группы по категориям.
        ORDER BY — внутри каждой группы сортирует товары по количеству продаж.
        RANK() — присваивает ранг с пропусками (например, 1, 2, 2, 4).
        Если нужно без пропусков — DENSE_RANK().

        Ключевые концепты:
        RANK() OVER (PARTITION BY ... ORDER BY ...) — ранжирование внутри группы.
        GROUP BY перед оконной функцией нужен, чтобы агрегировать продажи.
        CTE (WITH) помогает сделать запрос читаемым: сначала считаем агрегаты, потом фильтруем по рангу.
        В ORM используем func.rank().over() — это прямая аналогия SQL RANK().
        """
        query = text(
            """
            WITH product_sales AS (
                SELECT
                    pc.category_id,
                    oi.product_id,
                    COUNT(*) AS total_sales,
                    RANK() OVER (
                        PARTITION BY pc.category_id
                        ORDER BY COUNT(*) DESC
                    ) AS rank
                FROM order_items oi
                JOIN products_categories pc ON oi.product_id = pc.product_id
                GROUP BY pc.category_id, oi.product_id
            )
            SELECT
                category_id,
                product_id,
                total_sales,
                rank
            FROM product_sales
            WHERE rank <= 3
            ORDER BY category_id, rank;
            """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        """Реализация через SQLAlchemy ORM"""
        # Cчитаем количество продаж каждого товара в каждой категории
        product_sales = (
            select(
                ProductCategory.category_id,
                OrderItem.product_id,
                func.count().label("total_sales"),
                func.rank()
                .over(
                    partition_by=ProductCategory.category_id,
                    order_by=func.count().desc(),
                )
                .label("rank"),
            )
            .join(ProductCategory, ProductCategory.product_id == OrderItem.product_id)
            .group_by(ProductCategory.category_id, OrderItem.product_id)
            .cte("product_sales")
        )

        # Фильтрация топ-3
        query = (
            select(
                product_sales.c.category_id,
                product_sales.c.product_id,
                product_sales.c.total_sales,
                product_sales.c.rank,
            )
            .where(product_sales.c.rank <= 3)
            .order_by(product_sales.c.category_id, product_sales.c.rank)
        )

        res = await self.session.execute(query)
        rows = res.mappings().all()
        return rows


class UsersWithRepeatedPurchases:
    """
    Найти пользователей, купивших один и тот же товар более одного раза.
    Вывести:
    user_id, product_id, purchase_count — только те строки, где один пользователь купил один товар более 1 раза.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def raw_sql(self):
        """Мышление-решение
        Кратко:
        Группируем покупки по user_id и product_id, считаем количество раз, когда один и тот же товар был куплен.
        Оставляем только те комбинации, где COUNT(*) > 1.

        ---
        Шаги мышления:
        1. Из таблицы order_items (или аналогичной) достаём user_id, product_id.
           Для этого соединяем order_items с orders, чтобы получить user_id.
        2. Группируем по (user_id, product_id).
        3. Считаем количество покупок через COUNT(*).
        4. Фильтруем через HAVING COUNT(*) > 1 — только повторные покупки.
        5. Сортируем для читаемости по user_id, product_id.

        ---
        HAVING
        Используется вместо WHERE, потому что фильтрация происходит *после* группировки.
        WHERE фильтрует отдельные строки, а HAVING — агрегированные группы.

        Ключевые концепты:
        GROUP BY (user_id, product_id) — формирует пары "пользователь–товар".
        COUNT(*) — подсчёт количества покупок конкретного товара одним пользователем.
        HAVING COUNT(*) > 1 — фильтрует только повторяющиеся покупки.
        JOIN orders — нужен, чтобы из order_items получить user_id.
        Этот запрос часто применяется при анализе лояльности и паттернов повторных покупок.

        """
        query = text(
            """
            SELECT
                o.user_id,
                oi.product_id,
                COUNT(*) AS purchase_count
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.id
            GROUP BY o.user_id, oi.product_id
            HAVING COUNT(*) > 1
            ORDER BY o.user_id, oi.product_id;
            """
        )
        res = await self.session.execute(query)
        return res.mappings().all()

    async def orm(self):
        """Реализация через SQLAlchemy ORM"""
        query = (
            select(
                Order.user_id,
                OrderItem.product_id,
                func.count().label("purchase_count"),
            )
            .join(Order, OrderItem.order_id == Order.id)
            .group_by(Order.user_id, OrderItem.product_id)
            .having(func.count() > 1)
            .order_by(Order.user_id, OrderItem.product_id)
        )

        res = await self.session.execute(query)
        rows = res.mappings().all()
        return rows

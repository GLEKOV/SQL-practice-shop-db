from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from main_app.db_helper import db_helper
import asyncio
from sqlalchemy import text, select, asc, desc, func

from models import Product, Category


class SQL_Practice:
    """Сессии будут получаться через Dependency Injection на уровне класса в __init__"""

    class _Level_1:
        """Внутренний класс для методов задачи первого уровня

        Задача:
        Вывести все товары из таблицы products, отсортированные по возрастанию цены.
        Добавить ограничение — вывести только первые 10 товаров.
        """

        def __init__(self, session: AsyncSession):
            self.session = session

        async def raw_sql(self):
            query = text(
                """
            SELECT *
            FROM products
            ORDER BY price ASC
            LIMIT 10
            """
            )
            top_10_cheap_products = await self.session.execute(query)

            # region Result hints
            # в переменной лежит обёртка над данными - объект Result - и данные из него еще нужно достать
            """
            all(): Returns a list of all result rows. Each row can be a Row object (for column-based queries) or an ORM entity (for ORM-based queries).
            
            first(): Returns the first result row, or None if no rows are found.
            
            one(): Returns the first result row, and raises an exception if no rows or more than one row are found.
            
            one_or_none(): Returns the first result row, None if no rows are found, or raises an exception if more than one row is found.
            
            scalars(): Returns a ScalarResult object, which is an iterable of the primary scalar elements of each row (e.g., ORM entities when querying for User objects). This is particularly useful when you are selecting a single ORM entity or a single column per row.
            
            scalars().all(): Returns a list of all scalar results.
            
            mappings(): Returns a MappingResult object, which is an iterable of RowMapping objects. Each RowMapping behaves like a dictionary, allowing access to column values by name.
            
            mappings().all(): Returns a list of all RowMapping objects.
            
            fetchmany(size=None): Returns a list of up to size result rows. If size is None, a default number of rows is returned.
            
            fetchone(): Returns the next result row, or None if no more rows are available.
            
            yield_per(count): Configures the result to fetch rows in batches of count during iteration, useful for handling large result sets efficiently.
            """
            # endregion
            return top_10_cheap_products.mappings().all()  # каждая строка как dict

        async def orm(self):
            # по умолчанию сортировка идет по возрастанию - asc - по убыванию нужно .order_by(desc(Model.field))
            stmt = select(Product).order_by(asc(Product.price)).limit(10)
            res = await self.session.execute(stmt)
            return res.scalars().all()

    class _Level_2:
        """Внутренний класс для методов задачи второго уровня

        Задача:
        Вывести все товары из категории с category_id = 2, у которых цена между 50 и 200 единиц.
        Отсортировать по цене по убыванию.
        """

        def __init__(self, session: AsyncSession):
            self.session = session

        async def raw_sql(self):
            query = text(
                """
            SELECT *
            FROM products p
            JOIN products_categories pc ON p.id = pc.product_id
            WHERE pc.category_id = 2 AND p.price BETWEEN 50 AND 200
            ORDER BY p.price DESC
            """
            )
            res = await self.session.execute(query)
            return res.mappings().all()

        async def orm(self):
            stmt = (
                select(Product)
                .where(
                    Product.categories.any(Category.id == 2),
                    Product.price.between(50, 200),
                )
                .order_by(desc(Product.price))
                .options(selectinload(Product.categories))
            )
            res = await self.session.execute(stmt)
            return res.scalars().all()

    class _Level_3:
        """Внутренний класс для level_3 методов

        Задача:
        Вывести для каждой категории (category_id) количество товаров и среднюю цену товаров.
        Отсортировать по количеству товаров по убыванию.

        Таблицы: products, products_categories

        Использовать агрегаты COUNT и AVG

        Сортировка по количеству товаров
        """

        def __init__(self, session: AsyncSession):
            self.session = session

        async def raw_sql(self):
            """
            Solution explanation:
            Создаем таблицу с нужными нам данными - ID категории и данным по товарам в этой категории через JOIN
            Так как данные нужно вывести для каждой категории - по ним и группируем GROUP BY category_id
            Внутри групп по категории считаем:
            количество продуктов в них: | COUNT(*) AS product_count | (COUNT(*) - считает кол-во строк в группе)
            среднюю цену внутри такой группы (категории товаров) | AVG(price) as avg_price
            """
            query = text(
                """
            SELECT category_id, COUNT(*) AS product_count, AVG(price) as avg_price
            FROM products_categories pc
            JOIN products p ON pc.product_id = p.id
            GROUP BY category_id
            ORDER BY product_count DESC
            """
            )
            res = await self.session.execute(query)
            return res.mappings().all()

        async def orm(self):
            stmt = (
                select(
                    Category.id.label("category_id"),
                    func.count(Product.id).label("product_count"),
                    func.avg(Product.price).label("avg_price"),
                )
                .join(Category.products)
                .group_by(Category.id)
                .order_by(desc(func.count(Product.id)))
            )
            res = await self.session.execute(stmt)
            # ВАЖНО! - когда в SELECT мы выбираем отдельные поля и поля-агрегаты мы получим список кортежей, как в raw
            # Поэтому работать надо как со списком кортежей строк - в данном случае из кортежей делаем словари
            return res.mappings().all()

    def __init__(self, session: AsyncSession):
        """SQL_Practice class __init__
        Делаем атрибутами созданные экземпляры внутренних классов, чтобы можно было обращаться к их методам
        """
        self._session = session
        self.level_1 = self._Level_1(session)
        self.level_2 = self._Level_2(session)
        self.level_3 = self._Level_3(session)


async def main():
    # через async for достается Async session из генератора сессий
    async for session in db_helper.session_getter():
        sol = SQL_Practice(session)

        # level_1_________________________________________________________________
        print(
            "\n",
            "level 1____________________________________________________________",
            "\n",
        )
        products_raw_sql = await sol.level_1.raw_sql()
        for row in products_raw_sql:
            print(row["id"], row["name"], row["price"])

        print("\n \n")

        products_orm = await sol.level_1.orm()
        for row in products_orm:
            print(row.id, row.name, row.price)

        # level_2_________________________________________________________________
        print(
            "\n",
            "level 2____________________________________________________________",
            "\n",
        )
        l2rs = await sol.level_2.raw_sql()
        for row in l2rs:
            print("rawsql", row["id"], row["name"], row["price"])
        print("\n")

        l2orm = await sol.level_2.orm()
        for row in l2orm:
            print("orm   ", row.id, row.name, row.price)

        # level_3_________________________________________________________________
        print(
            "\n",
            "level 3____________________________________________________________",
            "\n",
        )
        l3rs = await sol.level_3.raw_sql()
        for row in l3rs:
            print(
                "rawsql",
                row["category_id"],
                row["product_count"],
                round(row["avg_price"], 2),
            )
        print("\n")

        l3orm = await sol.level_3.orm()
        for row in l3orm:
            print(
                "orm   ",
                row["category_id"],
                row["product_count"],
                round(row["avg_price"], 2),
            )


if __name__ == "__main__":
    asyncio.run(main())

# region Theory

# ORM

# res.scalars().all() - чтобы получить список объектов ORM
#                       (scalars() - снимает обёртку кортежем) - объект ORM [Product(...), Product(...)]
# res.scalar()        - чтобы получить первый объект запроса ORM - объект ORM

# res.all() - даёт список строк-кортежей из одного объекта [(Product(...),), (Product(...), ...)]
# res.first() - даёт кортеж первой строки (Product(...),)
# res.one() - то же что и выше, только кинет ошибку если количество строк в res 0 или больше одной
# res.one_or_none() - то же что и выше - но без ошибки на пустой результат


# SQL

# res.all() - вернет все строки - каждая объект Row = [Row(id=1,name='Apple',price=10), ...]
# res.first() - вернет первую строку или None
# res.one() - вернет ровно одну строку или ошибка
# res.one_or_none() - то же но без ошибки на пустой результат
# res.scalar() - первое значение первой строки (например id)
# res.scalars().all() - список значений первого столбца - например список id = [1,2,3]
# res.mappings().all() - вернет список строк в виде словарей = [{'id':1,'name':'Apple','price':10}, ...]
# res.fetchone() - извлекает одну строку = Row() со смещением указателя (можно забирать данные из res по одной строке)
# res.fetchmany(size=3) - извлекает список строк со смещением указателя, чтобы брать данные батчами

# endregion

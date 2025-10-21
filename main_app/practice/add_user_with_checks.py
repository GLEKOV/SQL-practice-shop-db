from main_app.models.fake_data_gen.fake_data import FakeData
from main_app.db_helper import db_helper
from models import User

from sqlalchemy import select
import asyncio

fake_data = FakeData()


async def check_user_exists(user: User) -> bool:
    """Checks if user exists by email in DB"""
    async with db_helper.session_factory() as session:
        email_exists = await session.scalar(
            select(User).where(User.email == user.email)
        )
        if email_exists:
            print(f"User with email {user.email} already exists!")
            return True
    return False


async def add_user(user: User):
    """Check if a user with the given email exists in the database.
    Add the user if the email is not found."""
    if not await check_user_exists(user):
        async with db_helper.session_factory() as session:
            session.add(user)
            await session.commit()
            print(f"added user: {user}")


if __name__ == "__main__":
    asyncio.run(add_user(fake_data.create_fake_users(how_many=1)[0]))

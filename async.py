import requests
import datetime
from aiohttp import ClientSession
import asyncio
import more_itertools
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String

PG_DSN = 'postgresql+asyncpg://app:1234@127.0.0.1:5431/star_wars'
engine = create_async_engine(PG_DSN)
Base = declarative_base(bind=engine)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

CHUNK_SIZE = 10

EXCLUDE_FIELDS = ['created', 'edited', 'url', 'detail']


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String, unique=True)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


async def insert(results):
    async with Session() as db_session:
        people = [People(**result) for result in results]
        try:
            db_session.add_all(people)
            await db_session.commit()
        except IntegrityError:
            print('Some or all chunk characters already exist')


def get_total_people():
    people = requests.get('https://swapi.dev/api/people/').json()
    return people['count']


async def get_name(link, session):
    async with session.get(link) as response:
        card = await response.json()
        if 'name' in card:
            name = card['name']
        elif 'title' in card:
            name = card['title']
    return name


async def get_people(people_id, session):
    db_card = {}
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        full_card = await response.json()
        for key, items in full_card.items():
            if key not in EXCLUDE_FIELDS:
                if type(items) is list:
                    names = (get_name(items[i], session) for i in range(0, len(items)))
                    results = await asyncio.gather(*names)
                    db_card[key] = ', '.join(results)
                elif key == 'homeworld':
                    db_card[key] = await get_name(items, session)
                else:
                    db_card[key] = items
        return db_card


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    db_tasks = []
    async with ClientSession() as http_session:
        people_count = get_total_people()
        coros = (get_people(i, http_session) for i in range(1, people_count+1))
        for coros_chunk in more_itertools.chunked(coros, CHUNK_SIZE):
            results = await asyncio.gather(*coros_chunk)
            db_tasks.append(asyncio.create_task(insert(results)))
    for task in db_tasks:
        await task


# Работая на Win получал RuntimeError - решение ниже помогло избавиться от этого
async def worker():
    start = datetime.datetime.now()
    await main()
    print(datetime.datetime.now() - start)

asyncio.get_event_loop().run_until_complete(worker())

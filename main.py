import asyncio
import datetime
from pprint import pprint
from aiohttp import ClientSession
from more_itertools import chunked
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import Column, Integer, ARRAY, String
from sqlalchemy.ext.declarative import declarative_base

CHUNK_SIZE = 10


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


PG_DSN = DSN = 'postgresql+asyncpg://app:secret@localhost:5431/app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String(10))  # because can be 'unknown'
    eye_color = Column(String(100))
    films = Column(ARRAY(String))  # строка с названиями фильмов через запятую
    gender = Column(String(10))
    hair_color = Column(String(100))
    height = Column(String(10))  # because can be 'unknown'
    homeworld = Column(String(254))
    mass = Column(String(10))  # because can be 'unknown'
    name = Column(String(100))
    skin_color = Column(String(50))
    species = Column(ARRAY(String))  # строка с названиями типов через запятую
    starships = Column(ARRAY(String))  # строка с названиями кораблей через запятую
    vehicles = Column(ARRAY(String))  # строка с названиями транспорта через запятую


async def get_person(people_id: int, session: ClientSession) -> dict:
    print(f'start {people_id}')

    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        response_data = await response

    print(f'end {people_id}')

    return {'people_id': people_id,
            'json': response_data.json(),
            'status': response_data.status_code}


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 80), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)

            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(id=item.get('people_id'),
                                birth_year=item.get('json').get('birth_year'),
                                eye_color=item.get('json').get('birth_year'),
                                films=item.get('json').get('birth_year'),
                                gender=item.get('json').get('birth_year'),
                                hair_color=item.get('json').get('birth_year'),
                                height=item.get('json').get('birth_year'),
                                homeworld=item.get('json').get('birth_year'),
                                mass=item.get('json').get('birth_year'),
                                name=item.get('json').get('birth_year'),
                                skin_color=item.get('json').get('birth_year'),
                                species=item.get('json').get('birth_year'),
                                starships=item.get('json').get('birth_year'),
                                vehicles=item.get('json').get('birth_year'),
                                )
                         for item in people_chunk])
        await session.commit()


async def main():
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await connection.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        task = asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task_ in tasks:
        await task_


start = datetime.datetime.now()
asyncio.run(main())
print(f'Working time: {datetime.datetime.now() - start}')

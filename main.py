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


async def get_person(people_id: int, session: ClientSession) -> dict | str:
    print(f'start {people_id}')

    # mirror https://www.swapi.dev/
    async with session.get(f'https://swapi.tech/api/people/{people_id}') as response:
        json_data = await response.json()

    print(f'end {people_id}')

    # pprint(json_data)
    person_data = {'id': people_id}

    if json_data.get('message') == 'ok':
        result_dic = json_data.get('result').get('properties')

        person_data.update({'birth_year': result_dic.get('birth_year'),
                            'eye_color': result_dic.get('eye_color'),
                            'films': result_dic.get('films'),
                            'gender': result_dic.get('gender'),
                            'hair_color': result_dic.get('hair_color'),
                            'height': result_dic.get('height'),
                            'homeworld': result_dic.get('homeworld'),
                            'mass': result_dic.get('mass'),
                            'name': result_dic.get('name'),
                            'skin_color': result_dic.get('skin_color'),
                            'species': result_dic.get('species'),
                            'starships': result_dic.get('starships'),
                            'vehicles': result_dic.get('vehicles')})

        # pprint(person_data)

    else:
        person_data.update({'birth_year': 'not found',
                            'eye_color': 'not found',
                            'films': 'not found',
                            'gender': 'not found',
                            'hair_color': 'not found',
                            'height': 'not found',
                            'homeworld': 'not found',
                            'mass': 'not found',
                            'name': 'not found',
                            'skin_color': 'not found',
                            'species': 'not found',
                            'starships': 'not found',
                            'vehicles': 'not found'})

    return person_data


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 100), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)

            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(**item) for item in people_chunk])
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

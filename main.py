import asyncio
import datetime
from pprint import pprint
from aiohttp import ClientSession
from more_itertools import chunked
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import Column, Integer, JSON
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
    json = Column(JSON)


async def get_person(people_id: int, session: ClientSession):
    print(f'start {people_id}')

    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()

    print(f'end {people_id}')

    return json_data


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 80), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)

            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(json=item) for item in people_chunk])
        await session.commit()

    ...


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

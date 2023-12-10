from sqlalchemy import text, select, update, insert

from src.database import sync_engine, async_engine
from src.models import metadata_obj, workers_table, ResumeOrm


def get_123():
    with sync_engine.connect() as conn:
        res = conn.execute(text("SELECT 1,2,3 union select 4,5,6"))
        print(f'{res.first()=}')


async def version122():
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT VERSION()"))
        print(f"{res.first()=}")


class SyncCore:
    @staticmethod
    def create_tables():
        sync_engine.echo = False
        metadata_obj.drop_all(sync_engine)
        metadata_obj.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def insert_data():
        with sync_engine.connect() as conn:
            stmt = """INSERT INTO workerss (username) VALUES
            ('BOBr'),
            ('Volk'); """

            conn.execute(text(stmt))
            conn.commit()

    @staticmethod
    def select_workers():
        with sync_engine.connect() as conn:
            query = select(workers_table)
            result = conn.execute(query)
            workers = result.all()
            print(f"{workers = }")

    @staticmethod
    def update_worker(worker_id: int = 2, new_name: str = 'Misha'):
        with sync_engine.connect() as conn:
            stmt = text("UPDATE workerss SET username=:new_name WHERE id=:id")
            stmt = stmt.bindparams(new_name=new_name, id=worker_id)

            stmt2 = (
                update(workers_table)
                .values(username=new_name)
                # .where(workers_table.c.id == worker_id)
                # .filter(workers_table.c.id == worker_id)
                .filter_by(id=worker_id)
            )
            conn.execute(stmt2)
            conn.commit()


class AsyncCore:

    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(metadata_obj.drop_all)
            await conn.run_sync(metadata_obj.create_all)

    @staticmethod
    async def insert_additional_resumes():
        async with async_engine.connect() as conn:
            workers = [
                {"username": "Artem"},  # id 3
                {"username": "Roman"},  # id 4
                {"username": "Petr"},  # id 5
            ]
            resumes = [
                {"title": "Python программист", "compensation": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "compensation": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "compensation": 80000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "compensation": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "compensation": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_workers = insert(workers_table).values(workers)
            insert_resumes = insert(ResumeOrm).values(resumes)
            await conn.execute(insert_workers)
            await conn.execute(insert_resumes)
            await conn.commit()
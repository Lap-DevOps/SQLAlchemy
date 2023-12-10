from sqlalchemy import select, func, cast, Integer, and_
from sqlalchemy.orm import aliased

from src.database import session_factory, sync_engine, async_session_factory, Base
from src.models import WorkersOrm, ResumeOrm


async def async_insert_data():
    async with async_session_factory() as session:
        worker_bobr = WorkersOrm(username='Bobr')
        worker_volk = WorkersOrm(username='Wolk')
        session.add_all([worker_bobr, worker_volk])
        await session.commit()


class SyncORM:
    @staticmethod
    def create_tables():
        sync_engine.echo = True
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def insert_data():
        with session_factory() as session:
            worker_bobr = WorkersOrm(username='Bobr')
            worker_volk = WorkersOrm(username='Wolk')
            session.add_all([worker_bobr, worker_volk])
            session.flush()
            session.commit()

    @staticmethod
    def select_workers():
        with session_factory() as session:
            worker_id = 1
            worker_jack = session.get(WorkersOrm, worker_id)
            query = select(WorkersOrm)
            result = session.execute(query)
            workers = result.scalars().all()
            print(f"{workers = }")

    @staticmethod
    def update_workers(worker_id: int = 1, new_worker_name='Antoxa'):
        with session_factory() as session:
            worker_jack = session.get(WorkersOrm, worker_id)
            worker_jack.username = new_worker_name
            session.expire_all()
            session.commit()

    @staticmethod
    def select_resumes_avr_compensation(like_language: str = "Python"):
        with session_factory() as session:
            query = (
                select(
                    ResumeOrm.workload,
                    cast(func.avg(ResumeOrm.compensation), Integer).label("avg_compensation"),
                )
                .select_from(ResumeOrm)
                .filter(and_(
                    ResumeOrm.title.contains(like_language),
                    ResumeOrm.compensation > 40000,
                ))
                .group_by(ResumeOrm.workload)
                .having(cast(func.avg(ResumeOrm.compensation), Integer) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            result = session.execute(query)
            print(result.all())

    @staticmethod
    def select_workers_with_lazy_relationships():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
            )
        res = session.execute(query)
        result = res.scalars().all()

        worker1 = result[0].resumes


class AsyncORM:
    @staticmethod
    async def join_cte_subquery_window_func():
        """
        WITH helper2 AS (
            SELECT *, compensation-avg_workload_compensation AS compensation_diff
            FROM
            (SELECT
                w.id,
                w.username,
                r.compensation,
                r.workload,
                avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation
            FROM resumes r
            JOIN workers w ON r.worker_id = w.id) helper1
        )
        SELECT * FROM helper2
        ORDER BY compensation_diff DESC;
        """
        async with async_session_factory() as session:
            r = aliased(ResumeOrm)
            w = aliased(WorkersOrm)
            subq = (
                select(
                    r,
                    w,
                    func.avg(r.compensation).over(partition_by=r.workload).cast(Integer).label(
                        "avg_workload_compensation"),
                )
                # .select_from(r)
                .join(r, r.worker_id == w.id).subquery("helper1")
            )
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )

            res = await session.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")

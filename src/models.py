import datetime
import enum
from typing import Annotated

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, text
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base, str_256

metadata_obj = MetaData()

workers_table = Table(
    'workerss',
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('username', String)
)

intpk = Annotated[int, mapped_column(primary_key=True)]
created_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"))]
updated_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"),
                                                        onupdate=datetime.datetime.utcnow)]


class WorkLoad(enum.Enum):
    parttime = 'parttime'
    fulltime = 'fulltime'


class WorkersOrm(Base):
    __tablename__ = 'workers'
    id: Mapped[intpk]
    username: Mapped[str] = mapped_column()


class ResumeOrm(Base):
    __tablename__ = 'resumes'
    id: Mapped[intpk]
    title: Mapped[str_256]
    compensation: Mapped[int | None]
    workload: Mapped[WorkLoad]
    worker_id: Mapped[int] = mapped_column(ForeignKey("workers.id", ondelete='CASCADE'))
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]

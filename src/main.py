import asyncio
import os
import os
import sys

from queries.core import AsyncCore
from queries.orm import AsyncORM

sys.path.insert(1, os.path.join(sys.path[0], ".."))

if __name__ == '__main__':
    # SyncCore.create_tables()
    # SyncCore.insert_data()
    # SyncCore.select_workers()
    # SyncCore.update_worker()
    # SyncORM.select_workers()
    # SyncORM.update_workers()
    # SyncORM.select_resumes_avr_compensation()
    # asyncio.run(AsyncCore.insert_additional_resumes())
    asyncio.run(AsyncORM.join_cte_subquery_window_func())



import asyncio
import os
import sys

from queries.core import SyncCore
from queries.orm import insert_data, create_tables, async_insert_data

sys.path.insert(1, os.path.join(sys.path[0], ".."))

if __name__ == '__main__':
    SyncCore.create_tables()
    SyncCore.insert_data()
    SyncCore.select_workers()
    SyncCore.update_worker()

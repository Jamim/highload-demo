import asyncio
import os
from datetime import datetime

from aiofiles.threadpool import wrap

from .base import BaseCollector


RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', 'raw_data')


class RawDataCollector(BaseCollector):

    output_file = None
    enabled = os.getenv('SAVE_RAW_DATA') == '1'

    def __init__(self, consumer):
        super().__init__(consumer)
        if self.enabled:
            self.path = os.path.join(RAW_DATA_PATH, consumer.consumer_id)
            os.mkdir(self.path)
            self._reset_output()

    def _reset_output(self):
        previous_output_file = self.output_file
        output_path = os.path.join(self.path, datetime.utcnow().isoformat())

        # using synchronous open to avoid race condition
        self.output_file = wrap(open(output_path, mode='bw'), loop=self.loop)

        if previous_output_file:
            self.loop.create_task(self._close_output(previous_output_file))

    async def _close_output(self, previous_output_file):
        pending_tasks = [task for task in asyncio.all_tasks(self.loop)
                         if task._coro.cr_code.co_name == 'save_raw_data']
        if pending_tasks:
            await asyncio.wait(pending_tasks)
        await previous_output_file.close()

    async def save_raw_data(self, data):
        await self.output_file.write(data)

    def collect_data(self, data):
        self.loop.create_task(self.save_raw_data(data))

    def flush(self):
        self._reset_output()

    def stop(self):
        self.loop.run_until_complete(self._close_output(self.output_file))

import os
from uuid import uuid4

import aiofiles

from .base import BaseCollector


SAVE_RAW_DATA = os.getenv('SAVE_RAW_DATA') == '1'
RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', 'raw_data')


class RawDataCollector(BaseCollector):

    def __init__(self, consumer):
        super().__init__(consumer)
        if SAVE_RAW_DATA:
            self.path = os.path.join(RAW_DATA_PATH, consumer.consumer_id)
            os.mkdir(self.path)

    async def save_raw_data(self, data):
        output_path = os.path.join(self.path, str(uuid4()))
        async with aiofiles.open(output_path, mode='bw') as output_file:
            await output_file.write(data)

    def collect_data(self, data):
        if SAVE_RAW_DATA:
            self.loop.create_task(self.save_raw_data(data))

    def flush(self):
        pass

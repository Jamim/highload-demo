import os
from uuid import uuid4

import aiofiles
import ujson


CONFIRMATION = os.getenv('CONFIRMATION')
if CONFIRMATION:
    CONFIRMATION = CONFIRMATION.encode()
SAVE_RAW_DATA = os.getenv('SAVE_RAW_DATA') == '1'


class Protocol:

    consumer = None
    loop = None
    transport = None

    def __init__(self, consumer):
        self.consumer = consumer
        self.loop = consumer.loop

    def connection_made(self, transport):
        self.transport = transport

    @staticmethod
    async def save_raw_data(data):
        async with aiofiles.open(str(uuid4()), mode='bw') as output_file:
            await output_file.write(data)

    @staticmethod
    def decode_data(data):
        # WARNING: some kind of validation should be there for the real app
        return ujson.loads(data)

    def datagram_received(self, data, address):
        if SAVE_RAW_DATA:
            self.loop.create_task(self.save_raw_data(data))
        self.consumer.process_data(self.decode_data(data))

        if CONFIRMATION:
            self.transport.sendto(CONFIRMATION, address)

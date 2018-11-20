import asyncio
import logging
import warnings
from os import getenv
from uuid import uuid4

from protocol import Protocol
from stats_collector import StatsCollector

warnings.filterwarnings('ignore', module='aioinflux.compat')
from aioinflux import InfluxDBClient  # noqa: E402

logger = logging.getLogger(__name__)


INFLUXDB_HOST = getenv('INFLUXDB_HOST', '127.0.0.1')
INFLUXDB_PORT = int(getenv('INFLUXDB_PORT', 8086))
INFLUXDB_DB = getenv('INFLUXDB_DB', 'highload-demo')


LISTEN_IP = getenv('LISTEN_IP', '0.0.0.0')
LISTEN_PORT = int(getenv('LISTEN_PORT', 51273))
LISTEN_ADDRESS = LISTEN_IP, LISTEN_PORT

STORE_STATS_INTERVAL = 10


class Consumer:

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.consumer_id = str(uuid4())
        self.stats_collector = StatsCollector(self.consumer_id)
        self.influxdb_client = InfluxDBClient(host=INFLUXDB_HOST,
                                              db=INFLUXDB_DB,
                                              loop=self.loop)
        logger.info('EventConsumer has been started: %s', self.consumer_id)

    def run(self):
        loop = self.loop
        coroutine = loop.create_datagram_endpoint(lambda: Protocol(self),
                                                  LISTEN_ADDRESS)
        transport, _ = loop.run_until_complete(coroutine)

        try:
            loop.run_until_complete(self.collect_stats())
        except KeyboardInterrupt:
            pass
        finally:
            transport.close()
            loop.close()

    async def collect_stats(self):
        while True:
            await asyncio.sleep(STORE_STATS_INTERVAL)
            try:
                await self.store_stats()
            except Exception:
                logger.exception('Stat storing has failed',
                                 extra={'consumer_id': self.consumer_id})

    async def store_stats(self):
        stats = self.stats_collector.get_stats()
        self.stats_collector.reset_stats()

        for entry in stats:
            self.loop.create_task(self.influxdb_client.write(entry))

    def process_data(self, data):
        self.stats_collector.collect_data(data)


if __name__ == '__main__':
    Consumer().run()

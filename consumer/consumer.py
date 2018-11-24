import asyncio
import logging
from time import time
from uuid import uuid4

import ujson

from protocols import UDPProtocol
from collectors import RawDataCollector, StatsCollector


STORE_INTERVAL = 10


class Consumer:

    def __init__(self, protocol_class):
        self.logger = self.configure_logging()

        self.loop = asyncio.get_event_loop()
        self.protocol = protocol_class(self)

        self._stop = False
        self.collecting_task = None
        self.sleeping_task = None
        self.consumer_id = str(uuid4())

        self.raw_data_collector = RawDataCollector(self)
        self.stats_collector = StatsCollector(self, ujson.loads)

        self.consumed_count = 0
        self.last_flush_time = time()

        self.logger.info('EventConsumer has been started: %s',
                         self.consumer_id)

    @staticmethod
    def configure_logging():
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        return logger

    def run(self):
        self.protocol.start()
        self.loop.create_task(self.collect_stats_and_data())

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self._stop = True
        self.protocol.stop()
        self.sleeping_task.cancel()
        loop = self.loop

        def run_pending_tasks():
            pending_tasks = asyncio.all_tasks(loop)
            if pending_tasks:
                loop.run_until_complete(asyncio.wait(pending_tasks))

        run_pending_tasks()
        self.stats_collector.flush()
        self.raw_data_collector.stop()

        run_pending_tasks()
        self.stats_collector.stop()
        loop.close()

    async def collect_stats_and_data(self):
        while not self._stop:
            collectors = self.raw_data_collector, self.stats_collector

            self.sleeping_task = self.loop.create_task(
                asyncio.sleep(STORE_INTERVAL)
            )
            try:
                await self.sleeping_task
            except asyncio.CancelledError:
                pass
            else:
                for collector in collectors:
                    try:
                        collector.flush()
                    except Exception:
                        self.logger.exception('Flushing has failed', extra={
                            'consumer_id': self.consumer_id,
                            'collector': collector.__class__.__name__
                        })

            current_time = time()
            duration = current_time - self.last_flush_time
            rate = self.consumed_count / duration

            # consumed count IS NOT 100% accurate
            self.logger.info(
                f'{self.consumed_count} packets were consumed in '
                f'{duration:.03f} seconds at {rate:.02f} average pps'
            )
            self.consumed_count = 0
            self.last_flush_time = current_time

    def consume_packet(self, packet):
        self.raw_data_collector.collect_data(packet)
        self.stats_collector.collect_data(packet)
        self.consumed_count += 1


if __name__ == '__main__':
    Consumer(UDPProtocol).run()

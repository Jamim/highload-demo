import warnings
from collections import Counter, defaultdict
from datetime import datetime
from os import getenv

from .base import BaseCollector

warnings.filterwarnings('ignore', module='aioinflux.compat')
from aioinflux import InfluxDBClient  # noqa: E402


INFLUXDB_HOST = getenv('INFLUXDB_HOST', '127.0.0.1')
INFLUXDB_PORT = int(getenv('INFLUXDB_PORT', 8086))
INFLUXDB_DB = getenv('INFLUXDB_DB', 'highload-demo')


class StatsCollector(BaseCollector):

    count_stats = None
    duration_stats = None

    def __init__(self, consumer):
        super().__init__(consumer)
        self.reset_stats()
        self.influxdb_client = InfluxDBClient(host=INFLUXDB_HOST,
                                              db=INFLUXDB_DB,
                                              loop=self.loop)

    def reset_stats(self):
        self.count_stats = defaultdict(Counter)
        self.duration_stats = defaultdict(Counter)

    def collect_data(self, data):
        country = data['country']

        count_stats = self.count_stats[country]
        duration_stats = self.duration_stats[country]

        for event in data['events']:
            event_type = event['type']
            count_stats[event_type] += 1
            duration_stats[event_type] += event['duration']

    def get_stats(self):
        timestamp = datetime.utcnow()
        for country, country_count_stats in self.count_stats.items():
            country_duration_stats = self.duration_stats[country]
            for event_type, count in country_count_stats.items():
                entry = {
                    'time': timestamp,
                    'measurement': 'events',
                    'tags': {
                        'consumer': self.consumer_id,
                        'country': country,
                        'event': event_type,
                    },
                    'fields': {
                        'avg_time': country_duration_stats[event_type] / count,
                        'count': count,
                    },
                }
                yield entry

    def flush(self):
        for entry in self.get_stats():
            self.loop.create_task(self.influxdb_client.write(entry))
        self.reset_stats()

    def stop(self):
        self.loop.run_until_complete(self.influxdb_client.close())

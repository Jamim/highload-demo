from collections import Counter, defaultdict
from datetime import datetime


class StatsCollector:

    consumer_id = None
    count_stats = None
    duration_stats = None

    def __init__(self, consumer_id):
        self.consumer_id = consumer_id
        self.reset_stats()

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
        stats = []
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
                stats.append(entry)

        return stats

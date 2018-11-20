class BaseCollector:

    def __init__(self, consumer):
        self.consumer_id = consumer.consumer_id
        self.loop = consumer.loop

    def collect_data(self, data):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

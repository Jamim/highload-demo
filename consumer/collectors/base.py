class CollectorMetaclass(type):

    def __new__(mcs, name, bases, class_dict):
        if not class_dict.get('enabled'):
            def stub(*args, **kwargs):
                pass

            for method in ('collect_data', 'flush', 'stop'):
                class_dict[method] = stub

        return super(CollectorMetaclass, mcs).__new__(
            mcs, name, bases, class_dict
        )


class BaseCollector(metaclass=CollectorMetaclass):

    enabled = False

    def __init__(self, consumer):
        self.consumer_id = consumer.consumer_id
        self.loop = consumer.loop

    def collect_data(self, data):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

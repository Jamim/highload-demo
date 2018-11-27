import importlib
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from time import time

import msgpack

json_modules = [json]

for name in ('simplejson', 'rapidjson', 'ujson'):
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError:
        pass
    else:
        json_modules.append(module)

FORMAT_MODULES = {
    'json': json_modules,
    'msgpack': (msgpack,)
}


PACKETS_PATH = os.getenv('PACKETS_PATH', 'test_data/packets')


class Timer:

    def __init__(self, action, postfix='', count=None):
        self.action = action
        self.postfix = postfix
        self.count = count

    def __enter__(self):
        self.start_time = time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time() - self.start_time
        print(f'{self.count} packets were {self.action} '
              f'in {duration:.03f} seconds '
              f'at {self.count/duration:.02f} pps rate '
              f'{self.postfix}')


def measure(packet_format):
    os.chdir(PACKETS_PATH)

    data = []
    with Timer('loaded') as timer:
        for filename in os.listdir('.'):
            if filename.endswith(packet_format):
                with open(filename, mode='rb') as input_file:
                    data.append(input_file.read())
        timer.count = len(data)

    for module in FORMAT_MODULES[packet_format]:
        measure_module_performance(module, data)


def measure_module_performance(module, data):
    loads = module.loads
    count = len(data)

    with Timer(f'parsed with {module.__name__}', '[main thread]', count):
        for packet in data:
            loads(packet)

    # DISCLAIMER:
    # I understand that this approach is wrong.
    # Data should be loaded in the same thread or process
    # where it will be processed.
    # Here I'm trying to measure the overhead.

    with Timer(f'parsed with {module.__name__}', '[4 threads]', count):
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(loads, data)

    with Timer(f'parsed with {module.__name__}', '[4 processes]', count):
        with ProcessPoolExecutor(max_workers=4) as executor:
            executor.map(loads, data)


def main():
    packet_format = 'msgpack' if '--msgpack' in sys.argv else 'json'
    measure(packet_format)


if __name__ == '__main__':
    main()

import argparse
import json
import os
import shutil
import sys

import msgpack
from faker import Faker

try:
    from test_data.countries import COUNTRIES
    from test_data.events import EVENTS
except ModuleNotFoundError:
    COUNTRIES = None
    EVENTS = None


faker = Faker()

QUOTE = "'"
ESCAPED_QUOTE = r"\\'"
GARBAGE_PLACEHOLDER = '%garbage%'
GARBAGE_PLACEHOLDER_PACKED = {
    'json': json.dumps(GARBAGE_PLACEHOLDER),
    'msgpack': msgpack.dumps(GARBAGE_PLACEHOLDER),
}
FORMAT_MODE = {
    'json': 'w',
    'msgpack': 'bw',
}

TARGET_PACKET_LENGTH = 50_000
TEST_DATA_PATH = 'test_data'
PACKETS_PATH = os.path.join(TEST_DATA_PATH, 'packets')


def generate_data(title, seed, count, generator):
    filename = os.path.join(TEST_DATA_PATH, f'{title}.py')
    with open(filename, 'w') as countries_file:
        countries_file.write(
            f'# This file was automatically generated using {__file__}\n'
            f'# with seed {seed} and count {count}\n\n'
            f'{title.upper()} = (\n'
        )
        for _ in range(count):
            # duplicates are fine for this demo
            countries_file.write(
                f"    '{generator().replace(QUOTE, ESCAPED_QUOTE)}',\n"
            )
        countries_file.write(')\n')


def generate_countries(args):
    generate_data('countries', args.seed, args.count, faker.country)


def generate_events(args):
    generate_data('events', args.seed, args.count, faker.catch_phrase)


def generate_packet(packet_index, events_per_packet, packet_format):
    choice = faker.random.choice
    random = faker.random.random

    events = []

    total_duration = 300  # 5 minutes
    for packets_left in reversed(range(1, events_per_packet + 1)):
        if packets_left > 1:
            duration = total_duration / packets_left * (1.5 - random())
        else:
            duration = total_duration
        total_duration -= duration

        events.append({
            'type': choice(EVENTS),
            'duration': duration,
        })

    dumps = globals()[packet_format].dumps
    packet = dumps({
        'country': choice(COUNTRIES),
        'user': faker.uuid4(),
        'events': events,
        'garbage': GARBAGE_PLACEHOLDER,
    })
    garbage_length = (
        TARGET_PACKET_LENGTH + len(GARBAGE_PLACEHOLDER) - len(packet)
    )
    if packet_format == 'msgpack':
        garbage_length -= 2  # adjusting data length

    packet = packet.replace(GARBAGE_PLACEHOLDER_PACKED[packet_format],
                            dumps('0' * garbage_length))

    filename = f'{packet_index:0>6d}.{packet_format}'
    file_path = os.path.join(PACKETS_PATH, filename)
    with open(file_path, mode=FORMAT_MODE[packet_format]) as packet_file:
        packet_file.write(packet)


def generate_packets(args):
    if not (COUNTRIES and EVENTS):
        print('Please generate countries and events first!')
        sys.exit(1)

    shutil.rmtree(PACKETS_PATH, ignore_errors=True)
    os.mkdir(PACKETS_PATH)

    events_per_packet = args.events_per_packet
    packet_format = 'msgpack' if args.msgpack else 'json'
    for index in range(args.count):
        generate_packet(index, events_per_packet, packet_format)


def main(args):
    if args.seed is None:
        args.seed = faker.pyint()
    faker.seed(args.seed)

    if args.countries:
        generate_countries(args)
    elif args.events:
        generate_events(args)
    elif args.packets:
        generate_packets(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Test data generator for highload-demo project.'
    )
    parser.add_argument('--seed', '-s', type=int,
                        help='set random seed')
    parser.add_argument('--count', '-c', type=int, default=100,
                        help='count of entries to generate')
    parser.add_argument('--events-per-packet', type=int, default=500,
                        help='count of events in each packet (max 500)')
    parser.add_argument('--msgpack', action='store_true',
                        help='dumps with msgpack instead of json')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--countries', action='store_true',
                       help='generate countries.py')
    group.add_argument('--events', action='store_true',
                       help='generate events.py')
    group.add_argument('--packets', action='store_true',
                       help='generate data packets')

    main(parser.parse_args())

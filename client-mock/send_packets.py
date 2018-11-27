import os
import socket
import sys
from os import getenv
from time import sleep

from tqdm import tqdm


CONSUMER_IP = getenv('CONSUMER_IP', '127.0.0.1')
CONSUMER_PORT = int(getenv('CONSUMER_PORT', 51273))
CONSUMER_ADDRESS = CONSUMER_IP, CONSUMER_PORT

CONFIRMATION = getenv('CONFIRMATION')
if CONFIRMATION:
    CONFIRMATION = CONFIRMATION.encode()

PACKETS_PATH = getenv('PACKETS_PATH', 'test_data/packets')


class UDPProtocol:

    def __init__(self):
        self.transport = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, packet, address):
        self.transport.sendto(packet, address)

    def stop(self):
        self.transport.close()


class Producer:

    def __init__(self, interval, packets, protocol_class):
        self.interval = interval
        self.packets = packets
        self.protocol = protocol_class()

    def run(self):
        try:
            self.send_packets()
        except KeyboardInterrupt:
            pass
        finally:
            self.protocol.stop()

    def send_packets(self):
        interval = self.interval
        send = self.protocol.send
        for packet in tqdm(self.packets, unit='packet'):
            send(packet, CONSUMER_ADDRESS)
            if interval:
                sleep(interval)


def main():
    os.chdir(PACKETS_PATH)
    iterations = int(sys.argv[1])
    interval = float(sys.argv[2])

    packets = []
    for filename in os.listdir('.'):
        with open(filename, mode='rb') as input_file:
            packets.append(input_file.read())
    Producer(interval, packets * iterations, UDPProtocol).run()


if __name__ == '__main__':
    main()

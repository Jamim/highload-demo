import asyncio
import os
from os import getenv

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
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        pass

    def datagram_received(self, packet, address):
        pass

    def send(self, packet, address):
        self.transport.sendto(packet, address)

    def start(self, loop):
        coroutine = loop.create_datagram_endpoint(lambda: self,
                                                  remote_addr=CONSUMER_ADDRESS)
        loop.run_until_complete(coroutine)

    def stop(self):
        self.transport.close()


class Producer:

    def __init__(self, packets, protocol_class):
        self.packets = packets
        self.protocol = protocol_class()
        self.loop = asyncio.get_event_loop()

    def run(self):
        self.protocol.start(self.loop)

        try:
            self.loop.run_until_complete(self.send_packets())
        except KeyboardInterrupt:
            self.protocol.stop()
            self.loop.close()

    async def send_packets(self):
        send = self.protocol.send
        for packet in tqdm(self.packets):
            send(packet, CONSUMER_ADDRESS)
        await asyncio.sleep(10)


if __name__ == '__main__':
    os.chdir(PACKETS_PATH)

    packets = []
    for filename in os.listdir('.'):
        if filename.endswith('.msgpack'):
            with open(filename, mode='rb') as input_file:
                packets.append(input_file.read())
    Producer(packets, UDPProtocol).run()

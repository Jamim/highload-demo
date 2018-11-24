from os import getenv


LISTEN_IP = getenv('LISTEN_IP', '0.0.0.0')
LISTEN_PORT = int(getenv('LISTEN_PORT', 51273))
LISTEN_ADDRESS = LISTEN_IP, LISTEN_PORT

CONFIRMATION = getenv('CONFIRMATION')
if CONFIRMATION:
    CONFIRMATION = CONFIRMATION.encode()


class UDPProtocol:

    def __init__(self, consumer):
        self.consumer = consumer
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        pass

    def datagram_received(self, packet, address):
        # WARNING: some kind of filtering should be there for the real app
        self.consumer.consume_packet(packet)

        if CONFIRMATION:
            self.transport.sendto(CONFIRMATION, address)

    def start(self):
        loop = self.consumer.loop
        coroutine = loop.create_datagram_endpoint(lambda: self, LISTEN_ADDRESS,
                                                  reuse_port=True)
        loop.run_until_complete(coroutine)

    def stop(self):
        self.transport.close()

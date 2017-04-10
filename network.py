import asyncio
import logging
import socket

class AsyncNetwork:
    PORT = 13337

    nodes = {}

    def __init__(self, evloop, nodeslist):
        self.evloop = evloop
        self.nodeslist = nodeslist

    async def create_server(self):
        self.server = await self.evloop.create_server(
            lambda: TCPProtocol(self.evloop), port=AsyncNetwork.PORT,
            family=socket.AF_INET, reuse_address=True, reuse_port=True
        )
        logging.info('Created server...')

    async def try_connect_to_remaining(self):
        for node, port in self.nodeslist:
            if (node == socket.gethostname() or
                    socket.gethostbyname(node) in AsyncNetwork.nodes):
                # don't connect to self or someone already connected to
                continue

            logging.info(f'Trying connect to {node}')
            try:
                transport, proto = await self.evloop.create_connection(
                    lambda: TCPProtocol(self.evloop), host=node, port=int(port),
                    family=socket.AF_INET
                )
            except ConnectionRefusedError:
                logging.warning(f'Could not connect to {node}')


    def close(self):
        self.server.close()


class Node:
    def __init__(self):
        pass


class TCPProtocol(asyncio.Protocol):
    def __init__(self, evloop):
        self.evloop = evloop
        logging.info('Created protocol!')

    def connection_made(self, transport):
        self.transport = transport
        self.peer = self.transport.get_extra_info('peername')[0]
        logging.info('Got connection from {}'.format(str(self.peer)))

        AsyncNetwork.nodes[self.peer] = self.transport

    def connection_lost(self, exc):
        logging.info('Connection lost with {}'.format(str(self.peer)))
        del AsyncNetwork.nodes[self.peer]

        super().connection_lost(exc)

    def data_received(self, data):
        logging.info(f'Got from {self.peer[0]}: {data}')

    def eof_received(self):
        pass

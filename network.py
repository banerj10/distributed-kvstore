import asyncio
import logging
import pickle
import socket

from messages import TextMsg

class AsyncNetwork:
    PORT = 13337

    # ip_addr -> Peer (has transport and protocol objs inside)
    nodes = {}

    def __init__(self, evloop, nodeslist):
        self.evloop = evloop
        self.server = None

        for node, _ in nodeslist:
            if node != socket.gethostname(): # don't add self
                ip = socket.gethostbyname(node)
                AsyncNetwork.nodes[ip] = None

    async def create_server(self):
        self.server = await self.evloop.create_server(
            lambda: TCPProtocol(self.evloop, self.request_handler),
            port=AsyncNetwork.PORT, family=socket.AF_INET, reuse_address=True,
            reuse_port=True
        )
        logging.info('Created server...')

    async def connect_to_peers(self):
        for nodeip in list(AsyncNetwork.nodes.keys()):
            if AsyncNetwork.nodes[nodeip] is not None:
                # don't connect to someone already connected to
                continue

            logging.info(f'Trying connect to {nodeip}')
            try:
                transport, proto = await self.evloop.create_connection(
                    lambda: TCPProtocol(self.evloop, self.request_handler),
                    host=nodeip, port=AsyncNetwork.PORT, family=socket.AF_INET
                )
            except ConnectionRefusedError:
                logging.warning(f'Could not connect to {nodeip}')
                continue

            AsyncNetwork.nodes[nodeip] = Peer(self.evloop, transport, proto)

    def request_handler(self, data):
        pass

    def close(self):
        self.server.close()


class Peer:
    def __init__(self, evloop, transport, protocol):
        self.evloop = evloop
        self.transport = transport
        self.protocol = protocol
        self.msgqueue = asyncio.Queue()

        asyncio.ensure_future(self.start(), loop=self.evloop)

    async def start(self):
        while not self.transport.is_closing():
            msg = await self.msgqueue.get()
            destination = msg.destination
            if not destination:
                logging.error('!!!!! NO DESTINATION !!!!!')
                continue
            pickled = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
            self.transport.write(pickled)

    async def send(self, msg):
        msg.destination = self.protocol.peer
        await self.msgqueue.put(msg)


class TCPProtocol(asyncio.Protocol):
    def __init__(self, evloop, req_handler):
        self.evloop = evloop
        self.req_handler = req_handler
        logging.info('Created protocol!')

    def connection_made(self, transport):
        self.transport = transport
        self.peer = self.transport.get_extra_info('peername')[0]
        logging.info(f'Got connection from {str(self.peer)}')

        AsyncNetwork.nodes[self.peer] = (self.transport, self)

    def connection_lost(self, exc):
        logging.info(f'Connection lost with {str(self.peer)}')
        del AsyncNetwork.nodes[self.peer]

        super().connection_lost(exc)

    def data_received(self, data):
        logging.info(f'Got data from {self.peer}')
        unpickled = pickle.loads(data)
        self.req_handler(unpickled)

    def eof_received(self):
        pass

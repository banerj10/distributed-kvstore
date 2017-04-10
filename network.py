import asyncio
import logging
import pickle
import socket

from messages import *
from storage import Store

class AsyncNetwork:
    PORT = 13337
    OWN_ID = -1
    OWN_IP = ''

    # ip_addr -> Peer (has transport and protocol objs inside)
    nodes = dict()

    # uuid4 -> (Event, Response msg)
    requests = dict()

    # id -> ip_addr
    ids = dict()
    # ip_addr -> id
    ips = dict()

    def __init__(self, evloop, nodeslist):
        self.evloop = evloop
        self.server = None

        for node, id in nodeslist:
            id = int(id)
            ip = socket.gethostbyname(node)
            AsyncNetwork.ids[id] = ip
            AsyncNetwork.ips[ip] = id

            if node != socket.gethostname(): # don't add self
                AsyncNetwork.nodes[ip] = None
            else:
                AsyncNetwork.OWN_ID = id
                AsyncNetwork.OWN_IP = ip

        logging.info(f'IDS: {AsyncNetwork.ids}')

    @staticmethod
    def placement(key, reverse=False, return_id=False):
        """
        Places the given key in the cluster and returns placement
        """
        if isinstance(key, str):
            key = Store.hash(key)

        orig = key
        if key == AsyncNetwork.OWN_ID:
            if return_id:
                return AsyncNetwork.OWN_ID
            else:
                return None

        while AsyncNetwork.nodes[AsyncNetwork.ids[key]] is None:
            if reverse:
                key -= 1
                if key < 0:
                    key = 9
            else:
                key += 1
                key = key % 10

            if key == orig or key == AsyncNetwork.OWN_ID:
                if return_id:
                    return AsyncNetwork.OWN_ID
                else:
                    return None

        if return_id:
            return key
        else:
            return AsyncNetwork.nodes[AsyncNetwork.ids[key]]

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

    def request_handler(self, msg):
        cls = msg.__class__.__name__
        handler = getattr(self, f'handle_{cls}', None)

        if handler is None:
            logging.warning(f'Dont recognize msg {cls}')
        else:
            logging.info(f'Got {cls}')
            handler(msg)

    def handle_SetMsg(self, msg):
        Store.hash_table[msg.key] = msg.value
        respondmsg = SetMsgResponse(msg.uid)

        # send replication msgs to others
        curr_id = AsyncNetwork.OWN_ID
        succ_id = (curr_id + 1) % 10
        pred_id = 9 if curr_id == 0 else (curr_id - 1)
        # TODO: expects succ_ip and pred_ip to exist in the Nodes dict
        # get successor ID
        while AsyncNetwork.nodes[AsyncNetwork.ids[succ_id]] is None:
            succ_id = (succ_id + 1) % 10
        # get predecessor ID
        while AsyncNetwork.nodes[AsyncNetwork.ids[pred_id]] is None:
            pred_id = 9 if pred_id == 0 else (pred_id - 1)

        # respond only if msg has an origin
        if msg.origin:
            self.evloop.create_task(
                AsyncNetwork.nodes[msg.origin].send(respondmsg)
            )
        # send replicas
        replicamsg = ReplicationMsg({msg.key: msg.value})
        self.evloop.create_task(
            AsyncNetwork.nodes[AsyncNetwork.ids[succ_id]].send(replicamsg)
        )
        self.evloop.create_task(
            AsyncNetwork.nodes[AsyncNetwork.ids[pred_id]].send(replicamsg)
        )

    def handle_SetMsgResponse(self, msg):
        orig_uid = msg.orig_uid
        event, _ = AsyncNetwork.requests[orig_uid]
        event.set()
        response = msg
        AsyncNetwork.requests[orig_uid] = event, response

    def handle_GetMsg(self, msg):
        value = Store.hash_table.get(msg.key, None)
        respondmsg = GetMsgResponse(msg.uid, value)
        asyncio.ensure_future(
            AsyncNetwork.nodes[msg.origin].send(respondmsg), loop=self.evloop)

    def handle_GetMsgResponse(self, msg):
        orig_uid = msg.orig_uid
        event, _ = AsyncNetwork.requests[orig_uid]
        event.set()
        response = msg
        AsyncNetwork.requests[orig_uid] = event, response

    def handle_GetOwners(self, msg):
        pass

    def handle_GetOwnersResponse(self, msg):
        pass

    def handle_ReplicationMsg(self, msg):
        orig_id = AsyncNetwork.ips[msg.origin]

        if orig_id not in Store.replicas:
            Store.replicas[orig_id] = dict()
        Store.replicas[orig_id].update(msg.data)

    def close(self):
        self.server.close()


class Peer:
    def __init__(self, evloop, transport, protocol):
        self.evloop = evloop
        self.transport = transport
        self.protocol = protocol
        self.msgqueue = asyncio.Queue()

        self.evloop.create_task(self.start())

    async def start(self):
        while not self.transport.is_closing():
            msg = await self.msgqueue.get()
            if not msg.destination:
                logging.error('!!!!! NO DESTINATION !!!!!')
                continue
            pickled = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
            self.transport.write(pickled)

    async def send(self, msg):
        msg.origin = self.protocol.addr
        msg.destination = self.protocol.peer
        logging.info(f'Sending {msg.type()} to {msg.destination}')
        await self.msgqueue.put(msg)


class TCPProtocol(asyncio.Protocol):
    def __init__(self, evloop, req_handler):
        self.evloop = evloop
        self.req_handler = req_handler
        self.addr = socket.gethostbyname(socket.gethostname())
        logging.info('Created protocol!')

    def connection_made(self, transport):
        self.transport = transport
        self.peer = self.transport.get_extra_info('peername')[0]
        logging.info(f'Got connection from {str(self.peer)}')

        AsyncNetwork.nodes[self.peer] = Peer(self.evloop, self.transport, self)

    def connection_lost(self, exc):
        logging.info(f'Connection lost with {str(self.peer)}')
        AsyncNetwork.nodes[self.peer] = None

        super().connection_lost(exc)

    def data_received(self, data):
        logging.info(f'Got data from {self.peer}')
        unpickled = pickle.loads(data)
        self.req_handler(unpickled)

    def eof_received(self):
        pass

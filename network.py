import asyncio
import copy
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

        for node, nodeid in nodeslist:
            nodeid = int(nodeid)
            ip = socket.gethostbyname(node)
            AsyncNetwork.ids[nodeid] = ip
            AsyncNetwork.ips[ip] = nodeid

            if node != socket.gethostname(): # don't add self
                AsyncNetwork.nodes[ip] = None
            else:
                AsyncNetwork.OWN_ID = nodeid
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

        logging.debug(f'handle_SendMsg: {curr_id} {succ_id} {pred_id}')

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

    def handle_GetMsg(self, msg, ret=False):
        value = Store.hash_table.get(msg.key, None)
        respondmsg = GetMsgResponse(msg.uid, value)

        if ret:
            return value
        if msg.origin:
            self.evloop.create_task(
                AsyncNetwork.nodes[msg.origin].send(respondmsg)
            )

    def handle_GetMsgResponse(self, msg):
        orig_uid = msg.orig_uid
        event, _ = AsyncNetwork.requests[orig_uid]
        event.set()
        response = msg
        AsyncNetwork.requests[orig_uid] = event, response

    def handle_GetOwners(self, msg, ret=False):
        value = Store.hash_table.get(msg.key, None)
        if value is None:
            for repdicts in Store.replicas.values():
                value = repdicts.get(msg.key, None)
                if value is not None:
                    break
        respondmsg = GetOwnersResponse(msg.uid, msg.key, (value is not None))

        if ret:
            return (value is not None)
        if msg.origin:
            self.evloop.create_task(
                AsyncNetwork.nodes[msg.origin].send(respondmsg)
            )

    def handle_GetOwnersResponse(self, msg):
        orig_uid = msg.orig_uid
        event, _ = AsyncNetwork.requests[orig_uid]
        event.set()
        response = msg
        AsyncNetwork.requests[orig_uid] = event, response

    def handle_ReplicationMsg(self, msg):
        orig_id = AsyncNetwork.ips[msg.origin]

        if orig_id not in Store.replicas:
            Store.replicas[orig_id] = dict()
        Store.replicas[orig_id].update(msg.data)

    def handle_StabilizationMsg(self, msg):
        origin_id = AsyncNetwork.ips[msg.origin]

        if msg.rename:
            Store.replicas[origin_id] = copy.deepcopy(
                Store.replicas[msg.designation]
            )
            Store.replicas[origin_id].update(msg.data)
        else:
            if msg.designation not in Store.replicas:
                Store.replicas[msg.designation] = dict()
            Store.replicas[msg.designation].update(msg.data)

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
        while self.protocol.is_alive:
            msg = await self.msgqueue.get()
            logging.debug('Sending msg from the queue...')
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

    def failure_predecessor(self, peer_ip, old_topology, failed_succ_id):
        peer_id = AsyncNetwork.ips[peer_ip]

        # find predecessor of peer_id
        failed_pred_id = 9 if peer_id == 0 else (peer_id - 1)
        while failed_pred_id != peer_id:
            if old_topology[AsyncNetwork.ids[failed_pred_id]] is not None:
                break
            failed_pred_id = 9 if failed_pred_id == 0 else (failed_pred_id - 1)

        if failed_pred_id != AsyncNetwork.OWN_ID:
            # I do not do anything if I'm not the predecessor of the failed peer
            logging.info(f'I am NOT predecessor of peer {peer_id+1:02d}')
            return

        stmsg1 = StabilizationMsg(copy.deepcopy(Store.hash_table),
                                  AsyncNetwork.OWN_ID)
        # TODO: destination might have failed
        self.evloop.create_task(
            AsyncNetwork.nodes[AsyncNetwork.ids[failed_succ_id]].send(stmsg1)
        )

    async def handle_failure(self, peer_ip, old_topology):
        peer_id = AsyncNetwork.ips[peer_ip]

        # find successor of peer_id
        failed_succ_id = (peer_id + 1) % 10
        while failed_succ_id != peer_id:
            if old_topology[AsyncNetwork.ids[failed_succ_id]] is not None:
                break
            failed_succ_id = (failed_succ_id + 1) % 10

        if failed_succ_id != AsyncNetwork.OWN_ID:
            # I do not do anything if I'm not the successor of the failed peer
            logging.info(f'I am NOT successor of peer {peer_id+1:02d}')
            self.failure_predecessor(peer_ip, old_topology, failed_succ_id)
            return

        logging.info(f'ALERT! Need to handle peer {peer_id+1:02d}\'s failure')

        """
        X = peer_id
        1. Send my data to predecessor. Predecessor will update R(peer_id) to
           R(self), and do R(self) += my data
        2. Predecessor will respond with his own data, I will create
           R(predecessor)
        3. I will merge: my data += R(X)
        4. Send R(X) to my successor. Successor updates R(me) with R(X)
        5. Delete R(X)
        """

        pred_id = 9 if peer_id == 0 else (peer_id - 1)
        while pred_id != peer_id:
            if old_topology[AsyncNetwork.ids[pred_id]] is not None:
                break
            pred_id = 9 if pred_id == 0 else (pred_id - 1)

        # STEP 1
        stmsg1 = StabilizationMsg(copy.deepcopy(Store.hash_table), peer_id,
                                  rename=True)
        # TODO: destination might have failed
        self.evloop.create_task(
            AsyncNetwork.nodes[AsyncNetwork.ids[pred_id]].send(stmsg1)
        )

        # STEP 2 - Is a response

        # STEP 3
        Store.hash_table.update(Store.replicas[peer_id])

        succ_id = (AsyncNetwork.OWN_ID + 1) % 10
        # get successor ID
        while True:
            if succ_id == AsyncNetwork.OWN_ID:
                break
            if AsyncNetwork.nodes[AsyncNetwork.ids[succ_id]] is not None:
                break
            succ_id = (succ_id + 1) % 10

        # STEP 4
        stmsg2 = StabilizationMsg(copy.deepcopy(Store.replicas[peer_id]),
                                  AsyncNetwork.OWN_ID)
        # TODO: destination might have failed
        self.evloop.create_task(
            AsyncNetwork.nodes[AsyncNetwork.ids[succ_id]].send(stmsg2)
        )

        # STEP 5
        del Store.replicas[peer_id]


class TCPProtocol(asyncio.Protocol):
    def __init__(self, evloop, req_handler):
        self.evloop = evloop
        self.req_handler = req_handler
        self.addr = socket.gethostbyname(socket.gethostname())
        self.is_alive = False
        logging.info('Created protocol!')

    def connection_made(self, transport):
        self.transport = transport
        self.is_alive = True
        self.peer = self.transport.get_extra_info('peername')[0]
        logging.info(f'Got connection from {str(self.peer)}')

        AsyncNetwork.nodes[self.peer] = Peer(self.evloop, self.transport, self)

    def connection_lost(self, exc):
        logging.info(f'Connection lost with {str(self.peer)}')
        self.is_alive = False
        self.evloop.create_task(
            AsyncNetwork.nodes[self.peer].handle_failure(
                self.peer, copy.copy(AsyncNetwork.nodes)
            )
        )
        AsyncNetwork.nodes[self.peer] = None

        super().connection_lost(exc)

    def data_received(self, data):
        logging.info(f'Got data from {self.peer}')
        unpickled = pickle.loads(data)
        self.req_handler(unpickled)

    def eof_received(self):
        pass

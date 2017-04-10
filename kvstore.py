import asyncio
from concurrent import futures
import logging

from messages import *
from network import AsyncNetwork
from storage import Store
from ui import UI

class KVStore:
    def __init__(self, evloop, nodeslist):
        self.evloop = evloop

        self.network = AsyncNetwork(self.evloop, nodeslist)
        self.ui = UI(self.evloop)
        self.ui.output('=============================')
        self.ui.output('==== Distributed KVStore ====')
        self.ui.output('=============================')
        self.ui.output(f'ID: {AsyncNetwork.OWN_ID + 1:02d}')
        self.ui.output(f'IP ADDR: {AsyncNetwork.OWN_IP}')

    async def main(self):
        await self.network.create_server()
        await self.network.connect_to_peers()

        try:
            while True:
                command = await self.ui.input()
                if command == '':
                    continue

                cmd = command.split()[0]
                data = command.split()[1:]

                cmd_handler = getattr(self, f'cmd_{cmd.lower()}', None)
                if cmd_handler is None:
                    self.ui.output(f'Unknown command "{cmd}"...')
                else:
                    await cmd_handler(data)

        except asyncio.CancelledError:
            self.network.close()
            self.ui.output('\nBYE!')

    async def cmd_set(self, data):
        if len(data) < 2:
            self.ui.output(f'Invalid! Usage: SET <key> <value>')
            return

        key = data[0]
        value = ' '.join(data[1:])

        hashed_key = Store.hash(key)
        logging.info(f'Key {key} hashed to {hashed_key}')
        dest = AsyncNetwork.placement(hashed_key)
        msg = SetMsg(key, value)

        if dest is None:
            logging.info('Storing in self!')
            self.network.handle_SetMsg(msg)
            self.ui.output('SET OK')
        else:
            # dest is a Peer object
            event = asyncio.Event()
            AsyncNetwork.requests[msg.uid] = (event, None)

            try:
                await asyncio.wait_for(dest.send(msg), 2, loop=self.evloop)
                await asyncio.wait_for(event.wait(), 3, loop=self.evloop)
            except asyncio.TimeoutError:
                # TODO: try sending to another peer
                logging.error('Failed to send SetMsg!')
                self.ui.output('SET FAILED')
            else:
                self.ui.output('SET OK')
            finally:
                del AsyncNetwork.requests[msg.uid]

        # sendlist = []
        # reqlist = []
        # for node, peer in AsyncNetwork.nodes.items():
        #     if peer is not None:
        #         msg = SetMsg(key, value)
        #         event = asyncio.Event()
        #
        #         sendlist.append(peer.send(msg))
        #         AsyncNetwork.requests[msg.uid] = (event, None)
        #         reqlist.append(msg.uid)
        #
        # await asyncio.wait(sendlist, loop=self.evloop, timeout=3)
        # done, pending = await asyncio.wait(
        #     [AsyncNetwork.requests[uid][0].wait() for uid in reqlist],
        #     loop=self.evloop, timeout=3
        # )
        #
        # if len(done) >= 1:
        #     self.ui.output('SET OK')
        # else:
        #     self.ui.output('SET FAILED')
        # # clean up the requests
        # for uid in reqlist:
        #     del AsyncNetwork.requests[uid]

    async def cmd_get(self, data):
        if len(data) != 1:
            self.ui.output(f'Invalid! Usage: GET <key>')
            return

        key = data[0]
        curr_id = AsyncNetwork.placement(key, return_id=True)
        succ_id = (curr_id + 1) % 10
        pred_id = 9 if curr_id == 0 else (curr_id - 1)
        # get successor ID
        while (succ_id != AsyncNetwork.OWN_ID or
                       AsyncNetwork.nodes[AsyncNetwork.ids[succ_id]] is None):
            succ_id = (succ_id + 1) % 10
        # get predecessor ID
        while (pred_id != AsyncNetwork.OWN_ID or
                       AsyncNetwork.nodes[AsyncNetwork.ids[pred_id]] is None):
            pred_id = 9 if pred_id == 0 else (pred_id - 1)

        try_ids = [curr_id, succ_id, pred_id]
        msg = GetMsg(key)

        for tryid in try_ids:
            logging.info(f'Trying to GetMsg from ID: {tryid}')
            if tryid == AsyncNetwork.OWN_ID:
                # get key from self
                value = self.network.handle_GetMsg(msg)
                if value is not None:
                    self.ui.output(f'Found: {value}')
                    return
            else:
                dest = AsyncNetwork.nodes[AsyncNetwork.ids[tryid]]
                if dest is None: # succ or pred is not alive
                    continue
                event = asyncio.Event()
                AsyncNetwork.requests[msg.uid] = (event, None)

                try:
                    await asyncio.wait_for(dest.send(msg), 2, loop=self.evloop)
                    await asyncio.wait_for(event.wait(), 3, loop=self.evloop)
                except asyncio.TimeoutError:
                    logging.error('Failed to send GetMsg!')
                else:
                    value = AsyncNetwork.requests[msg.uid][1].value
                    if value is not None:
                        self.ui.output(f'Found: {value}')
                        del AsyncNetwork.requests[msg.uid]
                        return
                finally:
                    del AsyncNetwork.requests[msg.uid]

        # none of the try_ids worked
        self.ui.output('Not found')

        # sendlist = []
        # reqlist = []
        # for node, peer in AsyncNetwork.nodes.items():
        #     if peer is not None:
        #         msg = GetMsg(key)
        #         event = asyncio.Event()
        #
        #         sendlist.append(peer.send(msg))
        #         AsyncNetwork.requests[msg.uid] = (event, None)
        #         reqlist.append(msg.uid)
        #
        # # TODO: add check for empty sendlist
        # await asyncio.wait(sendlist, loop=self.evloop, timeout=3)
        # done, pending = await asyncio.wait(
        #     [AsyncNetwork.requests[uid][0].wait() for uid in reqlist],
        #     loop=self.evloop, timeout=3
        # )
        #
        # if len(done) >= 1:
        #     values = [AsyncNetwork.requests[uid][1].value for uid in reqlist]
        #     for val in values:
        #         if val is not None:
        #             self.ui.output(f'Found: {val}')
        #             # clean up the requests
        #             for uid in reqlist:
        #                 del AsyncNetwork.requests[uid]
        #             return
        # # if we reach here, we could not find the key
        # self.ui.output('Not found')
        # # clean up the requests
        # for uid in reqlist:
        #     del AsyncNetwork.requests[uid]

    async def cmd_owners(self, data):
        if len(data) != 1:
            self.ui.output(f'Invalid! Usage: OWNERS <key>')
            return

        key = data[0]
        curr_id = AsyncNetwork.placement(key, return_id=True)
        succ_id = (curr_id + 1) % 10
        pred_id = 9 if curr_id == 0 else (curr_id - 1)
        # TODO: expects succ_ip and pred_ip to exist in the Nodes dict
        # get successor ID
        while AsyncNetwork.nodes[AsyncNetwork.ids[succ_id]] is None:
            succ_id = (succ_id + 1) % 10
        # get predecessor ID
        while AsyncNetwork.nodes[AsyncNetwork.ids[pred_id]] is None:
            pred_id = 9 if pred_id == 0 else (pred_id - 1)
        self.ui.output(f'{curr_id + 1:02d} {succ_id + 1:02d} {pred_id + 1:02d}')

    async def cmd_list_local(self, data):
        if len(data) != 0:
            self.ui.output(f'Invalid! Usage: LIST_LOCAL')
            return

        keys = []
        keys.extend(Store.hash_table.keys())
        for replica in Store.replicas.keys():
            keys.extend(Store.replicas[replica].keys())
        for key in keys:
            self.ui.output(f'{key}')
        self.ui.output('END LIST')

    async def cmd_batch(self, data):
        if len(data) != 2:
            self.ui.output(f'Invalid! Usage: BATCH <file1> <file2>')
            return

        self.ui.output('Don\'t know... :(')

    ####### CUSTOM COMMANDS #######

    async def cmd_connected(self, data):
        connected = [ip for ip, peer in AsyncNetwork.nodes.items() if peer != None]
        self.ui.output('\n'.join(connected))

    async def cmd_pending(self, data):
        pending = len(AsyncNetwork.requests.keys())
        self.ui.output(f'{pending} pending requests...')

    ###############################

logging.basicConfig(filename='app.log', level=logging.DEBUG)
logging.info('=============================')
logging.info('==== Distributed KVStore ====')
logging.info('=============================')

with open('nodeslist.txt', 'r') as f:
    nodes = [line.split() for line in f.readlines()]

evloop = asyncio.get_event_loop()
evloop.set_debug(True)
kvstore = KVStore(evloop, nodes)
main_task = evloop.create_task(kvstore.main())

pending = None
try:
    evloop.run_forever()
except KeyboardInterrupt:
    # main_task.cancel()
    # evloop.stop()
    pending = asyncio.Task.all_tasks(loop=evloop)
    for task in pending:
        task.cancel()
    # evloop.run_until_complete(main_task)

try:
    evloop.run_until_complete(asyncio.gather(*pending))
except asyncio.CancelledError:
    pass
finally:
    evloop.close()

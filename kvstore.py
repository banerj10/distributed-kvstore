import asyncio
import logging
import socket

from ui import UI
from network import AsyncNetwork
from messages import *

class KVStore:
    NODEID = int(socket.gethostname().split('.')[0][-2:]) - 1

    def __init__(self, evloop, nodeslist):
        self.evloop = evloop

        self.ui = UI(self.evloop)
        self.network = AsyncNetwork(self.evloop, nodeslist)

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
        data = data.split()
        if len(data) != 2:
            self.ui.output(f'Invalid: SET <key> <value>')
            return

        key = data[0]
        value = data[1]
        self.ui.output(f'Will SET {key} = {value}')

        sendlist = []
        waitlist = []
        for node, peer in AsyncNetwork.nodes.items():
            if peer is not None:
                msg = SetMsg(key, value)
                event = asyncio.Event()

                sendlist.append(peer.send(msg))
                AsyncNetwork.requests[msg.uid] = event
                waitlist.append(event.wait())

        await asyncio.wait(sendlist, loop=self.evloop, timeout=3)
        await asyncio.wait(waitlist, loop=self.evloop, timeout=3)

    async def cmd_connected(self, data):
        connected = [ip for ip, peer in AsyncNetwork.nodes.items() if peer != None]
        self.ui.output('\n'.join(connected))


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

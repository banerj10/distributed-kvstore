import asyncio
import logging
import sys

from network import AsyncNetwork


class UI:
    def __init__(self, evloop, log_level=logging.DEBUG):
        self.evloop = evloop

        self.queue = asyncio.Queue(loop=self.evloop)
        self.evloop.add_reader(sys.stdin, self._on_input)

    def _on_input(self):
        asyncio.ensure_future(
            self.queue.put(sys.stdin.readline()), loop=self.evloop
        )

    async def input(self, prompt=None):
        if prompt is None:
            nodeid = AsyncNetwork.OWN_ID + 1
            prompt = f'NODE {nodeid:02d} >>> '
        print(prompt, end='', flush=True)
        inp = await self.queue.get()
        return inp.strip()

    def output(self, msg):
        print(str(msg))

    def log(self, msg, level=logging.INFO):
        logging.log(level, msg)

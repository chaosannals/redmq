from asyncio import BaseEventLoop
from aiohttp import web
from .action import RedMQAction


class RedMQServer:
    '''
    '''

    def __init__(self, loop: BaseEventLoop, port, host):
        '''
        '''
        
        self.app = web.Application()
        self.action = RedMQAction()
        self.host = host
        self.port = port
        self.loop = loop

    async def serve(self):
        '''
        '''

        self.app.add_routes([
            web.get('/', self.action.index),
            web.post('/attach', self.action.attach),
            web.post('/detach', self.action.detach),
            web.post('/job/push', self.action.push),
            web.post('/job/pull', self.action.pull),
        ])
        self.server = await self.loop.create_server(
            self.app.make_handler(),
            host=self.host,
            port=self.port
        )

    def __enter__(self):
        '''
        '''
        return self

    def __exit__(self, et, ev, tb):
        if self.server is not None:
            self.server.close()
        if self.action is not None:
            self.loop.run_until_complete(self.action.deinit())
        

import os
from loguru import logger
from asyncio import BaseEventLoop
from aiohttp import web
from .action import RedMQAction


class RedMQServer:
    '''
    '''

    def __init__(self, loop: BaseEventLoop):
        '''
        '''
        
        self.app = web.Application()
        self.action = RedMQAction()
        self.host = os.getenv('REDMQ_HOST', '0.0.0.0')
        self.port = os.getenv('REDMQ_PORT', 33000)
        self.loop = loop

    async def serve(self):
        '''
        '''

        self.app.add_routes([
            web.get('/', self.action.index),
            web.post('/attach', self.action.attach),
            web.post('/detach', self.action.detach),
            web.post('/work/push', self.action.push),
            web.post('/work/pull', self.action.pull),
        ])
        logger.info(f'redmq start: http://{self.host}:{self.port}')
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
        '''
        回收资源。
        '''

        if self.server is not None:
            self.server.close()
        if self.action is not None:
            self.loop.run_until_complete(self.action.deinit())
        

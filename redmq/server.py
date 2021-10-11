import os
from loguru import logger
from asyncio import BaseEventLoop
from aiohttp import web
from .action import RedMQAction


class RedMQServer:
    '''
    HTTP 服务端
    '''

    def __init__(self, loop: BaseEventLoop):
        '''
        初始化。
        '''
        
        self.app = web.Application()
        self.action = RedMQAction()
        self.host = os.getenv('REDMQ_HOST', '0.0.0.0')
        self.port = os.getenv('REDMQ_PORT', 33000)
        self.loop = loop

    def __enter__(self):
        '''
        资源初始化。
        '''

        self.app.add_routes([
            web.get('/', self.action.index),
            web.post('/work/info', self.action.info),
            web.post('/work/push', self.action.push),
            web.post('/work/peek', self.action.peek),
            web.post('/work/pull', self.action.pull),
        ])

        return self

    def __exit__(self, et, ev, tb):
        '''
        回收资源。
        '''

        if self.server is not None:
            self.server.close()
        if self.action is not None:
            self.loop.run_until_complete(self.action.deinit())
        
    async def serve(self):
        '''
        启动服务。
        '''

        logger.info(f'redmq start: http://{self.host}:{self.port}')
        self.server = await self.loop.create_server(
            self.app.make_handler(),
            host=self.host,
            port=self.port
        )

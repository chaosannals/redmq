from datetime import datetime
import os
from loguru import logger
from asyncio import BaseEventLoop
from aiohttp import web
from .crypt import decrypt256
from .action import RedMQAction
from .account import Account, init, quit


class RedMQServer:
    '''
    HTTP 服务端
    '''

    def __init__(self, loop: BaseEventLoop):
        '''
        初始化。
        '''

        self.app = web.Application(middlewares=[
            self.authorize
        ])
        self.action = RedMQAction()
        self.host = os.getenv('REDMQ_HOST', '0.0.0.0')
        self.port = os.getenv('REDMQ_PORT', 33000)
        self.loop = loop

    def __enter__(self):
        '''
        资源初始化。
        '''

        self.loop.run_until_complete(init())
        self.app.add_routes([
            web.get('/', self.action.index),
            web.post('/login', self.action.login),
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

        self.loop.run_until_complete(quit())
        if self.server is not None:
            self.server.close()
        if self.action is not None:
            self.loop.run_until_complete(self.action.deinit())

    @web.middleware
    async def authorize(self, request: web.Request, handler):
        '''
        授权判定中间件
        '''

        urlpath = request.url.path

        if not urlpath.startswith('/login'):
            data = await request.json()
            app = data.get('key')
            if app is None:
                return web.json_response({
                    'code': -1,
                    'message': '认证参数无效',
                }, status=400)
            a = await Account.get_or_none(key=app)
            if a is None:
                return web.json_response({
                    'code': -1,
                    'message': '无效账号',
                }, status=400)
            now = datetime.now(tz=a.token_expired_at.tzinfo)
            if a.token_expired_at < now:
                return web.json_response({
                    'code': -1,
                    'message': '认证过期',
                }, status=400)
            token = bytes(a.token, encoding='utf8')
            request['data'] = decrypt256(token, data.get('data'))
            request['token'] = token
            logger.trace('auth {}  path: {}', app, urlpath)

        response = await handler(request)
        return response

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

from datetime import datetime, timedelta
from loguru import logger
from aiohttp.web import json_response, Request
from .account import Account
from .queue import RedMQ
from .crypt import encrypt256, decrypt256, random_text

class RedMQAction:
    '''
    HTTP 请求操作。
    '''

    def __init__(self):
        '''
        初始化。
        '''

        self.queue = RedMQ()

    async def deinit(self):
        '''
        析构。
        '''

    async def index(self, request: Request):
        '''
        提示页
        '''

        return json_response({
            'code': 0,
        })

    async def login(self, request: Request):
        '''
        
        '''

        data = await request.json()
        app = data.get('app')
        key = data.get('key')
        ekey = data.get('ekey')

        if app is None or key is None or ekey is None:
            return json_response({
                'code': -1,
                'message': '验证信息不可空'
            })

        a = await Account.get_or_none(key=app)

        if a is None:
            logger.warning('账号：{} 无效', app)
            return json_response({
                'code': -1,
                'message': '无效账号'
            })

        secret = bytes(a.secret, encoding='utf8')
        nkey = decrypt256(secret, ekey)
        if nkey != key:
            logger.warning('账号：{} 验证无效 {} {} {} {}', app, secret, key, nkey, ekey)
            return json_response({
                'code': -1,
                'message': '密码错误'
            })
        
        a.token = random_text()
        a.token_expired_at = datetime.now() + timedelta(hours=2)
        await a.save()
        logger.info('账号：{} 登录', app)

        return json_response({
            'code': 0,
            'data': encrypt256(secret, {
                'app': a.key,
                'token': a.token,
                'expired_at': a.token_expired_at.strftime('%Y-%m-%d %H:%M:%S')
            })
        })

    async def info(self, request: Request):
        '''
        获取信息。
        '''

        data = request['data']
        token = request['token']

        if 'queue' not in data:
            return json_response({
                'code': -1,
                'message': '请指定队列名',
            }, status=400)

        info = await self.queue.info(data['queue'])

        return json_response({
            'code': 0,
            'data': encrypt256(token, info),
        })

    async def push(self, request: Request):
        '''
        插入队列。
        '''

        data = request['data']
        token = request['token']

        if 'queue' not in data:
            return json_response({
                'code': -1,
                'message': '请指定队列。'
            })

        result = await self.queue.push(
            data['queue'],
            data['data']
        )

        return json_response({
            'code': 0,
            'data': encrypt256(token, result),
            'message': 'Ok'
        })

    async def pull(self, request: Request):
        '''
        消费队列。
        '''

        data = request['data']
        token = request['token']

        return json_response({
            'code': 0,
            'message': 'Ok'
        })

    async def peek(self, request:Request):
        '''
        窥探队列。
        '''

        data = request['data']
        token = request['token']

        if 'queue' not in data:
            return json_response({
                'code': -1,
                'message': '请指定队列。'
            })

        info = await self.queue.peek(data['queue'])

        return json_response({
            'code': 0,
            'data': encrypt256(token, info),
        })

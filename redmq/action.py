from loguru import logger
from aiohttp.web import json_response, Request
from .queue import RedMQ


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
        

        return json_response({
            'code': 1,
        })

    async def info(self, request: Request):
        '''
        获取信息。
        '''

        data = await request.json()

        if 'queue' not in data:
            return json_response({
                'code': -1,
                'message': '请指定队列名',
            }, status=400)

        info = await self.queue.info(data['queue'])

        return json_response({
            'code': 0,
            'info': info,
        })

    async def push(self, request: Request):
        '''
        插入队列。
        '''

        data = await request.json()

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
            'result': result,
            'message': 'Ok'
        })

    async def pull(self, request: Request):
        '''
        消费队列。
        '''

        data = await request.json()

        return json_response({
            'code': 0,
            'message': 'Ok'
        })

    async def peek(self, request:Request):
        '''
        窥探队列。
        '''

        data = await request.json()

        if 'queue' not in data:
            return json_response({
                'code': -1,
                'message': '请指定队列。'
            })

        info = await self.queue.peek(data['queue'])

        return json_response({
            'code': 0,
            'data': info,
        })

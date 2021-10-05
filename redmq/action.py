import os
from loguru import logger
from aioredis import Redis, ConnectionPool
from aiohttp.web import  Response, json_response, Request

class RedMQAction:
    '''
    '''

    def __init__(self):
        '''
        '''
        
        host = os.getenv('REDMQ_REDIS_HOST', '127.0.0.1')
        port = os.getenv('REDMQ_REDIS_PORT', 6379)
        self.pool = ConnectionPool.from_url(
            url = f'redis://{host}:{port}',
            decode_responses=True
        )
        self.redis = Redis(connection_pool=self.pool)
        

    async def deinit(self):
        '''
        '''
        
        await self.pool.disconnect()
        

    async def index(self, request: Request):
        '''
        '''
        

        return Response()

    async def push(self, request: Request):
        '''
        '''

        data = await request.json()

        logger.debug(data)

        await self.redis.xadd(
            name=data['queue'] if 'queue' in data else 'default',
            fields=data['data']
        )

        return json_response({
            'code': 0,
            'message': 'Ok'
        })

    async def pull(self, request: Request):
        '''
        '''

        data = await request.json()

        return json_response({
            'code': 0,
            'message': 'Ok'
        })

    async def attach(self, request):
        '''
        '''

        data = await request.json()
        return json_response({
            'code': 0,
            'message': 'Ok'
        })

    async def detach(self, request):
        '''
        '''

        data = await request.json()
        return json_response({
            'code': 0,
            'message': 'Ok'
        })

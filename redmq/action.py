import os
from aioredis import Redis, ConnectionPool
from aiohttp.web import  Response

class RedMQAction:
    '''
    '''

    def __init__(self):
        host = os.getenv('REDMQ_REDIS_HOST', '127.0.0.1')
        port = os.getenv('REDMQ_REDIS_PORT', 6379)
        self.pool = ConnectionPool.from_url(
            url = f'redis://{host}:{port}',
            decode_responses=True
        )
        self.redis = Redis(connection_pool=self.pool)

    async def deinit(self):
        await self.pool.disconnect()
        

    async def index(self, request):
        '''
        '''

        return Response()

    async def push(self, request):
        '''
        '''

    async def pull(self, request):
        '''
        '''

        data = request.post()

    async def attach(self, request):
        '''
        '''

    async def detach(self, request):
        '''
        '''

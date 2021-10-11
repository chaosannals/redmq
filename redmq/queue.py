import json
import os
from loguru import logger
from aioredis import Redis, ConnectionPool


class RedMQ:
    '''
    队列管理
    '''

    def __init__(self):
        '''
        初始化。
        '''

        host = os.getenv('REDMQ_REDIS_HOST', '127.0.0.1')
        port = os.getenv('REDMQ_REDIS_PORT', 6379)
        self.pool = ConnectionPool.from_url(
            url=f'redis://{host}:{port}',
            decode_responses=True
        )
        self.redis = Redis(connection_pool=self.pool)

    async def info(self, name):
        '''
        查看队列信息。
        '''

        r = await self.redis.xinfo_stream(name=name)
        r['groups'] = await self.redis.xinfo_groups(name)
        for g in r['groups']:
            gn = g['name']
            g['consumers'] = await self.redis.xinfo_consumers(name, gn)
        return r

    async def push(self, name, data):
        '''
        向指定队列插入任务。
        '''

        return await self.redis.xadd(
            name=name,
            fields={
                'retry_count': 0,
                'data': json.dumps(data, ensure_ascii=False)
            }
        )

    async def pull(self, name, ids):
        '''
        '''

    async def peek(self, name, count=1):
        '''
        窥探指定队列的信息。
        '''

        return await self.redis.xread({
            name: '0-0',
        }, count=count, block=100)

    async def save(self, rid, info):
        '''
        '''

    async def group_create(self, queue, name, sid='0'):
        '''
        '''

        return await self.redis.xgroup_create(queue, name, sid)

    async def group_destroy(self, queue, name):
        '''
        删除消息组。
        '''

        return await self.redis.xgroup_destroy(queue, name)

    async def group_info(self, queue, name):
        '''
        读取队列组信息的任务。
        '''

        s = await self.redis.xinfo_stream(queue)
        c = await self.redis.xinfo_consumers(queue, name)
        p = await self.redis.xpending(queue, name)

        return {
            'info': s,
            'consumers': c,
            'pendings': p,
        }

    async def group_pull(self, queue, name, consumer):
        '''
        读取任务。
        '''

        return await self.redis.xreadgroup(
            groupname=name,
            consumername=consumer,
            streams={queue: '>'},
            count=1,
            block=100
        )

    async def group_ack(self, queue, name, *ids):
        '''
        '''

        return await self.redis.xack(
            name=queue,
            groupname=name,
            ids=ids
        )

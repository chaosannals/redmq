import json
import os
from datetime import datetime
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

    async def count(self, name):
        '''
        查询长度。
        '''

        return await self.redis.xlen(name)

    async def push(self, name, data):
        '''
        向指定队列插入任务。
        '''

        return await self.redis.xadd(
            name=name,
            fields={
                'create_at':  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data': json.dumps(data, ensure_ascii=False)
            }
        )

    async def pull(self, name, ids):
        '''
        拉取指定队列信息。
        '''

    async def peek(self, name, count=1):
        '''
        窥探指定队列的信息。
        '''

        return await self.redis.xread({
            name: '0-0',
        }, count=count, block=100)

    async def remove(self, queue, *ids):
        '''

        '''

        return await self.redis.xdel(queue, *ids)

    async def find(self, name, sid):
        '''
        
        '''
        return await self.redis.xrange(name, sid, count=1)

    async def range(self, name, min_id='-', max_id='+', count=None):
        '''

        '''

        return await self.redis.xrange(name, min_id, max_id, count)

    async def save(self, rid, info):
        '''
        '''

    async def group_create(self, queue, name, sid='0', mkstream=False):
        '''
        创建消息组。
        '''

        return await self.redis.xgroup_create(queue, name, sid, mkstream)

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

    async def group_pull(self, queue, name, consumer, mid='>', count=1):
        '''
        读取任务。
        '''

        return await self.redis.xreadgroup(
            groupname=name,
            consumername=consumer,
            streams={queue: mid},
            count=count,
            block=100
        )

    async def group_pend(self, queue, name, consumer, min_id='-', max_id='+', count=100):
        '''
        读取 pandings
        '''

        return await self.redis.xpending_range(
            name=queue,
            groupname=name,
            min=min_id,
            max=max_id,
            count=count,
            consumername=consumer
        )

    async def group_ack(self, queue, name, *ids):
        '''
        提交任务。
        '''

        return await self.redis.xack(queue, name, *ids)

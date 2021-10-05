import os
import inspect
from importlib import import_module
from pkgutil import iter_modules
from multiprocessing import Process
from aioredis import ConnectionPool
from loguru import logger
from .worker import RedMQWorker

class RedMQContractor:
    '''
    '''

    def __init__(self):
        '''
        '''

        self.workspace = os.getenv('REDMQ_WORKSPACE', 'works')
        self.queues = {}
        host = os.getenv('REDMQ_REDIS_HOST', '127.0.0.1')
        port = os.getenv('REDMQ_REDIS_PORT', 6379)
        self.pool = ConnectionPool.from_url(
            url = f'redis://{host}:{port}',
            decode_responses=True
        )
        self.process = Process(target=self._dispatch)

    def _dispatch(self):
        '''
        '''

        while True:
            pass


    def find_worker(self, m):
        '''
        '''
        def predicate(v):
            if not inspect.isclass(v):
                return False
            if v == RedMQWorker:
                return False
            return issubclass(v, RedMQWorker)

        for n, v in inspect.getmembers(m, predicate=predicate):
            return n
        return None
    
    def contract(self, m=None):
        '''
        递归获取指定工作空间的队列任务信息。
        '''

        # 最外层，赋予指定包名
        if m is None:
            m = import_module(self.workspace)
        n = m.__name__
        
        # 执行分析。
        w = self.find_worker(m)
        if w is not None:
            self.queues[n] = w

        # 递归
        if m.__loader__.is_package(n):
            for _, child, _ in iter_modules(m.__path__):
                mn = f'{n}.{child}'
                m = import_module(mn)
                self.contract(m)

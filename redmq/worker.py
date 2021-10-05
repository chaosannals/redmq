import os
import abc
from time import sleep
from multiprocessing import Process
from aioredis import Redis
from dotenv import load_dotenv
from loguru import logger


class RedMQWorker:
    '''
    '''

    def __init__(self, queue, pool):
        '''
        '''

        self.queue = queue
        self.process = Process(target=self._work)
        self.redis = Redis(connection_pool=pool)

    def _init(self):
        '''
        '''

        load_dotenv()
        is_debug = os.getenv('REDMQ_DEBUG', False)

        # 日志初始化
        log_level = os.getenv('REDMQ_LOG_LEVEL', 'TRACE' if is_debug else 'INFO')
        logger.remove()
        logger.add(
            f'runtime/logs/{{time:YYYY-MM-DD}}-{self.queue}.log',
            level=log_level,
            rotation='00:00',
            retention='7 days',
            encoding='utf8'
        )
        logger.info(f'mode: {"debug" if is_debug else "release"}')

    def _loop(self):
        '''
        '''

    def _work(self):
        '''
        '''

        self._init()
        while True:

            data = self.redis.xread(
                self.queue,
                count=1
            )

            self.on_work(data)

            # 让出 CPU
            sleep(2)

    @abc.abstractmethod
    def on_work(self, data):
        pass

    @abc.abstractmethod
    def on_fail(self, data):
        pass

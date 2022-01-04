import asyncio
import os
import abc
import json
from time import sleep
from multiprocessing import Process
from dotenv import load_dotenv
from loguru import logger
from aioredis.exceptions import ResponseError
from .queue import RedMQ


class RedMQWorker:
    '''
    队列执行进程，每个任务独立一个。
    '''

    def __init__(self, queue):
        '''
        初始化。
        '''

        self.queue = None  # 需要在进程程序里初始化
        self.queue_name = queue
        self.process = Process(target=self._work)

    def _init(self):
        '''
        初始化进程。
        '''

        load_dotenv()
        is_debug = os.getenv('REDMQ_DEBUG', False)

        # 日志初始化
        log_level = os.getenv(
            'REDMQ_LOG_LEVEL', 'TRACE' if is_debug else 'INFO')
        logger.remove()
        logger.add(
            f'runtime/logs/{{time:YYYY-MM-DD}}-{self.queue_name}.log',
            level=log_level,
            rotation='00:00',
            retention='7 days',
            encoding='utf8'
        )
        logger.info(f'mode: {"debug" if is_debug else "release"}')

        self.queue = RedMQ()

    async def _try(self, d, group_name, consumer_name, is_retry=False):
        '''
        执行任务。
        '''

        try:
            tid = d[0]
            data = d[1]
            data['id'] = tid
            if 'data' in data:
                data['data'] = json.loads(data['data'])
            if is_retry:
                p = await self.queue.group_pend(
                    self.queue_name,
                    group_name,
                    consumer_name,
                    min_id=tid,
                    count=1
                )
                if len(p) > 0:
                    data['try_count'] = p[0]['times_delivered']
            else:
                data['try_count'] = 1

            if self.on_work(data):
                logger.info(
                    f'queue: {self.queue_name} group: {group_name} consumer: {consumer_name} d: {d}')
                await self.queue.group_ack(self.queue_name, group_name, tid)
                await self.queue.remove(self.queue_name, tid)
        except Exception as e:
            self.on_fail(d, e)
            logger.error(
                f'queue: {self.queue_name} group: {group_name} consumer: {consumer_name} d: {d} e: {e}')

    async def _loop(self, loop, pid):
        '''
        进程异步主循环
        '''

        group_name = f'redmq-group-{self.queue_name}'
        consumer_name = f'redmq-consumer-{self.queue_name}'

        logger.info(
            f'queue: {self.queue_name} group: {group_name} consumer: {consumer_name} pid: {pid}')
        try:
            await self.queue.group_create(self.queue_name, group_name, mkstream=True)
            logger.info(
                f'queue: {self.queue_name} group: {group_name} created.')
        except ResponseError as e:
            logger.warning(f'queue: {self.queue_name} {e}')

        try:
            while True:
                r = await self.queue.group_pull(self.queue_name, group_name, consumer_name)
                if len(r) == 0:  # 空队列尝试失败任务
                    logger.debug(f'queue: {self.queue_name} is empty.')
                    rp = await self.queue.group_pull(self.queue_name, group_name, consumer_name, 0)
                    if len(rp[0][1]) > 0:
                        logger.debug(f'rp: {rp}')
                        await self._try(rp[0][1][0], group_name, consumer_name, is_retry=True)
                    else:
                        await asyncio.sleep(10)
                    continue
                await self._try(r[0][1][0], group_name, consumer_name)
        except ResponseError as e:
            logger.error(f'queue: {self.queue_name} {e}')
            await asyncio.sleep(10)
            logger.info(f'queue: {self.queue_name} restart')
            await self._loop(loop, pid)

    def _work(self):
        '''
        进程主函数
        '''

        try:
            self._init()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._loop(loop, self.process.pid))
        except KeyboardInterrupt as e:
            logger.info('keyboard interrupt')
        except Exception as e:
            logger.error(f'{type(e)}: {e} | {e.__traceback__}')

    def work(self):
        '''
        启动。
        '''

        self.process.start()

    @abc.abstractmethod
    def on_work(self, info) -> bool:
        '''
        任务处理事件。
        '''
        pass

    @abc.abstractmethod
    def on_fail(self, info, e):
        '''
        任务失败处理事件。
        '''
        pass

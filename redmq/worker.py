import os
import abc
from time import sleep
from multiprocessing import Process
from dotenv import load_dotenv
from loguru import logger
from .queue import RedMQ


class RedMQWorker:
    '''
    队列执行进程，每个任务独立一个。
    '''

    def __init__(self, queue):
        '''
        初始化。
        '''

        self.queue = None # 需要在进程程序里初始化
        self.queue_name = queue
        self.process = Process(target=self._work)

    def _init(self):
        '''
        初始化进程。
        '''

        load_dotenv()
        is_debug = os.getenv('REDMQ_DEBUG', False)

        # 日志初始化
        log_level = os.getenv('REDMQ_LOG_LEVEL', 'TRACE' if is_debug else 'INFO')
        logger.remove()
        logger.add(
            f'runtime/logs/{{time:YYYY-MM-DD}}-{self.queue_name}.log',
            level=log_level,
            rotation='00:00',
            retention='7 days',
            encoding='utf8'
        )
        self.queue = RedMQ()
        
        logger.info(f'mode: {"debug" if is_debug else "release"}')

    def _loop(self):
        '''
        '''

    def _work(self):
        '''
        '''
        try:
            self._init()
            while True:
                logger.debug(f'work: {self.queue_name}')
                sleep(10)

                # data = self.queue.peek()

                # if data is not None:
                #     try:
                #         self.on_work(data)
                #     except Exception as e:
                #         self.on_fail(data, e)
                # else:
                #     # 让出 CPU
                #     sleep(2)
        except KeyboardInterrupt as e:
            logger.info('keyboard interrupt')
        except Exception as e:
            logger.error(e)
    
    def work(self):
        '''
        启动。
        '''

        self.process.start()

    @abc.abstractmethod
    def on_work(self, data):
        '''
        任务处理事件。
        '''
        pass

    @abc.abstractmethod
    def on_fail(self, data, e):
        '''
        任务失败处理事件。
        '''
        pass

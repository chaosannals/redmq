import os
import sys
import inspect
from time import sleep
from importlib import import_module
from pkgutil import iter_modules
from multiprocessing import Process
from loguru import logger
from dotenv import load_dotenv
from .worker import RedMQWorker


class RedMQContractor:
    '''
    遍历工作空间，获取操作并启用 worker。
    '''

    def __init__(self):
        '''
        初始化。
        '''

        self.workers = {}
        self.workspace = os.getenv('REDMQ_WORKSPACE', 'works')
        self.process = Process(target=self._work)

    def _init(self):
        '''
        '''
        load_dotenv()
        is_debug = os.getenv('REDMQ_DEBUG', False)

        # 日志初始化
        log_level = os.getenv(
            'REDMQ_LOG_LEVEL', 'TRACE' if is_debug else 'INFO')
        logger.remove()
        logger.add(
            f'runtime/logs/{{time:YYYY-MM-DD}}-contractor.log',
            level=log_level,
            rotation='00:00',
            retention='7 days',
            encoding='utf8'
        )
        logger.info(f'mode: {"debug" if is_debug else "release"}')

    def _work(self):
        '''
        进程主函数。
        '''

        try:
            self._init()

            while True:
                sleep(10)
                pass
        except KeyboardInterrupt as e:
            logger.info('keyboard interrupt')
        except Exception as e:
            logger.error(e)

    def contract(self):
        '''
        递归获取指定工作空间的队列任务信息。
        '''

        # 队列工作进程
        m = import_module(self.workspace)
        self.workers = self.dispatch(m)
        for k, v in self.workers.items():
            m = import_module(k)
            wc = getattr(m, v)
            worker = wc()
            logger.info(f'contract: {k} => {v}')
            worker.work()
        
        # 监控进程
        self.process.start()
        logger.info(f'start contractor.')

    @classmethod
    def dispatch(cls, m):
        '''
        '''

        # 最外层，赋予指定包名
        result = {}
        n = m.__name__

        # 执行分析。
        w = cls.find_worker(m)
        if w is not None and n not in result:
            result[n] = w

        # 递归
        if m.__loader__.is_package(n):
            for _, child, _ in iter_modules(m.__path__):
                mn = f'{n}.{child}'
                m = import_module(mn)
                r = cls.dispatch(m)
                result.update(r)
        return result

    @classmethod
    def find_worker(cls, m):
        '''
        模块里面查找 Worker 子类。
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

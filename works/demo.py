from loguru import logger
from redmq.worker import RedMQWorker

class DemoWork(RedMQWorker):
    '''
    示例任务
    '''

    def __init__(self):
        '''
        初始化
        '''
        super().__init__(__name__)

    def on_work(self, info):
        '''
        任务执行
        '''

        logger.info(info)
        if info['try_count'] > 10:
            return True
        if info['data']['bbb'] >= 9:
            return False
        return True



    def on_fail(self, info, e):
        '''
        失败处理
        '''

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

    def on_work(self, data):
        '''
        任务执行
        '''

    def on_fail(self, data, e):
        '''
        失败处理
        '''

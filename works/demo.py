from redmq.worker import RedMQWorker

class DemoWork(RedMQWorker):
    '''
    示例任务
    '''

    def __init__(self):
        '''
        初始化
        '''

    def on_work(self, data):
        '''
        任务执行
        '''

    def on_fail(self, data):
        '''
        失败处理
        '''

from redmq.worker import RedMQWorker

class TestWork(RedMQWorker):
    '''
    '''

    def __init__(self):
        '''
        '''
        super().__init__(__name__)

    def on_work(self, data):
        '''
        '''

    def on_fail(self, data, e):
        '''
        '''
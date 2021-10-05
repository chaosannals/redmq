import asyncio
import os
import sys
from multiprocessing import freeze_support
from loguru import logger
from dotenv import load_dotenv
from redmq import contractor
from redmq.server import RedMQServer
from redmq.contractor import RedMQContractor


def main():
    '''
    '''

    try:
        # 初始化
        is_debug = os.getenv('REDMQ_DEBUG', False)
    
        # 日志初始化
        log_level = os.getenv('REDMQ_LOG_LEVEL', 'TRACE' if is_debug else 'INFO')
        logger.remove()
        logger.add(
            sink=sys.stdout,
            level='INFO'
        )
        logger.add(
            'runtime/logs/{time:YYYY-MM-DD}.log',
            level=log_level,
            rotation='00:00',
            retention='7 days',
            encoding='utf8'
        )
        logger.info(f'mode: {"debug" if is_debug else "release"}')

        # 启动工作器
        contractor = RedMQContractor()
        contractor.contract()

        # 执行服务器循环
        loop = asyncio.get_event_loop()
        with RedMQServer(loop) as server:
            loop.run_until_complete(server.serve())
            loop.run_forever()
    except KeyboardInterrupt as e:
        logger.info('keyboard interrupt')
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    freeze_support()
    load_dotenv()
    main()

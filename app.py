import asyncio
import os
from loguru import logger
from redmq import major_init
from redmq.server import RedMQServer


def main():
    '''
    '''

    try:
        # 初始化
        major_init()

        # 执行循环
        host = os.getenv('REDMQ_HOST', '0.0.0.0')
        port = os.getenv('REDMQ_PORT', 33000)
        loop = asyncio.get_event_loop()
        with RedMQServer(loop, host=host, port=port) as server:
            loop.run_until_complete(server.serve())
            loop.run_forever()
    except KeyboardInterrupt as e:
        logger.info('keyboard interrupt')
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    main()

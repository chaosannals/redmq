import sys
import os
from dotenv import load_dotenv
from loguru import logger

def minor_init(number):
    '''
    '''

    load_dotenv()
    is_debug = os.getenv('REDMQ_DEBUG', False)
    # 日志初始化
    log_level = os.getenv('REDMQ_LOG_LEVEL', 'TRACE' if is_debug else 'INFO')
    logger.remove()
    logger.add(
        f'runtime/logs/{{time:YYYY-MM-DD}}-{number}.log',
        level=log_level,
        rotation='00:00',
        retention='7 days',
        encoding='utf8'
    )
    logger.info(f'mode: {"debug" if is_debug else "release"}')

def major_init():
    '''
    '''

    # 加载环境变量
    load_dotenv()
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
from loguru import logger
from tortoise import Tortoise
from tortoise.fields import *
from tortoise.models import Model

class Account(Model):
    class Meta:
        table='rmq_account'

    key = CharField(32)
    secret = CharField(32)
    token = CharField(32, null=True)
    token_expired_at = DatetimeField(null=True)
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

async def init():
    '''
    
    '''

    await Tortoise.init(
        db_url='sqlite://runtime/redmq.db',
        modules={
            'models': [
                'redmq.account',
            ],
        },
    )

    await Tortoise.generate_schemas()

    ac = await Account.all().count()
    if ac == 0:
        key='demokey'
        secret='1234567890ABCEDF1234567890ABCEDF'
        logger.info('创建初始账号: {} | {}', key, secret)
        await Account.create(key=key, secret=secret)

async def quit():
    '''
    
    '''
    
    await Tortoise.close_connections()
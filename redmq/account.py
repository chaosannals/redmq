from enum import unique
from tortoise import Tortoise
from tortoise.fields import *
from tortoise.models import Model

class Account(Model):
    class Meta:
        table_name='rmq_account'

    key = CharField(32)
    secret = CharField(32)
    token = CharField(32)
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

async def init():
    '''
    
    '''

    await Tortoise.init(
        db_url='sqlite://redmq.db',
        modules={
            'models': [
                'redmq.account',
            ],
        },
    )

    await Tortoise.generate_schemas()

async def quit():
    '''
    
    '''
    
    await Tortoise.close_connections()
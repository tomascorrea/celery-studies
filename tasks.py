# -*- coding: utf-8 -*-

from datetime import datetime
import pylibmc

from celery import Celery, current_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
from celery.utils.log import get_task_logger

celery = Celery('tasks', backend='amqp', broker='amqp://guest@localhost//')

logger = get_task_logger(__name__)


@celery.task(name='tasks.task_without_rate_limit')
def task_without_rate_limit():

    mc = pylibmc.Client(["127.0.0.1"], binary=True, behaviors={"tcp_nodelay": True, "ketama": True})

    now = datetime.now()
    now = now.replace(microsecond=0)

    key = "task_without_rate_limit-%s" % now

    obj = mc.get(key)
    if not obj:
        mc.set(key, 1)
    else:
        mc.incr(key)
    
    logger.info('Ola')


@celery.task(name='tasks.task_with_rate_limit', rate_limit="10/s")
def task_with_rate_limit():

    mc = pylibmc.Client(["127.0.0.1"], binary=True, behaviors={"tcp_nodelay": True, "ketama": True})

    now = datetime.now()
    now = now.replace(microsecond=0)

    key = "task_with_rate_limit-%s" % now

    obj = mc.get(key)
    if not obj:
        mc.set(key, 1)
    else:
        mc.incr(key)
    
    logger.info('Ola')

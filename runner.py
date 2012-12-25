#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import pylibmc
from datetime import datetime
import time


from komandr import *
from tasks import task_without_rate_limit, task_with_rate_limit, celery


@command
def call_task(times=None):
    if not times or not times.isdigit:
        task_with_rate_limit.apply_async()
        task_without_rate_limit.apply_async()

    else:
        for a in range(int(times)):
            task_with_rate_limit.apply_async()
            task_without_rate_limit.apply_async()


@command
def change_rate_limit(limit):
    celery.control.rate_limit('tasks.task_with_rate_limit', limit)


@command
def actual_execution_rate():
    
    mc = pylibmc.Client(["127.0.0.1"], binary=True, behaviors={"tcp_nodelay": True, "ketama": True})


    while True:

        time.sleep(0.01)

        now = datetime.now()
        now = now.replace(microsecond=0)

        key_without = "task_without_rate_limit-%s" % now
        key_with = "task_with_rate_limit-%s" % now

        
        rate_witout = mc.get(key_without) or 0
        rate_with = mc.get(key_with) or 0

        
        print('%s - with: %03d - without: %03d\r' % (now, rate_with, rate_witout), end='')
        



main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from kombu import Queue, Exchange
from kombu.common import Broadcast
from .taskutils import get_tasks_registry
from .confreader import conf
from .ha import redis_ha_switch_on, redis_sentinels, redis_master_name, redis_password, redis_db

broker_url = conf.get('config', 'broker_url')

if redis_ha_switch_on:
    result_backend_transport_options = {'sentinels': redis_sentinels,
                                        'master_name': redis_master_name,
                                        'sentinel_kwargs': {'password': redis_password, 'db': redis_db}}
else:
    result_backend = conf.get('config', 'backend_url')

include = []
for k in get_tasks_registry().keys():
    include.append(k)

enable_utc = True
timezone = conf.get('config', 'timezone')
task_serializer = 'json'
result_serializer = 'json'
task_send_sent_event = True
worker_max_tasks_per_child = 5
# task_soft_time_limit = 2
# task_time_limit = 10
# task_ignore_result = True

# ------------begin custom settings------------- #
# server_name = ('server01', 'server02')
# ------------end custom settings------------- #

task_queues = [
    Queue('default', Exchange('default'), routing_key='default'),

    # Queue(server_name[0] + '_1', Exchange(server_name[0]), routing_key=server_name[0] + '_1'),
    # Queue(server_name[0] + '_2', Exchange(server_name[0]), routing_key=server_name[0] + '_2'),
    # Queue(server_name[0] + '_3', Exchange(server_name[0]), routing_key=server_name[0] + '_3'),
    #
    # Queue(server_name[1] + '_1', Exchange(server_name[1]), routing_key=server_name[1] + '_1'),
    # Queue(server_name[1] + '_2', Exchange(server_name[1]), routing_key=server_name[1] + '_2'),
    # Queue(server_name[1] + '_3', Exchange(server_name[1]), routing_key=server_name[1] + '_3'),

    Queue('SHARE_Q', Exchange('SHARE_Q'), routing_key='SHARE_Q'),

    Queue('DEDICATED_Q', Exchange('DEDICATED_Q'), routing_key='DEDICATED_Q'),

    Broadcast('broadcast_tasks'),
]

task_default_queue = 'default'
task_default_exchange_type = 'direct'
task_default_routing_key = 'default'

beat_schedule = {
    'send-every-10-seconds': {
        'task': 'keep_conn_alive',
        'schedule': 10.0,
        'args': ('alive',),
        'options': {'queue': 'broadcast_tasks'}
    },
}

# task_annotations = {'tasks.add': {'rate_limit': '10/s'}}
# result_expires=3600

# task_routes = {
#          'myproj.tasks.add' : {'queue':'q1'},
#          'myproj.tasks.add' : {'queue':'q2'},
#     }

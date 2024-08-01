import os
from celery import Celery 
from datetime import timedelta

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Eshop.settings')

celery_app = Celery('Eshop')


celery_app.autodiscover_tasks()

celery_app.conf.broker_url = 'amqp://'
celery_app.conf.result_backend = 'rpc://'
celery_app.conf.task_serializer = 'json'
celery_app.conf.result_serializer = 'pickle'
celery_app.conf.accept_content = ['json', 'pickle']
celery_app.conf.result_expires = timedelta(days=1)
celery_app.conf.task_always_eager = False
celery_app.conf.worker_prefetch_multiplier = 4


# celery_app.conf.update(
#     broker_url='amqp://',
#     result_backend='rpc://',
#     task_serializer='json',
#     result_serializer='pickle',
#     accept_content=['json', 'pickle'],
#     result_expires=timedelta(days=1),
#     task_always_eager=False,
#     worker_prefetch_multiplier=4,
#     broker_connection_retry_on_startup=True,
# )

    

from celery import Celery
import settings

__author__ = 'kenneth'

app = Celery('tasks', broker=settings.BROKER_URL)


@app.task
def fetch_all_messages(org):
    pass
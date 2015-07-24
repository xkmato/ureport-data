from celery import Celery
from temba.base import TembaException
from models import Org, BaseDocument
import settings

__author__ = 'kenneth'

app = Celery('tasks', broker=settings.BROKER_URL)

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERYBEAT_SCHEDULE=settings.CELERYBEAT_SCHEDULE
)


@app.task
def fetch_all(entities=None):
    print "Started Here"
    if not entities:
        entities = [cls for cls in BaseDocument.__subclasses__()]
    assert iter(entities)
    for org in Org.find():
        for entity in entities:
            try:
                entity.fetch_objects(org)
            except TembaException as e:
                print e #Todo Do something with this guy
                continue

import logging
from celery import Celery
from temba.base import TembaException, TembaPager

from ureport_data.models import Org, BaseDocument
import settings

logging.basicConfig(format=settings.FORMAT)
logger = logging.getLogger("tasks")

__author__ = 'kenneth'

app = Celery('ureport_data.tasks', broker=settings.BROKER_URL)

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERYBEAT_SCHEDULE=settings.CELERYBEAT_SCHEDULE
)

#b87eb439834a8069a0f0dd213c3fcc56f78b8781

@app.task
def fetch_all(entities=None):
    print "Started Here"
    if not entities:
        entities = [cls for cls in BaseDocument.__subclasses__()]
    assert iter(entities)
    for org in Org.find({"is_active": True}):
        for entity in entities:
            try:
                n = 1
                while True:
                    entity.fetch_objects(org, pager=TembaPager(n))
                    n += 1
            except TembaException as e:
                logger.error("Temba is misbehaving: %s", str(e))
                continue
            except Exception as e:
                logger.error("Things are dead: %s", str(e))

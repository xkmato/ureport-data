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


@app.task
def fetch_all(entities=None, orgs=None):
    print "Started Here"
    if not entities:
        entities = [dict(name=cls) for cls in BaseDocument.__subclasses__()]
    if not orgs:
        orgs = Org.find({"is_active": True})
    else:
        orgs = [Org.find_one({'api_token': api_key}) for api_key in orgs]
    assert iter(entities)
    for org in orgs:
        for entity in entities:
            try:
                n = entity.get('start_page', 1)
                while True:
                    entity.get('name').fetch_objects(org, pager=TembaPager(n))
                    n += 1
            except TembaException as e:
                logger.error("Temba is misbehaving: %s", str(e))
                continue
            except Exception as e:
                logger.error("Things are dead: %s", str(e))
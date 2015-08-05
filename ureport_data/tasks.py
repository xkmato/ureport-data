import logging
from celery import Celery
from datetime import datetime
import requests
from retrying import retry
from temba.base import TembaException, TembaPager, TembaAPIError, TembaConnectionError

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


def retry_if_temba_api_or_connection_error(exception):
    if isinstance(exception, TembaAPIError) and isinstance(exception.caused_by,
                                                           requests.HTTPError
                                                           ) and 399 < exception.caused_by.response.status_code < 499:
        return False
    return isinstance(exception, TembaAPIError) or isinstance(exception, TembaConnectionError)


@retry(retry_on_exception=retry_if_temba_api_or_connection_error, stop_max_attempt_number=settings.RETRY_MAX_ATTEMPTS,
       wait_fixed=settings.RETRY_WAIT_FIXED)
def fetch_entity(entity, org, n):
    logger.info("Trying to fetch Object: %s for Org:%s Page %s at %s", str(entity.__class__), org.name, str(n),
                str(datetime.now()))
    entity.get('name').fetch_objects(org, pager=TembaPager(n))


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
                    fetch_entity(entity, org, n)
                    n += 1
            except TembaException as e:
                logger.error("Temba is misbehaving: %s", str(e))
                continue
            except Exception as e:
                logger.error("Things are dead: %s", str(e))

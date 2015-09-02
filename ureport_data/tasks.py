import logging
import traceback
from celery import Celery
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
                                                           ) and 399 < exception.caused_by.response.status_code < 500:
        return False
    logger.warning("Raised an exception: %s - Retrying in %s minutes", str(exception),
                   str(settings.RETRY_WAIT_FIXED/60000))
    return isinstance(exception, TembaAPIError) or isinstance(exception, TembaConnectionError)


@retry(retry_on_exception=retry_if_temba_api_or_connection_error, stop_max_attempt_number=settings.RETRY_MAX_ATTEMPTS,
       wait_fixed=settings.RETRY_WAIT_FIXED)
def fetch_entity(entity, org, n, af=None):
    flows = entity.get('flows', None)
    entity = entity.get('name')
    logger.info("Fetching Object of type: %s for Org: %s on Page %s", str(entity), org.name, str(n))
    if flows:
        logger.info("Fetching Runs for flows %s", str(flows))
        entity.fetch_objects(org, pager=TembaPager(n), af=af, **{'flows': flows})
    else:
        entity.fetch_objects(org, pager=TembaPager(n), af=af)


@app.task
def fetch_all(entities=None, orgs=None, af=None):
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
                logger.info('Entity %s' % entity)
                n = entity.get('start_page', 1)
                while True:
                    fetch_entity(entity, org, n, af=af)
                    n += 1
            except TembaException as e:
                logger.error("Temba is misbehaving: %s - No retry", str(e))
                continue
            except Exception as e:
                logger.error("Things are dead: %s - No retry", str(traceback.format_exc()))

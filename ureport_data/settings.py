import datetime
import os
from pymongo import Connection

DATABASE = "rapidpro"
CONNECTION = Connection()
FORMAT = '%(asctime)-15s %(message)s'
SITE_API_HOST = 'https://app.rapidpro.io/api/v1'
API_ENDPOINT = 'https://app.rapidpro.io/api/v1'
BROKER_URL = 'redis://'

cron_minutes = int(os.environ.get('FETCH_SLEEP', 60*24*7))

CELERYBEAT_SCHEDULE = {
    'sync-contacts': {
        'task': 'ureport_data.tasks.fetch_all',
        'schedule': datetime.timedelta(minutes=cron_minutes),
        'args': ()
    }
}
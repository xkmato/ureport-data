import datetime
import os
from pymongo import Connection

DATABASE = "rapidpro"
CONNECTION = Connection()
FORMAT = '%(asctime)-15s %(message)s'
SITE_API_HOST = 'https://app.rapidpro.io/api/v2'
API_ENDPOINT = 'https://app.rapidpro.io'
BROKER_URL = 'redis://'

cron_minutes = int(os.environ.get('FETCH_SLEEP', 60*24*2))

CELERYBEAT_SCHEDULE = {
    'sync-contacts': {
        'task': 'ureport_data.tasks.fetch_all',
        'schedule': datetime.timedelta(minutes=cron_minutes),
        'args': ()
    }
}

RETRY_MAX_ATTEMPTS = int(os.environ.get('RETRY_MAX_ATTEMPTS', 10))
RETRY_WAIT_FIXED = int(os.environ.get('RETRY_WAIT_FIXED', 15*60*1000))

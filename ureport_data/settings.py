import datetime
from pymongo import Connection

DATABASE = "rapidpro"
CONNECTION = Connection()
FORMAT = '%(asctime)-15s %(message)s'
SITE_API_HOST = 'https://app.rapidpro.io/api/v1'
API_ENDPOINT = 'https://app.rapidpro.io/api/v1'
BROKER_URL = 'redis://'

CELERYBEAT_SCHEDULE = {
    'sync-contacts': {
        'task': 'ureport_data.tasks.fetch_all',
        'schedule': datetime.timedelta(days=7),
        'args': ()
    }
}
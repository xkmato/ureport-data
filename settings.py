from pymongo import Connection

DATABASE = "rapidpro"
CONNECTION = Connection()
FORMAT = '%(asctime)-15s %(message)s'
SITE_API_HOST = 'https://app.rapidpro.io/api/v1'
# SITE_API_USER_AGENT = 'data/1.0'
API_ENDPOINT = 'https://app.rapidpro.io/api/v1'
BROKER_URL = 'redis://localhost:6379/0'
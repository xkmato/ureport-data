import sys
import tasks
from models import Org

__author__ = 'kenneth'


if __name__ == '__main__':
    arguments = sys.argv
    if len(arguments) != 3:
        print "ERROR"
        print "Command must be run `python ureport_data/createorgs.py < Org Name> <API Key> <Sync Now>`"
        print "Org Name: String"
        print "API Key: String"
        print "Sync Now: String - Either of 'true' or 'false'"
    else:
        c, name, key, sync = tuple(arguments)

        o = Org.create(name=name, api_token=key)
        tasks.fetch_all.delay(orgs=[o.api_token])

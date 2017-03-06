import sys
import tasks
from models import Org

__author__ = 'kenneth'


if __name__ == '__main__':
    arguments = sys.argv
    if len(arguments) != 4:
        print "ERROR"
        print "Command must be run `python ureport_data/createorgs.py < Org Name> <API Key> <Sync Now>`"
        print "Org Name: String"
        print "API Key: String"
        print "Sync Now: String - Either of 'true' or 'false'"
    else:
        c, name, key, sync = tuple(arguments)
        sync = True if sync == 'true' else False

        o = Org.create(name=name, api_token=key)

        if sync:
            tasks.fetch_all(orgs=[o.api_token])

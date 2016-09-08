import logging
from datetime import datetime
import sys

import humongolus as orm
import humongolus.field as field
import pymongo
import pytz
from temba_client.v2 import TembaClient
from temba_client.exceptions import TembaNoSuchObjectError, TembaException
from temba_client.v2.types import ObjectRef

import settings

logging.basicConfig(format=settings.FORMAT)
logger = logging.getLogger("models")

orm.settings(logger=logger, db_connection=settings.CONNECTION)


class LastSaved(orm.Document):
    _db = settings.DATABASE
    _collection = 'last_saveds'

    coll = field.Char()
    last_saved = field.TimeStamp()
    org = field.DynamicDocument()

    @classmethod
    def update_or_create(cls, collection):
        obj = cls.find_one({'coll': collection})
        if not obj:
            obj = cls()
            obj.collection = collection
        obj.last_saved = datetime.now()
        obj.save()


class Org(orm.Document):
    _db = settings.DATABASE
    _collection = "orgs"

    name = field.Char(required=True)
    language = field.Char()
    timezone = field.Char(default="UTC")
    api_token = field.Char(required=True)
    is_active = field.Boolean(default=True)
    config = field.Char()

    def get_temba_client(self):
        agent = getattr(settings, 'SITE_API_USER_AGENT', None)
        return TembaClient('https://app.rapidpro.io', self.api_token, user_agent=agent)

    @classmethod
    def create(cls, **kwargs):
        org = cls()
        for k,v in kwargs.items():
            setattr(org, k, v)
        org.save()
        return org


class BaseDocument(orm.Document):
    _db = settings.DATABASE
    fetch_key = 'uuid'

    org = field.DynamicDocument()
    created_on = field.TimeStamp()
    org_oid = field.ObjectId()

    @classmethod
    def create_from_temba(cls, org, temba):
        obj = cls()
        obj.org = org
        for key, value in temba.__dict__.items():
            class_attr = getattr(cls, key, None)
            if class_attr is None:
                continue
            if isinstance(class_attr, orm.List):
                item_class = getattr(sys.modules[__name__], key.rstrip('s').capitalize())
                if issubclass(item_class, BaseDocument):
                    getattr(obj, key).extend(item_class.get_objects_from_uuids(org, getattr(temba, key)))
                if issubclass(item_class, orm.EmbeddedDocument):
                    getattr(obj, key).extend(item_class.create_from_temba_list(getattr(temba, key)))
            elif class_attr == field.DynamicDocument:
                item_class = getattr(sys.modules[__name__], key.capitalize())
                if issubclass(item_class, BaseDocument):
                    setattr(obj, key, item_class.get_or_fetch(org, getattr(temba, key)))
                if issubclass(item_class, orm.EmbeddedDocument):
                    setattr(obj, key, item_class.create_from_temba(getattr(temba, key)))

            else:
                setattr(obj, key, value)
        obj.save()
        return obj

    @classmethod
    def get_or_fetch(cls, org, uuid):
        if uuid == None: return None
        if hasattr(cls, 'uuid'):
            obj = cls.find_one({'uuid': uuid.uuid}) if isinstance(uuid, ObjectRef) else cls.find_one({'uuid': uuid})
            if cls == Label:
                obj = cls.find_one({'name': uuid})
        else:
            obj = cls.find_one({'id': uuid})
        if not obj:
            try:
                obj = cls.fetch(org, uuid.uuid) if isinstance(uuid, ObjectRef) else cls.fetch(org, uuid)
            except AttributeError:
                obj = uuid.uuid if isinstance(uuid, ObjectRef) else uuid
            except (TembaNoSuchObjectError, TembaException):
                obj = None
        return obj

    @classmethod
    def create_from_temba_list(cls, org, temba_lists):
        obj_list = []
        for temba_list in temba_lists.iterfetches():
            if len(temba_list) > 0 and hasattr(temba_list[0], 'contact'):
                contacts = [t.contact for t in temba_list]
                Contact.get_objects_from_uuids(org, contacts)
            q = None
            for temba in temba_list:
                if hasattr(temba, 'uuid'):
                    q = {'uuid': temba.uuid}
                elif hasattr(temba, 'id'):
                    q = {'id': temba.id}
                if not q or not cls.find_one(q):
                    obj_list.append(cls.create_from_temba(org, temba))
        return obj_list

    @classmethod
    def _in_not_in(cls, uuids):
        uuids = [u.uuid for u in uuids]
        k = cls.fetch_key.rstrip('s')
        objs = list(cls.find({k: {'$in': uuids}}))
        e_uuids = [getattr(c, k) for c in objs]
        return objs, list(set(uuids)-set(e_uuids))

    @classmethod
    def get_objects_from_uuids(cls, org, uuids):
        func = "get_%s" % cls._collection
        client = org.get_temba_client()
        fetch_all = getattr(client, func)
        objs, not_in = cls._in_not_in(uuids)

        def chunks(l, n):
            for i in xrange(0, len(l), n):
                yield l[i:i+n]

        if not_in:
            for chunk in chunks(not_in, getattr(settings, 'FETCH_MAX_UUIDS', 50)):
                objs.append(cls.create_from_temba_list(org, fetch_all(**{cls.fetch_key: chunk})))
        return objs

    @classmethod
    def fetch(cls, org, uuid):
        func = "get_%s" % cls._collection
        fetch = getattr(org.get_temba_client(), func)
        return cls.create_from_temba(org, fetch(**{cls.fetch_key: uuid}).all()[0])

    @classmethod
    def fetch_objects(cls, org, af=None, **kwargs):
        func = "get_%s" % cls._collection
        fetch_all = getattr(org.get_temba_client(), func)
        if af:
            after = None
        else:
            try:
                after = cls.find({'org.id': org._id}).sort("created_on", pymongo.DESCENDING).next().modified
                tz = pytz.timezone(org.timezone)
                after = tz.localize(after)
            except StopIteration:
                after = None
        if 'flows' in kwargs:
            objs = cls.create_from_temba_list(org, fetch_all(after=after, flows=kwargs.get('flows')))
        else:
            if cls.__name__ == 'Message':
                objs = cls.create_from_temba_list(org, fetch_all(after=after, folder='inbox'))
            else:
                objs = cls.create_from_temba_list(org, fetch_all(after=after))
        return objs


class Group(BaseDocument):
    _collection = 'groups'

    uuid = field.Char()
    name = field.Char()
    size = field.Integer()


class Urn(orm.EmbeddedDocument):
    type = field.Char()
    identity = field.Char()

    @classmethod
    def create_from_temba(cls, temba):
        urn = cls()
        if temba and len(temba.split(':')) > 1:
            urn.type, urn.identity = tuple(temba.split(':'))
            return urn
        urn.identity = temba
        return urn

    @classmethod
    def create_from_temba_list(cls, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(temba))
        return obj_list


class Contact(BaseDocument):
    _collection = 'contacts'

    uuid = field.Char()
    name = field.Char()
    urns = orm.List(type=Urn)
    groups = orm.List(type=Group)
    language = field.Char()
    fields = field.Char()


class Broadcast(BaseDocument):
    _collection = 'broadcasts'
    fetch_key = 'id'

    id = field.Integer()
    urns = orm.List(type=Urn)
    contacts = orm.List(type=Contact)
    groups = orm.List(Group)
    text = field.Char()
    status = field.Char()


class Campaign(BaseDocument):
    _collection = 'campaigns'

    uuid = field.Char()
    name = field.Char()
    group = field.DynamicDocument()


class Event(BaseDocument):

    _collection = 'events'

    uuid = field.Char()
    campaign = field.DynamicDocument()
    relative_to = field.Char()
    offset = field.Integer()
    unit = field.Char()
    delivery_hour = field.Integer()
    message = field.Char()
    flow = field.DynamicDocument()


# class Field(BaseDocument):
#
#     _collection = 'fields'
#
#     key = field.Char()
#     label = field.Char()
#     value_type = field.Char()


class Ruleset(orm.EmbeddedDocument):
    @classmethod
    def create_from_temba(cls, temba):
        rule_set = cls()
        rule_set.uuid = temba.uuid
        rule_set.label = temba.label
        rule_set.response_type = temba.response_type
        return rule_set

    @classmethod
    def create_from_temba_list(cls, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(temba))
        return obj_list

    _collection = 'rule_sets'

    uuid = field.Char()
    label = field.Char()
    response_type = field.Char()


class Label(BaseDocument):

    _collection = 'labels'
    fetch_key = 'uuid'

    uuid = field.Char()
    name = field.Char()
    count = field.Integer()


class Flow(BaseDocument):

    _collection = 'flows'

    uuid = field.Char()
    name = field.Char()
    archived = field.Char()
    labels = orm.List(type=Label)
    participants = field.Integer()
    runs = field.Integer()
    completed_runs = field.Integer()
    rulesets = orm.List(type=Ruleset)


class Message(BaseDocument):

    _collection = 'messages'
    fetch_key = 'id'

    id = field.Integer()
    broadcast = field.DynamicDocument()
    contact = field.DynamicDocument()
    urn = field.DynamicDocument()
    status = field.Char()
    type = field.Char()
    labels = orm.List(type=Label)
    direction = field.Char()
    archived = field.Char()
    text = field.Char()
    delivered_on = field.TimeStamp()
    sent_on = field.TimeStamp()


class RunValueSet(orm.EmbeddedDocument):
    @classmethod
    def create_from_temba(cls, temba):
        run_value_set = cls()
        run_value_set.node = temba.node
        run_value_set.category = temba.category
        run_value_set.text = temba.text
        run_value_set.rule_value = temba.rule_value
        run_value_set.label = temba.label
        run_value_set.value = temba.value
        run_value_set.time = temba.time
        return run_value_set

    @classmethod
    def create_from_temba_list(cls, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(temba))
        return obj_list

    _collection = 'rule_value_sets'

    node = field.Char()
    category = field.Char()
    text = field.Char()
    rule_value = field.Char()
    label = field.Char()
    value = field.Char()
    time = field.TimeStamp()


class FlowStep(orm.EmbeddedDocument):
    @classmethod
    def create_from_temba(cls, temba):
        flow_step = cls()
        flow_step.node = temba.node
        flow_step.text = temba.text
        flow_step.value = temba.value
        flow_step.type = temba.type
        flow_step.arrived_on = temba.arrived_on
        flow_step.left_on = temba.left_on
        return flow_step

    @classmethod
    def create_from_temba_list(cls, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(temba))
        return obj_list

    _collection = 'flow_steps'

    node = field.Char()
    text = field.Char()
    value = field.Char()
    type = field.Char()
    arrived_on = field.TimeStamp()
    left_on = field.TimeStamp()


class Run(BaseDocument):

    _collection = 'runs'
    fetch_key = 'id'

    id = field.Integer()
    flow = field.Char()
    contact = field.DynamicDocument()
    steps = orm.List(type=FlowStep)
    values = orm.List(type=RunValueSet)
    completed = field.Char()


class CategoryStats(orm.EmbeddedDocument):
    @classmethod
    def create_from_temba(cls, temba):
        category_stats = cls()
        category_stats.count = temba.count
        category_stats.label = temba.label
        return category_stats

    @classmethod
    def create_from_temba_list(cls, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(temba))
        return obj_list

    count = field.Integer()
    label = field.Char()


class Result(BaseDocument):

    _collection = 'results'
    fetch_key = None

    boundary = field.Char()
    set = field.Integer()
    unset = field.Integer()
    open_ended = field.Char()
    label = field.Char()
    categories = orm.List(type=CategoryStats)


class Geometry(orm.EmbeddedDocument):
    @classmethod
    def create_from_temba(cls, temba):
        geometry = cls()
        geometry.type = temba.type
        geometry.coordinates = temba.coordinates
        return geometry

    @classmethod
    def create_from_temba_list(cls, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(temba))
        return obj_list

    type = field.Char()
    coordinates = field.Char()


class Boundary(BaseDocument):
    @classmethod
    def fetch(cls, org, uuid):
        return None

    _collection = 'boundaries'
    fetch_key = None

    boundary = field.Char()
    name = field.Char()
    level = field.Char()
    parent = field.Char()
    geometry = orm.List(type=Geometry)


Org.boundaries = orm.Lazy(type=Boundary, key='org.id')
Org.results = orm.Lazy(type=Result, key='org.id')
Org.runs = orm.Lazy(type=Run, key='org.id')
Org.messages = orm.Lazy(type=Message, key='org.id')
Org.flows = orm.Lazy(type=Flow, key='org.id')
Org.labels = orm.Lazy(type=Label, key='org.id')
Org.events = orm.Lazy(type=Event, key='org.id')
Org.campaigns = orm.Lazy(type=Campaign, key='org.id')
Org.broadcasts = orm.Lazy(type=Broadcast, key='org.id')
Org.contacts = orm.Lazy(type=Contact, key='org.id')
Org.groups = orm.Lazy(type=Group, key='org.id')
Contact.messages = orm.Lazy(type=Message, key='contact.id')
Broadcast.messages = orm.Lazy(type=Message, key='broadcast.id')
Flow.flow_runs = orm.Lazy(type=Run, key='flow.id')
Value = RunValueSet
Step = FlowStep
Categorie = CategoryStats

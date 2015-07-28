import logging
from datetime import datetime
import sys

import humongolus as orm
import humongolus.field as field
from temba import TembaClient
from temba.base import TembaNoSuchObjectError, TembaException

import settings

logging.basicConfig(format=settings.FORMAT)
logger = logging.getLogger("models")

orm.settings(logger=logger, db_connection=settings.CONNECTION)


class LastSaved(orm.Document):
    _db = settings.DATABASE
    _collection = 'last_saveds'

    coll = field.Char()
    last_saved = field.TimeStamp()

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
    is_active = field.Char(default=True)
    config = field.Char()

    def get_temba_client(self):
        host = getattr(settings, 'SITE_API_HOST', None)
        agent = getattr(settings, 'SITE_API_USER_AGENT', None)

        if not host:
            host = '%s/api/v1' % settings.API_ENDPOINT  # UReport sites use this

        return TembaClient(host, self.api_token, user_agent=agent)


class BaseDocument(orm.Document):
    _db = settings.DATABASE

    org = field.DynamicDocument()
    created_on = field.TimeStamp()

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
            obj = cls.find_one({'uuid': uuid})
            if cls == Label:
                obj = cls.find_one({'name': uuid})
        else:
            obj = cls.find_one({'id': uuid})
        if not obj:
            try:
                obj = cls.fetch(org, uuid)
            except (TembaNoSuchObjectError, TembaException):
                obj = None
        return obj

    @classmethod
    def create_from_temba_list(cls, org, temba_list):
        obj_list = []
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
    def get_objects_from_uuids(cls, org, uuids):
        objs = []
        for uuid in uuids:
            try:
                objs.append(cls.get_or_fetch(org, uuid))
            except TembaNoSuchObjectError:
                continue
        return objs

    @classmethod
    def fetch(cls, org, uuid):
        func = "get_%s" % cls._collection
        fetch = getattr(org.get_temba_client(), func.rstrip('s'))
        return cls.create_from_temba(org, fetch(uuid))

    @classmethod
    def fetch_objects(cls, org):
        func = "get_%s" % cls._collection
        ls = LastSaved.find_one({'coll': cls._collection})
        after = getattr(ls, 'last_saved', None)
        fetch_all = getattr(org.get_temba_client(), func)
        try:
            objs = cls.create_from_temba_list(org, fetch_all(after=after))
            if not ls:
                ls = LastSaved()
            ls.coll = cls._collection
            ls.last_saved = datetime.now(tz=org.timezone)
            ls.save()
        except TypeError:
            objs = cls.create_from_temba_list(org, fetch_all())
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
        if len(temba.split(':')) > 1:
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
    group = field.DocumentId


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

    id = field.Integer()
    flow = field.DynamicDocument()
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

    boundary = field.Char()
    name = field.Char()
    level = field.Char()
    parent = field.Char()
    geometry = orm.List(type=Geometry)


Org.boundaries = orm.Lazy(type=Boundary, key='org._id')
Org.results = orm.Lazy(type=Result, key='org._id')
Org.runs = orm.Lazy(type=Run, key='org._id')
Org.messages = orm.Lazy(type=Message, key='org._id')
Org.flows = orm.Lazy(type=Flow, key='org._id')
Org.labels = orm.Lazy(type=Label, key='org._id')
Org.events = orm.Lazy(type=Event, key='org._id')
Org.campaigns = orm.Lazy(type=Campaign, key='org._id')
Org.broadcasts = orm.Lazy(type=Broadcast, key='org._id')
Org.contacts = orm.Lazy(type=Contact, key='org._id')
Org.groups = orm.Lazy(type=Group, key='org._id')
Value = RunValueSet
Step = FlowStep
Categorie = CategoryStats

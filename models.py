import logging
from datetime import datetime
import humongolus as orm
import humongolus.field as field
import sys
from temba import TembaClient
from temba.base import TembaNoSuchObjectError
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
            if isinstance(class_attr, orm.List):
                item_class = class_attr.__kwargs__.get('type')
                if isinstance(item_class, BaseDocument):
                    getattr(obj, key).extend(item_class.get_objects_from_uuids(org, getattr(temba, key)))
                if isinstance(item_class, orm.EmbeddedDocument):
                    getattr(obj, key).extend(item_class.create_from_temba_list(org, getattr(temba, key)))
            elif isinstance(class_attr, field.DynamicDocument):
                item_class = getattr(sys.modules[__name__], key.capitalize())
                if isinstance(item_class, BaseDocument):
                    setattr(obj, key, item_class.get_or_fetch(org, getattr(temba, key)))
                if isinstance(item_class, orm.EmbeddedDocument):
                    setattr(obj, key, item_class.create_from_temba_list(org, getattr(temba, key)))
            else:
                setattr(obj, key, value)
        obj.save()
        return obj

    @classmethod
    def get_or_fetch(cls, org, uuid):
        if uuid == None: return None
        if hasattr(cls, 'uuid'):
            obj = cls.find_one({'uuid': uuid})
        else:
            obj = cls.find_one({'id': uuid})
        if not obj:
            try:
                obj = cls.fetch(org, uuid)
            except TembaNoSuchObjectError:
                obj = None
        return obj

    @classmethod
    def create_from_temba_list(cls, org, temba_list):
        obj_list = []
        for temba in temba_list:
            if hasattr(temba, 'uuid'):
                q = {'uuid': temba.uuid}
            else:
                q = {'id': temba.id}
            if not cls.find_one(q):
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
        fetch = getattr(org.get_temba_client(), func.strip('s'))
        return cls.create_from_temba(org, fetch(uuid))

    @classmethod
    def fetch_objects(cls, org):
        func = "get_%s" % cls._collection
        ls = LastSaved.find_one({'coll': cls._collection})
        after = getattr(ls, 'last_saved', None)
        fetch_all = getattr(org.get_temba_client(), func)
        try:
            objs = cls.create_from_temba_list(org, fetch_all(after=after))
            print objs
        except TypeError:
            objs = cls.create_from_temba_list(org, fetch_all())
            print objs
        return objs


class Group(BaseDocument):
    _collection = 'groups'

    uuid = field.Char()
    name = field.Char()
    size = field.Integer()

    @classmethod
    def create_from_temba(cls, org, temba):
        group = cls()
        group.org = org
        group.uuid = temba.uuid
        group.name = temba.name
        group.size = temba.size
        group.save()
        return group


class URN(orm.EmbeddedDocument):
    type = field.Char()
    identity = field.Char()

    @classmethod
    def create_from_temba(cls, temba):
        urn = cls()
        urn.type, urn.identity = tuple(temba.split(':'))
        return urn

    @classmethod
    def create_from_temba_list(cls, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(temba))
        return obj_list


class Contact(BaseDocument):
    _collection = 'contacts'

    @classmethod
    def create_from_temba(cls, org, temba):
        contact = cls()
        contact.org = org
        contact.uuid = temba.uuid
        contact.name = temba.name
        contact.language = temba.language
        contact.groups.extend(Group.get_objects_from_uuids(org, temba.groups))
        contact.urns.extend(URN.create_from_temba_list(temba.urns))
        contact.save()
        return contact

    uuid = field.Char()
    name = field.Char()
    urns = orm.List(type=URN)
    groups = orm.List(type=Group)
    language = field.Char()
    fields = field.Char()


class Broadcast(BaseDocument):
    _collection = 'broadcasts'

    id = field.Integer()
    urns = orm.List(type=URN)
    contacts = orm.List(type=Contact)
    groups = orm.List(Group)
    text = field.Char()
    status = field.Char()

    @classmethod
    def create_from_temba(cls, org, temba):
        broadcast = cls()
        broadcast.org = org
        broadcast.id = temba.id
        broadcast.text = temba.text
        broadcast.status = temba.status
        broadcast.created_on = temba.created_on
        broadcast.urns.extend(URN.create_from_temba_list(temba.urns))
        broadcast.contacts.extend(Contact.get_objects_from_uuids(org, temba.contacts))
        broadcast.groups.extend(Group.get_objects_from_uuids(org, temba.groups))
        broadcast.save()
        return broadcast


class Campaign(BaseDocument):
    _collection = 'campaigns'

    uuid = field.Char()
    name = field.Char()
    group = field.DocumentId


class Event(BaseDocument):
    @classmethod
    def create_from_temba(cls, org, temba):
        event = cls()
        event.org = org
        event.uuid = temba.uuid
        event.campaign = Campaign.get_or_fetch(org, temba.campaign)
        event.relative_to = temba.relative_to
        event.offset = temba.offset
        event.unit = temba.unit
        event.delivery_hour = temba.delivery_hour
        event.message = temba.message
        event.flow = Flow.get_or_fetch(org, temba.flow)
        event.created_on = temba.created_on
        event.save()
        return event

    _collection = 'events'

    uuid = field.Char()
    campaign = field.DynamicDocument()
    relative_to = field.Char()
    offset = field.Integer()
    unit = field.Char()
    delivery_hour = field.Integer()
    message = field.Char()
    flow = field.DynamicDocument()


class Field(BaseDocument):
    @classmethod
    def create_from_temba(cls, org, temba):
        _field = cls()
        _field.org = org
        _field.key = temba.key
        _field.label = temba.label
        _field.value_type = temba.value_type
        _field.save()
        return _field

    _collection = 'fields'

    key = field.Char()
    label = field.Char()
    value_type = field.Char()


class RuleSet(orm.EmbeddedDocument):
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
    @classmethod
    def create_from_temba(cls, org, temba):
        flow = cls()
        flow.org = org
        flow.uuid = temba.uuid
        flow.name = temba.name
        flow.archived = temba.archived
        flow.labels = temba.labels
        flow.participants = temba.participants
        flow.runs = temba.runs
        flow.complete_runs = temba.complete_runs
        flow.rulesets.append(RuleSet.create_from_temba_list(temba.rulesets))
        flow.save()
        return flow

    _collection = 'flows'

    uuid = field.Char()
    name = field.Char()
    archived = field.Char()
    labels = field.Char()
    participants = field.Integer()
    runs = field.Integer()
    complete_runs = field.Integer()
    rulesets = orm.List(type=RuleSet)


class Message(BaseDocument):
    @classmethod
    def create_from_temba(cls, org, temba):
        print temba.__dict__
        message = cls()
        message.org = org
        message.id = temba.id
        message.broadcast = Broadcast.get_or_fetch(org, temba.broadcast)
        message.contact = Contact.get_or_fetch(org, temba.contact)
        message.urn = URN.create_from_temba(temba.urn)
        message.status = temba.status
        message.type = temba.type
        message.labels = temba.labels
        message.direction = temba.direction
        message.archived = temba.archived
        message.text = temba.text
        message.delivered_on = temba.delivered_on
        message.sent_on = temba.sent_on
        message.created_on = temba.created_on
        message.save()
        return message

    _collection = 'messages'

    id = field.Integer()
    broadcast = field.DynamicDocument()
    contact = field.DynamicDocument()
    urn = field.DynamicDocument()
    status = field.Char()
    type = field.Char()
    labels = field.Char()
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
        run_value_set.rule_value = temba.rule_vale
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
    @classmethod
    def create_from_temba(cls, org, temba):
        run = cls()
        run.org = org
        run.id = temba.id
        run.flow = Flow.get_or_fetch(org, temba.flow)
        run.contact = Contact.get_or_fetch(org, temba.contact)
        run.steps.append(FlowStep.create_from_temba_list(temba.steps))
        run.values.append(RunValueSet.create_from_temba_list(temba.values))
        run.created_on = temba.created_on
        run.completed = temba.completed
        run.save()
        return run

    _collection = 'run'

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

    _collection = 'category_stats'

    count = field.Integer()
    label = field.Char()


class Result(BaseDocument):
    @classmethod
    def create_from_temba(cls, org, temba):
        result = cls()
        result.org = org
        result.boundary = temba.boundary
        result.set = temba.set
        result.unset = temba.unset
        result.open_ended = temba.openended
        result.label = temba.label
        result.categories.append(CategoryStats.create_from_temba_list(temba.categories))

    _collection = 'result'

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

    _collection = 'geometries'

    type = field.Char()
    coordinates = field.Char()


class Boundary(BaseDocument):
    @classmethod
    def fetch(cls, org, uuid):
        pass

    @classmethod
    def create_from_temba(cls, org, temba):
        boundary = cls()
        boundary.boundary = temba.boundary
        boundary.name = temba.name
        boundary.level = temba.level
        boundary.parent = temba.parent
        boundary.geometry.append(Geometry.create_from_temba_list(temba.geometry))

    @classmethod
    def create_from_temba_list(cls, org, temba_list):
        obj_list = []
        for temba in temba_list:
            obj_list.append(cls.create_from_temba(org, temba))
        return obj_list

    _collection = 'boundaries'

    boundary = field.Char()
    name = field.Char()
    level = field.Char()
    parent = field.Char()
    geometry = orm.List(type=Geometry)

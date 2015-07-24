import unittest
from datetime import datetime
from models import Org, Urn, Contact, Group, Broadcast, Campaign, Event, Flow, Label, Message, Run, Boundary, Result

__author__ = 'kenneth'


class FakeTemba(object):
    __dict__ = {}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__dict__ = kwargs
            setattr(self, k, v)


class TestModels(unittest.TestCase):
    def setUp(self):
        org = Org.find_one()
        self.org = org
        self.urns = ['tel:1234', 'twitter:5678', '876565']
        self.temba_group = FakeTemba(uuid='090IOU98', name='test_group', size=1)
        self.temba_contact = FakeTemba(uuid='97976768', name='test_contact', urns=self.urns,
                                 groups=[self.temba_group.uuid], fields={'name': 'test_field'}, language='en',
                                 modified_on=datetime.now())
        self.temba_broadcast = FakeTemba(id=1, urns=self.urns, contacts=[self.temba_contact.uuid],
                                         groups=[self.temba_group.uuid], text='test test message', status='S',
                                         created_on=datetime.now())
        self.temba_campaign = FakeTemba(uuid='IOUIU8908', name='test_campaign', group=self.temba_group.uuid,
                                        created_on=datetime.now())
        self.rule_set = FakeTemba(uuid='iteueiot', label='some label', response_type='I')
        self.temba_flow = FakeTemba(uuid='89077897897', name='test_flow', archived='T', labels=[], participants=3,
                                    runs=3, completed_runs=2, rulesets=[self.rule_set], created_on=datetime.now())
        self.temba_event = FakeTemba(uuid='79079079078', campaign=self.temba_campaign.uuid, relative_to='yuyyer',
                                     offset=5, unit='something', delivery_hour=4, message='Some message',
                                     flow=self.temba_flow.uuid, created_on=datetime.now())
        self.temba_label = FakeTemba(uuid='0789089789', name='test_label', count=5)
        self.temba_message = FakeTemba(id=242, broadcast=self.temba_broadcast.id, contact=self.temba_contact.uuid,
                                       urn=self.urns[0], status='S', type='F', labels=[self.temba_label.name],
                                       direction='I', archived='F', text='Hi There', created_on=datetime.now(),
                                       delivered_on=datetime.now(), sent_on=datetime.now())
        self.temba_run_value_set = FakeTemba(node='90890', category='SC', text='Some Text', rule_value='Y', value='yes',
                                             label='some', time=datetime.now())
        self.temba_flow_step = FakeTemba(node='Some Node', text='Yo yo', value='youngh', type='I',
                                         arrived_on=datetime.now(), left_on=datetime.now())
        self.temba_run = FakeTemba(id=43, flow=self.temba_flow.uuid, contact=self.temba_contact.uuid,
                                   steps=[self.temba_flow_step], values=[self.temba_run_value_set],
                                   create_on=datetime.now(), completed='y')
        self.temba_geometry = FakeTemba(type='some geo type', coordinates='gulu lango')
        self.temba_boundary = FakeTemba(boundary='some boundary', name='test_boundary', level='U', parent='b',
                                        geometry=[self.temba_geometry])
        self.temba_category_stats = FakeTemba(count=10, label='stats')
        self.temba_result = FakeTemba(boundary=None, set=4, unset=5, open_ended='open ended?', label='result1',
                                      categories=[self.temba_category_stats])

    def test_create_from_temba(self):
        urn = Urn.create_from_temba(self.urns[0])
        self.assertEqual((urn.type, urn.identity), tuple(self.urns[0].split(':')))
        group_count = Group.find().count()
        Group.create_from_temba(self.org, self.temba_group)
        self.assertEqual(group_count+1, Group.find().count())
        contact_count = Contact.find().count()
        Contact.create_from_temba(self.org, self.temba_contact)
        self.assertEqual(contact_count+1, Contact.find().count())
        broadcast_count = Broadcast.find().count()
        Broadcast.create_from_temba(self.org, self.temba_broadcast)
        self.assertEqual(broadcast_count+1, Broadcast.find().count())
        campaign_count = Campaign.find().count()
        Campaign.create_from_temba(self.org, self.temba_campaign)
        self.assertEqual(campaign_count+1, Campaign.find().count())
        flow_count = Flow.find().count()
        flow = Flow.create_from_temba(self.org, self.temba_flow)
        self.assertEqual(flow_count+1, Flow.find().count())
        self.assertEqual(flow.rulesets[0].uuid, self.rule_set.uuid)
        event_count = Event.find().count()
        Event.create_from_temba(self.org, self.temba_event)
        self.assertEqual(event_count+1, Event.find().count())
        label_count = Label.find().count()
        Label.create_from_temba(self.org, self.temba_label)
        self.assertEqual(label_count+1, Label.find().count())
        message_count = Message.find().count()
        Message.create_from_temba(self.org, self.temba_message)
        self.assertEqual(message_count+1, Message.find().count())
        run_count = Run.find().count()
        run = Run.create_from_temba(self.org, self.temba_run)
        self.assertEqual(run_count+1, Run.find().count())
        self.assertEqual(run.values[0].text, self.temba_run_value_set.text)
        self.assertEqual(run.steps[0].text, self.temba_flow_step.text)
        boundary_count = Boundary.find().count()
        boundary = Boundary.create_from_temba(self.org, self.temba_boundary)
        self.assertEqual(boundary_count+1, Boundary.find().count())
        self.assertEqual(boundary.geometry[0].coordinates, self.temba_geometry.coordinates)
        result_count = Result.find().count()
        result = Result.create_from_temba(self.org, self.temba_result)
        self.assertEqual(result_count+1, Result.find().count())
        self.assertEqual(result.categories[0].label, self.temba_category_stats.label)

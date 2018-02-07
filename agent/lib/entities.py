#-*- coding: utf-8 -*-

import types, inspect, re
import traceback
from datetime import datetime, timedelta
from .soa_config import *

class not_found(object):
    unknown_context = 'unknown'

    def __getattr__(cls, name):
        def method(cls, *args, **kwargs):
            print("Unknown mehtod '{0}'".format(name))

            return "help", "do"

        return method

    @classmethod
    def cancel(cls, *args, **kwargs):
        return cls.cancel.__name__, cls.unknown_context

class entity(object):
    first_context = 'do'
    unknown_context = 'unknown'
    entity_dict = None

    @classmethod
    def __init__(cls, *args, **kwargs):
        cls.compound_df = soa_info.compound_entities

    @classmethod
    def __getattr__(cls, name):
        def method(cls, *args, **kwargs):
            print("Unknown mehtod '{0}'".format(name))

            return "help", "do"

        return method

    @classmethod
    def get_compound_entity(cls, entities):
        pattern = '^(?=.*' + ')(?=.*'.join(entities) + ').*$'
        df = cls.compound_df[cls.compound_df.entities.str.contains(pattern)]
        row, _ = df.shape

        check_flag = False
        if row > 0 :
            for index, row in df.iterrows():
                items = row['entities'].split(',')
                for item in items:
                    item = item.strip()
                    if re.match('.*\+',item) and (item[:-1] not in entities):
                        check_flag = True

                compound_entity = row['domain']

        if check_flag:
            compound_entity = None

        return compound_entity

    @classmethod
    def do(cls, *args, **kwargs):
        return cls.__name__, cls.first_context

    @classmethod
    def not_found(cls, *args, **kwargs):
        return cls.__name__, cls.unknown_context

    @classmethod
    def input_validation(cls, check_list, entities):
        empty_entities = []
        for key in check_list:
            if key.replace("+","") not in entities.keys() and re.match("^(.*)\+$",key):
                empty_entities.append(key.replace("+",""))

        return empty_entities

    @classmethod
    def input_request(cls, entities):
        for entity in entities:
            message = "required " + entity

        return message

    @classmethod
    def get_entity_type(cls, entities):
        entities_type = []
        for entity in entities:
            if entity:
                print("match entity:",entity)
                if re.match("^([0-9]+)d$",entity):
                    entity_type = "time"
                else:
                    entity_type = cls.entity_dict[cls.entity_dict['meanings'] == entity]['type'].tolist().pop()

                entities_type.append((entity, entity_type))

        return entities_type

    @classmethod
    def set_arg(cls, args, user_convo):

        return_args = {}
        for key, value in user_convo['args'].items():
            return_args[key] = value

        print("entities args:",args)
        entities = cls.get_entity_type(args)
        print("entities:",entities)
        for entity, entity_type  in entities:
            return_args[entity_type] = entity

        return return_args

    @classmethod
    def get_time(cls, time_info):
        value = time_info[:-1]
        kind = time_info[-1:]

        now = datetime.now()
        if kind == 'd':
            target = now + timedelta(days=int(value))

        return target

    @classmethod
    def get_sao_info(cls, user_convo):
        es_subject = user_convo['context']
        es_action = user_convo['prev_sao']['action']
        es_object = user_convo['prev_sao']['object']

        return es_subject, es_action, es_object

class something(entity):
    @classmethod
    def __get_compound_entity__(cls, *args, **kwargs):
        cls.entities = [ value for value in kwargs['subjects'] if value not in kwargs['derived_verb']]
        cls.derived_verb = kwargs['derived_verb']
        print("entities:{}, derived_verb:{})".format(cls.entities, cls.derived_verb))

        if len(cls.entities) == 1:
            cls.compound_entity = cls.entities[0]
            check_flag =True 
        else:
            cls.compound_entity =  cls.get_compound_entity(cls.entities)
            if not cls.compound_entity:
                cls.compound_entity = 'Unknown'

    @classmethod
    def do(cls, *args, **kwargs):
        cls.__get_compound_entity__(*args, **kwargs)
        action = cls.derived_verb[0] if cls.derived_verb != [] else cls.first_context
        return action, cls.compound_entity

    @classmethod
    def change(cls, *args, **kwargs):
        cls.__get_compound_entity__(*args, **kwargs)
        return cls.change.__name__, cls.compound_entity

    @classmethod
    def tell(cls, *args, **kwargs):
        cls.__get_compound_entity__(*args, **kwargs)
        return cls.tell.__name__, cls.compound_entity

    @classmethod
    def want(cls, *args, **kwargs):
        cls.__get_compound_entity__(*args, **kwargs)
        return cls.want.__name__, cls.compound_entity

    @classmethod
    def come(cls, *args, **kwargs):
        cls.__get_compound_entity__(*args, **kwargs)
        return cls.come.__name__, cls.compound_entity

class cls_conversation(entity):
    @classmethod
    def do(cls, *args, **kwargs):
        return cls.__name__, cls.first_context

    @classmethod
    def accept_another_subject(cls, subject, self_subject):
        if subject != self_subject:
            flag = False
        else:
            flag = True

        return flag

class cls_abstraction(entity):
    @classmethod
    def do(cls, *args, **kwargs):
        return cls.__name__, cls.first_context

class cls_weather(entity):
    @classmethod
    def do(cls, *args, **kwargs):
        return cls.__name__, cls.first_context

class cls_area(entity):
    @classmethod
    def do(cls, *args, **kwargs):
        user_convo = kwargs['user_convo']
        area = kwargs['subject']
        user_convo['args']['area'] =  area
        es_subject, es_action, es_object = cls.get_sao_info(user_convo)

        domain, answer, *params = getattr(eval(es_subject), es_action)(es_object, user_convo=user_convo, subject=kwargs['subject'])
        return domain, answer, params[0]

class cls_action(entity):
    @classmethod
    def do(cls, *args, **kwargs):
        user_convo = kwargs['user_convo']
        area = kwargs['subject']
        user_convo['args']['area'] =  area
        es_subject, es_action, es_object = cls.get_sao_info(user_convo)

        domain, answer, *params = getattr(eval(es_subject), es_action)(es_object, user_convo=user_convo, subject=kwargs['subject'])
        return domain, answer, params[0]

class greeting(cls_conversation):
    @classmethod
    def do(cls, *args, **kwargs):
        if cls.accept_another_subject(kwargs['subject'], cls.__name__):
            return cls.__name__, cls.first_context
        else:
            return "help", cls.first_context

    @classmethod
    def longtime(cls, *args, **kwargs):
        return cls.__name__, inspect.getframeinfo(inspect.currentframe()).function

class weather(cls_weather):
    required_entity = ['day','area+']
    
    @classmethod
    def how(cls, *args, **kwargs):
        print("args:",args)
        print("kwargs:",kwargs)

        convo_args = cls.set_arg(args, kwargs['user_convo'])
        empty_entities = cls.input_validation(cls.required_entity, convo_args)

        print("empty_entities:",empty_entities)
        print("convo args:",convo_args)

        if len(empty_entities) > 0:
            message = cls.input_request(empty_entities)
        else:
            message = "answer " + cls.get_weather(convo_args)

        return cls.__name__, message, convo_args

    @classmethod
    def get_weather(cls, *args):
        try:
            weather_date = cls.get_time(args[0]['time'])
        except KeyError:
            weather_date = cls.get_time('0d')

        message = args[0]['area'] + '의 ' + weather_date.strftime('%m월 %d일') + ' 날씨는 xxxx입니다.'
        return message

    @classmethod
    def do(cls, *args, **kwargs):
        domain, message, convo_args = cls.how(*args, **kwargs)
        return domain, message, convo_args

    @classmethod
    def tell(cls, *args, **kwargs):
        domain, message, convo_args = cls.how(*args, **kwargs)
        return domain, message, convo_args

class time(entity):
    @classmethod
    def do(cls, *args, **kwargs):
        user_convo = kwargs['user_convo']
        time_info = kwargs['subject']
        user_convo['args']['time'] = time_info
        es_subject, es_action, es_object = cls.get_sao_info(user_convo)

        domain, answer, *params = getattr(eval(es_subject), es_action)(es_object, user_convo=user_convo, subject=kwargs['subject'])
        return domain, answer, params[0]

class cls_cs(entity):
    @classmethod
    def do(cls, *args, **kwargs):
        if args[0] == 'cancel':
                cls.action = 'cancel'
        else:
                cls.action = cls.first_context

        action_not = ' not' if kwargs['action_neg'] == 'not' else ''
        return cls.__name__, cls.action + action_not

    @classmethod
    def want(cls, *args, **kwargs):
        cls_name, cls_action = cls.do(*args, **kwargs)
        return cls_name, cls_action

    @classmethod
    def give(cls, *args, **kwargs):
        cls_name, cls_action = cls.do(*args, **kwargs)
        return cls_name, cls_action

class refund(cls_cs):
    pass

class order(cls_cs):
    pass

class cancelorder(cls_cs):
    pass

class returns(cls_cs):
    pass

class manager(cls_abstraction):
    @classmethod
    def change(cls, *args, **kwargs):
        return cls.change.__name__, cls.__name__, 

    @classmethod
    def call(cls, *args, **kwargs):
        return cls.call.__name__, cls.__name__, 

class money(cls_abstraction):
    @classmethod
    def giveme(cls, *args, **kwargs):
        return cls.__name__, cls.giveme.__name__

    @classmethod
    def turn(cls, *args, **kwargs):
        cls_name, cls_action = cls.giveme(*args, **kwargs)
        return cls_name, cls_action
        

class cls_derived(entity):
    use_entities = []
    verb_entities = ['change']

    @classmethod
    def do(cls, *args, **kwargs):
        if len(args[0]) == 1:
            if args[0][0] in cls.use_entities:
                target = args[0][0]
            else:
                target = 'unknown'

            cls_action = cls.__name__
        else:
            cls_entities = [ value for value in args[0] if value not in cls.verb_entities ]
            cls_actions = [ value for value in args[0] if value in cls.verb_entities ]
            print("entities:{}, actions:{})".format(cls_entities, cls_actions))

            compound_entity =  cls.get_compound_entity(cls_entities)
            target = compound_entity if compound_entity else 'unknown'

            cls_action = cls_actions[0] if len(cls_actions) > 0 else cls.__name__

        return cls_action, target

    @classmethod
    def want(cls, *args, **kwargs):
        cls_name, target = cls.do(*args, **kwargs)
        return cls_name, target

class cancel(cls_derived):
    use_entities = ['refund', 'order']

class change(cls_derived):
    use_entities = ['email', 'address']

class enable(cls_derived):
    pass

class exchange(cls_derived):
    pass

class connection(cls_derived):
    use_entities = ['manager']

class please(cls_derived):
    use_entities = ['manager']

class request(cls_derived):
    use_entities = ['manager']

class want(cls_derived):
    use_entities = ['manager', 'refund', 'order','returns']

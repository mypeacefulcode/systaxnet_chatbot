#-*- coding: utf-8 -*-

import types, inspect, re
import traceback
from datetime import datetime, timedelta
from .soa_config import *

class not_found(object):
    def __getattr__(self, name):
        def method(self, *args, **kwarg):
            print("Unknown mehtod '{0}'".format(name))

            return "help", "do"

        return method

class entity(object):
    first_context = 'do'
    unknown_context = 'unknown'
    entity_dict = None

    def __getattr__(self, name):
        def method(self, *args, **kwarg):
            print("Unknown mehtod '{0}'".format(name))

            return "help", "do"

        return method

    @classmethod
    def do(cls, *args, **kwarg):
        return cls.__name__, cls.first_context

    @classmethod
    def not_found(cls, *args, **kwarg):
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

class cls_conversation(entity):
    @classmethod
    def do(cls, *args, **kwarg):
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
    def do(cls, *args, **kwarg):
        return cls.__name__, cls.first_context

class cls_weather(entity):
    @classmethod
    def do(cls, *args, **kwarg):
        return cls.__name__, cls.first_context

class cls_area(entity):
    @classmethod
    def do(cls, *args, **kwarg):
        user_convo = kwarg['user_convo']
        area = kwarg['subject']
        user_convo['args']['area'] =  area
        es_subject, es_action, es_object = cls.get_sao_info(user_convo)

        domain, answer, *params = getattr(eval(es_subject), es_action)(es_object, user_convo=user_convo, subject=kwarg['subject'])
        return domain, answer, params[0]

class cls_action(entity):
    @classmethod
    def do(cls, *args, **kwarg):
        user_convo = kwarg['user_convo']
        area = kwarg['subject']
        user_convo['args']['area'] =  area
        es_subject, es_action, es_object = cls.get_sao_info(user_convo)

        domain, answer, *params = getattr(eval(es_subject), es_action)(es_object, user_convo=user_convo, subject=kwarg['subject'])
        return domain, answer, params[0]

class greeting(cls_conversation):
    @classmethod
    def do(cls, *args, **kwarg):
        if cls.accept_another_subject(kwarg['subject'], cls.__name__):
            return cls.__name__, cls.first_context
        else:
            return "help", cls.first_context

    @classmethod
    def longtime(cls, *args, **kwarg):
        return cls.__name__, inspect.getframeinfo(inspect.currentframe()).function

class weather(cls_weather):
    required_entity = ['day','area+']
    
    @classmethod
    def how(cls, *args, **kwarg):
        print("args:",args)
        print("kwarg:",kwarg)

        convo_args = cls.set_arg(args, kwarg['user_convo'])
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
    def do(cls, *args, **kwarg):
        domain, message, convo_args = cls.how(*args, **kwarg)
        return domain, message, convo_args

    @classmethod
    def tell(cls, *args, **kwarg):
        domain, message, convo_args = cls.how(*args, **kwarg)
        return domain, message, convo_args

class time(entity):
    @classmethod
    def do(cls, *args, **kwarg):
        user_convo = kwarg['user_convo']
        time_info = kwarg['subject']
        user_convo['args']['time'] = time_info
        es_subject, es_action, es_object = cls.get_sao_info(user_convo)

        domain, answer, *params = getattr(eval(es_subject), es_action)(es_object, user_convo=user_convo, subject=kwarg['subject'])
        return domain, answer, params[0]

class cls_cs(entity):
    @classmethod
    def do(cls, *args, **kwarg):
        return cls.__name__, cls.first_context

    @classmethod
    def want(cls, *args, **kwarg):
        return cls.__name__, cls.first_context

class refund(cls_cs):
    @classmethod
    def do(cls, *args, **kwarg):
        return cls.__name__, cls.first_context

class money(cls_abstraction):
    @classmethod
    def giveme(cls, *args, **kwarg):
        return cls.__name__, cls.giveme.__name__

class cancel(cls_abstraction):
    @classmethod
    def do(cls, *args, **kwarg):
        if args[0] == ['refund', 'order']:
            target = args[0]
        else:
            target = 'unknown'

        return target, cls.__name__

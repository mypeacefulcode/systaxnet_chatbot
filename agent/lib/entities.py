#-*- coding: utf-8 -*-

import types
from .soa_config import *

class entity(object):
    first_context = 'do'
    def __getattr__(self, name):
        def method(self, *args, **kwarg):
            print("Unknown mehtod '{0}'".format(name))

        return method

    @classmethod
    def do(cls, *args, **kwarg):
        return soa_info.message[cls.__name__], cls.first_context

class cls_conversation(entity):
    @classmethod
    def do(cls, *args, **kwarg):
        return soa_info.message[cls.__name__.split('_')[1]], cls.first_context

class cls_abstraction(entity):
    @classmethod
    def do(cls, *args, **kwarg):
        return soa_info.message[cls.__name__.split('_')[1]], cls.first_context

class greeting(cls_conversation):
    @classmethod
    def do(cls, *args, **kwarg):
        return cls.__name__, cls.first_context

#-*- coding: utf-8 -*-
import pandas as pd

class soa_info(object):
    message = {}
    message['entity'] = 'help'
    message['cls_conversation'] = 'greeting'
    message['cls_abstraction'] = 'help'
    message['cls_cs'] = 'help'

    csv_file = "./data/compound_entities.csv"
    compound_entities = pd.read_csv(csv_file).fillna("")

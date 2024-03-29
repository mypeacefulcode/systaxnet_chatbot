#-*- coding: utf-8 -*-

import sys, traceback, copy
import pandas as pd
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None
import numpy as np
import redis, re, ast 
from .entities import *

class ExecutionStructure(object):
    def __init__(self, config, redisdb, logger):
        self.logger = logger
        self.config = config
        self.redisdb = redisdb

        self.special_tokens = []

        # Make entities dataframe
        path = self.config['entities_config']['csv_path'] + '/'
        csv_file = path + self.config['entities_config']['csv_entities_file']
        self.entities = pd.read_csv(csv_file)

        csv_file = path + self.config['entities_config']['parse_label_file']
        self.parse_label = pd.read_csv(csv_file)

        csv_file = path + self.config['entities_config']['compound_entities_file']
        self.compound_entities = pd.read_csv(csv_file).fillna("")

        csv_file = path + self.config['entities_config']['verify_dict_file']
        self.verify_dict = pd.read_csv(csv_file).fillna("")

        setattr(entity, 'entity_dict', self.entities)
        
    def analyze_pos(self, analyzer):
        for index, row in self.pt_df.iterrows():
            if row['pos'] == "UNKNOWN":
                try:
                    child_text = self.pt_df[(self.pt_df['head_token_idx'] == index) & \
                                                (self.pt_df['token_idx'] != index)]['text'].tolist().pop()
                except IndexError:
                    traceback.print_exc()
                    child_text = ""

                text = self.pt_df[self.pt_df['token_idx'] == index]['text'].tolist().pop()
                word = text + child_text

                response = analyzer.call(word).decode('utf-8')
                token = response.split('|')[0]
                token_pos = token.split('/')[2]
                self.pt_df.a_pos[self.pt_df.token_idx == index] = token_pos
            else:
                self.pt_df.a_pos[self.pt_df.token_idx == index] = row['pos']

    def dependency_label_rule(self, entity, my_label, my_pos, parent_label, brothers_label, depth, parent_es_kind):
        labels_set = set([my_label] + brothers_label)
        print("labels_set, my_label, parent_es_kind:", labels_set, my_label, parent_es_kind)

        time_entity = False
        meaning =  entity['meanings'].tolist()
        if len(meaning) > 0:
            if meaning[0].split(',')[0] in ['day']:
                time_entity = True

        es_kind = None

        es_action = set(['ROOT-VERB','ROOT-PRT','ROOT-ADJ','ADVCL-VERB','ROOT-ADJECTIVE','AUX-VERB','ROOT-AFFIX','SUFF','PRECOMP'])
        #es_subject = set(['ROOT-NOUN','NSUBJ','AUX-NOUN','CCOMP','ADVCL-NOUN','DOBJ','NN'])
        es_subject = set(['ROOT-NOUN','NSUBJ','AUX-NOUN','CCOMP','ADVCL-NOUN','NN'])
        es_modifier = set(['RCMOD','ADVMOD','ATTR'])
        es_object = set(['DOBJ','NN'])
        es_special = set(['NEG', 'NUM'])

        if depth <= 1:
            for key, labels in [('object',es_object), ('action', es_action), ('subject',es_subject), ('modifier',es_modifier), ('special',es_special)]:
                s = labels_set.intersection(labels)
                print("S:",s)
                if my_label in s and key == 'modifier':
                    if my_label == 'ATTR' and parent_es_kind == 'action':
                        es_kind = 'modifier'
                    else:
                        es_kind = key
                elif len(s) > 0 and my_label == s.pop():
                    es_kind = key
                    print("loop es kind:", key)
        else:
            print("dept else - my_label, parent_label, parent_es_kind:",my_label, parent_label, parent_es_kind)
            if my_label == "SUFF" and parent_es_kind[:7] == 'subject':
                es_kind = "subject_suff"
            elif my_label == "DEP" and parent_es_kind[:6] == 'object':
                es_kind = "object"
            elif my_label == "DEP" and parent_es_kind[:7] == 'subject':
                es_kind = "subject"
            elif my_label == "DEP" and parent_es_kind[:6] == 'action':
                es_kind = "subject"
            elif my_label == "NN" and parent_es_kind[:6] == "object":
                es_kind = "object"
            elif my_label == "NN" and parent_es_kind[:7] == "subject":
                es_kind = "subject"
            elif my_label in ["NN","DOBJ"] and parent_es_kind[:6] == "action":
                es_kind = "subject"
            elif my_label in ["DOBJ"]:
                es_kind = "object"
            elif my_label in es_special:
                es_kind = "special"

        print("key:",es_kind)
        return es_kind

    def get_es_kind(self, token, parent_label, brothers_label, depth, parent_es_kind):
        print("------------------------ sub begin ---------------------------")
        label = token['label'].tolist().pop()
        word = token['text'].tolist().pop()
        pos = token['a_pos'].tolist().pop()
        e_key = word + "/" + pos
        print("e_key:",e_key)
        e = self.entities[self.entities['word'] == e_key]

        es_kind = self.dependency_label_rule(e, label, pos, parent_label, brothers_label, depth, parent_es_kind)
        print("------------------------ sub end ---------------------------")

        es_kind_str = es_kind if es_kind else 'None'
        return_str = e_key + ":" + label + "/" + str(token['token_idx'].tolist().pop()) + "/" + \
                     str(token['head_token_idx'].tolist().pop()) + ":" + es_kind_str
        return es_kind, return_str

    def merge_es_dict(self, es_kind, child_label, parse_dict, child_parse_dict):
        print("Start -------------------------------")
        print("merge_es_dict: parse_dict ->", parse_dict)
        print("merge_es_dict: child_parse_dict ->", child_parse_dict)
        for child_es_kind in child_parse_dict.keys():
            value = child_parse_dict[child_es_kind]
            print("++ Loop ++")
            print("++ parse_dict:", parse_dict)
            print("++ child_parse_dict:", child_parse_dict)
            print("++ value:", value)
            print("++ child_es_kind:", child_es_kind)
            print("++++++++++")
            if child_label.split('-')[0] in ['ADVCL','RCMOD','CCOMP']:
                if es_kind + '_modifier' not in parse_dict.keys():
                    parse_dict[es_kind + '_modifier'] = []

                if value[0] not in parse_dict[es_kind + '_modifier']:
                    parse_dict[es_kind + '_modifier'] += value
            elif re.match(".+_modifier", child_es_kind):
                if es_kind + '_modifier' not in parse_dict.keys():
                    parse_dict[es_kind + '_modifier'] = []

                if value[0] not in parse_dict[es_kind + '_modifier']:
                    parse_dict[es_kind + '_modifier'] += value
            elif child_es_kind in ['subject']:
                if child_es_kind  not in parse_dict.keys():
                    parse_dict[child_es_kind] = []

                if value[0] not in parse_dict[child_es_kind]:
                    parse_dict[child_es_kind] += value
            elif child_es_kind in ['special']:
                if child_label in ['NEG']:
                    if es_kind + '_neg' not in parse_dict.keys():
                        parse_dict[es_kind + '_neg'] = []

                    if value[0] not in parse_dict[es_kind + '_neg']:
                        parse_dict[es_kind + '_neg'] += value
                elif child_label in ['NUM']:
                    if es_kind + '_num' not in parse_dict.keys():
                        parse_dict[es_kind + '_num'] = []

                    if value[0] not in parse_dict[es_kind + '_num']:
                        parse_dict[es_kind + '_num'] += value
            elif es_kind == 'object' and child_es_kind in ['object']:
                if value[0] not in parse_dict[es_kind]:
                    parse_dict[es_kind] += value
            else:
                parse_dict.update(child_parse_dict)
        print("merge_es_dict: parse_dict ->", parse_dict)
        print("End-------------------------------")

    def read_parse_tree(self, idx, parent_es_kind = None, brothers_label = [], depth = 0):

        token = self.pt_df[self.pt_df['token_idx'] == idx]
        label = token['label'].tolist().pop()
        pos = token['a_pos'].tolist().pop()

        depth = 0 if label.split('-')[0] in ['ROOT','ADVCL','RCMOD','CCOMP'] else depth + 1

        parse_dict = {}
        es_kind, e_key = self.get_es_kind(token, label, brothers_label, depth, parent_es_kind)
        print("es_kind:",es_kind)
        if es_kind:
            if es_kind == "modifier":
                if pos in ["NOUN"]:
                    es_kind = "subject"
                elif pos in ["VERB"]:
                    es_kind = "action"

            if es_kind not in parse_dict.keys():
                parse_dict[es_kind] = []
            parse_dict[es_kind].append(e_key)

            print("es_kind, es_kye:",es_kind, e_key)

            child_idxs = self.pt_df[(self.pt_df['head_token_idx'] == idx) & (self.pt_df['token_idx'] != idx)]['token_idx'].tolist()
            child_labels = self.pt_df[(self.pt_df['head_token_idx'] == idx) & (self.pt_df['token_idx'] != idx)]['label'].tolist()
            childs = zip(child_idxs, child_labels)

            print("self_idx:",idx)
            print("child_idxs:",child_idxs)
            for idx, child_label in childs:
                child_parse_dict = self.read_parse_tree(idx, es_kind, child_labels, depth)
                soa_cnt = len(set([key.split('_')[0] for key in child_parse_dict.keys()]))
                print("Child parse dict(depth:%s,soa_cnt:%s):%s"%(depth,soa_cnt,child_parse_dict))
                if soa_cnt > 1 and depth > 0:
                    if es_kind + '_modifier' not in parse_dict.keys():
                        parse_dict[es_kind + '_modifier'] = []
                    parse_dict[es_kind + '_modifier'].append(child_parse_dict)
                else:
                    self.merge_es_dict(es_kind, child_label, parse_dict, child_parse_dict)

            print("(L)Merge parse dict:",parse_dict)
        return parse_dict

    def make_execution_structure(self, df, analyzer):
        self.pt_df = df
        self.pt_df['a_pos'] = pd.Series(index=self.pt_df.index)
        self.pt_df['exec_pos'] = pd.Series(index=self.pt_df.index)

        self.analyze_pos(analyzer)

        root_idx = self.pt_df.loc[self.pt_df['label'] == 'ROOT']['token_idx'].tolist().pop()

        df = self.pt_df.loc[self.pt_df['label'].isin(['ROOT','ADVCL','AUX'])]
        tokens = zip(df['token_idx'].tolist(), df['label'].tolist(), df['a_pos'].tolist())

        for idx, label, pos in tokens:
            self.pt_df.label[self.pt_df.token_idx == idx] = label + '-' + pos.upper().strip()

        print("* make_execution_structure // self.pt_df:\n",self.pt_df)
        parse_dict = self.read_parse_tree(root_idx)
        print("* make_execution_structure // parse_dict:",parse_dict)

        return parse_dict

    def verify_domain(self, domain_df, es_dict):
        p = re.compile('.*\+')
        for index, row in domain_df.iterrows():
            check_flag = True
            for key in ['subject','object','action']:
                items = row[key].split(',')
                for item in items:
                    if p.match(item) and (item[:-1] not in es_dict[key]):
                        check_flag = False

            if check_flag:
                return True, row

        return False, None

    def get_user_convo(self, user_id):
        name = "CONVO-" + user_id
        # Temporary
        #user_convo = self.redisdb.hgetall(name)
        user_convo = {}

        if user_convo == {}:
            user_convo = {
                'context' : None,
                'prev_sao' : {},
                'formatter' : None,
                'request' : None,
                'args' : {}
            }
        else:
            user_convo['prev_sao'] = ast.literal_eval(user_convo['prev_sao'])
            user_convo['args'] = ast.literal_eval(user_convo['args'])
            if user_convo['request'] == 'None':
                user_convo['request'] = None

        return user_convo

    def save_user_context(self, user_id, user_convo):
        name = "CONVO-" + user_id
        self.redisdb.hmset(name, user_convo)

    def get_meaning(self, ekey):
        print("ekey:",ekey)
        word = ekey.split(':')[0]
        print("word:",word,ekey)
        e = self.entities[self.entities['word'] == word]
        print("E:",e)
        row, _ = e.shape
        if row == 1:
            meaning = e['meanings'].tolist().pop() + ':' + ekey.split(':')[1] + ':' + ekey.split(':')[2] + ':' \
                      + e['dependency'].tolist().pop() + ":" + e['d-verb'].tolist().pop() + ':' + e['type'].tolist().pop()

            if e['type'].tolist().pop() in ['time','neg']:
                self.special_tokens.append(meaning)
        elif row == 0:
            meaning = 'not_found' + ':' + ekey.split(':')[1] + ':' + ekey.split(':')[2] + ':T:F:None'
        else:
            meaning = None


        return meaning

    def infer_meaning(self, es_type, p_meaning, values):
        print("* infer_meaning // values:",values)
        meanings = []
        if type(values) == dict:
            for key, value in values.items():
                for s_value in value:
                    tmp = s_value.split(':')
                    tmp[2] = key
                    meaning = ':'.join(tmp)
                    meanings.append(meaning)
        elif type(values) == list:
            meanings = values[:]
        else:
            meanings.append(values)

        return meanings 

    def get_token_by_index(self, es_dict, idx):
        return_value = None
        for key, values in es_dict.items():
            for value in values:
                if value.split(':')[1].split('/')[1] == idx:
                    return_value = value
                    break
        return return_value

    def remove_special_tokens(self, es_dict):
        special_entities = {}
        for key, values in es_dict.items():
            new_values = list(set(values).difference(self.special_tokens))
            es_dict[key] = values if new_values == [] else new_values

        for token in self.special_tokens:
            head_token_idx = token.split(':')[1].split('/')[2]
            try:
                head_token = self.get_token_by_index(es_dict, head_token_idx).split(':')[0]
            except KeyError as e:
                traceback.print_exc()
                head_token = ''
            special_entities[head_token] = token.split(':')[0]

        return es_dict, special_entities

    def read_parse_dict(self, parse_dict):
        es_dict = {}
        print("1. parse_dict:",parse_dict)

        for es_type in ['subject', 'object', 'action', 'modifier']:
            try:
                print("es_dict(sub loop):",es_dict)
                sub_keys = []
                meanings = []
                m_meanings = []
                for key in parse_dict.keys():
                    if re.match(es_type + "_", key):
                        sub_keys.append(key)
                
                for value in parse_dict[es_type]:
                    m_meanings.append(self.get_meaning(value))
                meanings = m_meanings[:]
                print("++++++++++++++++++++++++++++++++++")
                print("rr meanings:",meanings)

                for key in sub_keys:
                    print("key:",key)
                    if key.split('_')[1] == 'modifier':
                        if type(parse_dict[key][0]) == dict:
                            sub_es_dict = self.read_parse_dict(parse_dict[key][0])
                            values = self.infer_meaning(es_type, m_meanings, sub_es_dict)
                        else:
                            values = parse_dict[key]

                        print("values:",values)
                        if es_type in ['subject','object']:
                            tmp = []
                            dependency = False
                            for meaning in m_meanings:
                                if meaning.split(':')[3] == 'T':
                                    dependency = True
                                else:
                                    tmp.append(meaning)
                            if dependency:
                                meanings = values[:]
                                meanings += tmp[:]
                        else:
                            tmp = []
                            dependency = False
                            check_es_dict = copy.deepcopy(es_dict)
                            for meaning in m_meanings:
                                if meaning.split(':')[3] == 'T':
                                    dependency = True
                                else:
                                    tmp.append(meaning)

                            for value in values:
                                sub_meaning = self.get_meaning(value)
                                sub_es_type = value.split(':')[2] 
                                print("sub_meaning:{}, check_es_dict:{}".format(sub_meaning, check_es_dict))
                                if sub_meaning and sub_es_type != "action" and \
                                   (sub_es_type not in check_es_dict.keys() or check_es_dict[sub_es_type] == []):
                                    key_header = "action_" if sub_es_type not in ['subject', 'object', 'action'] else ""
                                    if sub_es_type not in es_dict.keys():
                                        es_dict[key_header + sub_es_type] = []
                                    es_dict[key_header + sub_es_type].append(sub_meaning)
                                elif sub_es_type == 'action' and dependency and sub_meaning.split(':')[0] != 'not_found':
                                    meanings = tmp[:]
                                    meanings.append(sub_meaning)

                        print("meanings:",meanings)
                    elif key.split('_')[1] == 'suff':
                        if es_type == 'subject':
                            parse_dict['action'] += parse_dict[key]
                    elif key == 'modifier':
                        pass
                    elif key.split('_')[1] == 'neg':
                        if key not in es_dict.keys():
                            es_dict[key] = []
                        for value in parse_dict[key]:
                            sub_meaning = self.get_meaning(value)
                            es_dict[key].append(sub_meaning)

            except KeyError as e:
                traceback.print_exc()

            es_dict[es_type] = meanings
            print("es_dict(loop):",es_dict)

        return es_dict

    def choice_derived_verb(self, derived_verbs):
        derived_verb = ''
        prev_index = 99
        pattern = re.compile('(ROOT)*(NSUBJ)*(DOBJ)*')
        for verb in derived_verbs:
            x = pattern.match(verb[1].split('/')[0])
            if type(x.lastindex) == int and x.lastindex < prev_index:
                derived_verb = verb[0]
                prev_index = x.lastindex

        return [derived_verb]

    def read_intent(self, parse_dict, user_id):
        self.parse_dict = parse_dict
        self.user_id = user_id

        print("self.parse_dict:\n",self.parse_dict)
        print("self.pt_df:\n",self.pt_df)

        #    user_convo = {
        #        'context' : None,
        #        'prev_sao' : {},
        #        'formatter' : None,
        #        'request' : None,
        #        'arg' : {}
        #    }

        user_convo = self.get_user_convo(user_id)
        es_dict = self.read_parse_dict(self.parse_dict)
        print("Result -> es_dict:",es_dict)
        print("user convo:",user_convo)

        tmp_es_dict = {
            'subject':[],
            'object':[],
            'action':[]
        }
        if 'action_neg' not in es_dict.keys():
            action_neg = ''
        print("receive es_dict:",es_dict)
        print("special tokens:",self.special_tokens)
        es_dict, special_entities  = self.remove_special_tokens(es_dict)
        print("final es_dict:",es_dict)

        try:
            if user_convo['request']:
                es_action = user_convo['prev_sao']['action'].split(':')[0]
                es_subject = user_convo['prev_sao']['subject'].split(':')[0]
                es_object = es_dict['subject'].split(':')[0]
            else:
                if es_dict['action'] != []:
                    es_action = ''
                    for value in es_dict['action']:
                        if value.split(':')[0] == 'not_found' and es_action == '':
                            es_action = 'not_found'
                        elif value.split(':')[0] != 'not_found':
                            es_action = value.split(':')[0]
                else:
                    es_action = 'do'

                if 'action_neg' in es_dict.keys():
                    if es_dict['action_neg'][0].split(':')[0] == 'not':
                        action_neg = 'not'

                if es_dict['subject'] == [] and es_dict['object'] != []:
                    objects = []
                    for value in es_dict['object']:
                        if value.split(':')[3] == 'F':
                            es_dict['subject'].append(value)
                        else:
                            objects.append(value)
                    es_dict['object'] = objects
                    
                es_object = []
                es_tmp = []
                for value in map(lambda x: x.split(':'), es_dict['object']):
                    if value[3] == 'T':
                        es_tmp.append(value[0])
                    else:
                        es_object.append(value[0])
                es_object = es_tmp if es_object == [] else es_object

                #es_object = es_dict['object'][0].split(':')[0] if es_dict['object'] else None
                derived_verb = []
                derived_verb_candidate = []
                if len(es_dict['subject']) > 1:
                    subjects = []
                    prev_idx = -1
                    for value in map(lambda x: x.split(':'), es_dict['subject']):
                        if value[0] != 'not_found':
                            subjects.append(value[0])
                        if value[4] == 'T' and value[1].split('/')[0] not in ['NN']:
                            derived_verb.append(value[0])
                            derived_verb_candidate.append(value)
                    if len(subjects) == len(derived_verb):
                        derived_verb = self.choice_derived_verb(derived_verb_candidate)
                        print("Derived verb Check!!\n{}\nReturn Verb:{}".format(derived_verb_candidate, derived_verb))

                    print("++subjects:",subjects)
                    if len(subjects) > 1:
                        es_subject = "something()"
                    elif len(subjects) == 1:
                        es_subject = subjects[0]
                        subjects = None
                    else:
                        es_subject = 'not_found'
                        subjects = None
                elif len(es_dict['subject']) == 1:
                    es_subject = es_dict['subject'][0].split(':')[0] + '()' if es_dict['subject'][0] else 'entity()'
                    subjects = None
                else:
                    es_subject = 'not_found()'
                    subjects = None

            if re.match("^([0-9]+)d\(\)$", es_subject):
                es_subject = "time()"
               
            print("es_subject, es_object, es_action:",es_subject, es_object, es_action)
            send_params = {
                'user_convo':user_convo,
                'subjects':subjects,
                'action_neg':action_neg,
                'derived_verb':derived_verb,
                'compound_entities':self.compound_entities,
                'special_entities':special_entities
            }
            print("special_entities:",special_entities)
            domain, answer, *params = getattr(eval(es_subject), es_action)(es_object, **send_params)
            print("Return values: {0}, {1}, {2}".format(domain, answer, params))
        except NameError:
            traceback.print_exc()
            es_subject = es_dict['subject'][0].split(':')[0]
            cls = self.entities[self.entities['meanings'] == es_subject]['type'].tolist().pop()
            domain, answer, *params = getattr(eval('cls_' + cls), es_action)(es_object, user_convo=user_convo, subject=es_subject)
            print("Call subject class: {0}, {1}, {2}".format(domain, answer, params))

        domain = self.config['soa_info']['message'][domain] if domain in self.config['soa_info']['message'].keys() else domain

        request = None
        if answer.split(' ')[0] == "required":
            request = " ".join(answer.split(' ')[1:])

        print("params:",params)
        param = params[0] if len(params) > 0 else {}
        user_convo = {
            'context' : domain,
            'prev_sao' : {'subject':es_subject.replace("()",""), 'action':es_action, 'object':es_object},
            'formatter' : domain + ' ' + answer,
            'request' : request,
            'args' : param
        }

        self.special_tokens = []
        return domain, answer, user_convo

    def make_formatter(self, domain, context, check_dict):
        validation_value = ''
        for result in check_dict['results']:
            condition, value = list(iter(result.items()))[0]
            if value == False:
                validation_value = self.config['validation_formatter'][condition]
                break

        if validation_value != '':
            formatter = ' '.join([domain, validation_value]) 
        else:
            formatter = ' '.join([domain, context]) 

        print("formatter:",formatter)
        return formatter

    def check_domain(self, domain, context, user_id):
        results = []
        if domain != '':
            for condition in self.config['context'][domain]['conditions']:
                results.append({condition:getattr(self, condition)(domain, context)})
        check_dict = {
            'results' : results
        }
        return check_dict

    def exists_order(self, context, sub_context):
        return True

    def before_delivery(self, context, sub_context):
        return True

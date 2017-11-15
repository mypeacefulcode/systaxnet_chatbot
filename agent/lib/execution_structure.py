#-*- coding: utf-8 -*-

import sys, traceback
import pandas as pd
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None
import numpy as np
import redis, re, json

class ExecutionStructure(object):
    def __init__(self, config, redisdb, logger):
        self.logger = logger
        self.config = config
        self.redisdb = redisdb

        # Make entities dataframe
        path = self.config['entities_config']['csv_path'] + '/'
        csv_file = path + self.config['entities_config']['csv_entities_file']
        self.entities = pd.read_csv(csv_file)

        csv_file = path + self.config['entities_config']['parse_label_file']
        self.parse_label = pd.read_csv(csv_file)

        csv_file = path + self.config['entities_config']['domain_exp_file']
        self.domain_exp = pd.read_csv(csv_file).fillna("")

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
                token_pos = token.split('/')[1]
                self.pt_df.a_pos[self.pt_df.token_idx == index] = token_pos
            else:
                self.pt_df.a_pos[self.pt_df.token_idx == index] = row['pos']

    def dependency_label_rule(self, entity, my_label, parent_label, brothers_label, depth, parent_es_kind):
        labels_set = set([my_label] + brothers_label)
        print("labels_set, my_label, parent_es_kind:", labels_set, my_label, parent_es_kind)

        time_entity = False
        mean =  entity['means'].tolist()
        if len(mean) > 0:
            if mean[0].split(',')[0] in ['day']:
                time_entity = True

        es_kind = None

        es_action = set(['ROOT-VERB','ROOT-PRT','ROOT-ADJ','ADVCL-VERB','ROOT-ADJECTIVE','SUFF'])
        es_subject = set(['ROOT-NOUN','NSUBJ','AUX','CCOMP','ADVCL-NOUN'])
        es_modifier = set(['RCMOD','ADVMOD','ATTR'])
        es_object = set(['DOBJ'])
        es_special = set(['NEG', 'NUM'])

        if depth <= 1:
            for key, labels in [('action', es_action), ('subject',es_subject), ('object',es_object), ('modifier',es_modifier), ('special',es_special)]:
                s = labels_set.intersection(labels)
                print("S:",s)
                if my_label in s and key == 'modifier':
                    es_kind = key
                elif len(s) > 0 and my_label == s.pop():
                    es_kind = key
                    print("loop es kind:", key)
        else:
            print("dept else - my_label, parent_label:",my_label, parent_label)
            if my_label == "SUFF" and parent_es_kind == 'subject':
                es_kind = "subject_suff"
            elif my_label == "DEP" and parent_es_kind == 'object':
                es_kind = "object"
            elif my_label == "NN" and parent_es_kind == "subject":
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

        es_kind = self.dependency_label_rule(e, label, parent_label, brothers_label, depth, parent_es_kind)
        print("------------------------ sub end ---------------------------")

        return es_kind, e_key

    def merge_es_dict(self, es_kind, child_label, parse_dict, child_parse_dict):

        for child_es_kind in child_parse_dict.keys():
            print("child es kind key:", child_es_kind)
            print("(L)parse dict:",parse_dict)
            print("(L)Child parse dict:",child_parse_dict)
            print("(L)Child es kind:",child_es_kind)
            if child_label.split('-')[0] in ['ADVCL','RCMOD','CCOMP']:
                if es_kind + '_modifier' not in parse_dict.keys():
                    parse_dict[es_kind + '_modifier'] = []
                parse_dict[es_kind + '_modifier'] += child_parse_dict[child_es_kind]
                parse_dict[es_kind + '_modifier'].append(child_parse_dict[child_es_kind])
            elif child_es_kind in ['modifier']:
                if es_kind + '_modifier' not in parse_dict.keys():
                    parse_dict[es_kind + '_modifier'] = []
                parse_dict[es_kind + '_modifier'] += child_parse_dict[child_es_kind]
            elif child_es_kind in ['subject']:
                if child_es_kind  not in parse_dict.keys():
                    parse_dict[child_es_kind] = []
                parse_dict[child_es_kind] += child_parse_dict[child_es_kind]
            elif child_es_kind in ['special']:
                if child_label in ['NEG']:
                    if es_kind + '_neg' not in parse_dict.keys():
                        parse_dict[es_kind + '_neg'] = []
                    parse_dict[es_kind + '_neg'] += child_parse_dict[child_es_kind]
                elif child_label in ['NUM']:
                    if es_kind + '_num' not in parse_dict.keys():
                        parse_dict[es_kind + '_num'] = []
                    parse_dict[es_kind + '_num'] += child_parse_dict[child_es_kind]
            else:
                parse_dict.update(child_parse_dict)

        return parse_dict

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
                    parse_dict = self.merge_es_dict(es_kind, child_label, parse_dict, child_parse_dict)

            print("(L)Merge parse dict:",parse_dict)
        return parse_dict

    def make_execution_structure(self, df, analyzer):
        self.pt_df = df
        self.pt_df['a_pos'] = pd.Series(index=self.pt_df.index)
        self.pt_df['exec_pos'] = pd.Series(index=self.pt_df.index)
        self.pt_df.to_csv('parse_tree.csv', index=False)

        self.analyze_pos(analyzer)

        root_idx = self.pt_df.loc[self.pt_df['label'] == 'ROOT']['token_idx'].tolist().pop()

        df = self.pt_df.loc[self.pt_df['label'].isin(['ROOT','ADVCL'])]
        tokens = zip(df['token_idx'].tolist(), df['label'].tolist(), df['a_pos'].tolist())

        for idx, label, pos in tokens:
            self.pt_df.label[self.pt_df.token_idx == idx] = label + '-' + pos.upper().strip()

        print("self.pt_df:\n",self.pt_df)
        parse_dict = self.read_parse_tree(root_idx)
        print("parse_dict:",parse_dict)

        return parse_dict

    """
    def verify_answer(self, exec_dict, context, sub_context):
        answer = exec_dict['action_entity'] + exec_dict['subject_entity']
        if answer == ['아니다']:
            r_value = False
        elif answer == ['맞다']:
            r_value = True
        elif sub_context == 'select-order':
            if es_how['origin'][0].strip() in ['2 번','1 번']:
                r_value = True
            else:
                r_value = False
        else:
            r_value = None
            pass

        return r_value

    def get_next_context(self, context, sub_context):
        next_context = ''
        if context == 'cancel-order':
            if sub_context == 'begin':
                next_context = 'select-order'
            elif sub_context == 'select-order':
                next_context = 'finish'
        elif context == 'refund':
            if sub_context == 'begin':
                next_context = 'select-order'
            elif sub_context == 'select-order':
                next_context = 'finish'

        return next_context
    """

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

    def get_status(self, user_id):
        name = "CONTEXT-" + user_id
        self.user_context = self.redisdb.hgetall(name)

        if self.user_context == {}:
            self.user_context = {
                    'context':'',
                    'sub-context':'',
                    'prev-formatter':''
            }

    def save_user_context(self, user_id, user_context):
        name = "CONTEXT-" + user_id
        self.redisdb.hmset(name, user_context)

    def get_mean(self, word):
        print("word:",word)
        e = self.entities[self.entities['word'] == word]
        row, _ = e.shape
        if row == 1:
            mean = e['means'].tolist().pop()
        else:
            mean = None

        return mean

    def infer_meaning(self, es_dict):
        
        subject_pattern = '^(?=.*' + es_dict['subject'] + ').*$' if es_dict['subject'] else ''
        object_pattern = '^(?=.*' + es_dict['object'] + ').*$' if es_dict['object'] else ''
        action_pattern = '^(?=.*' + es_dict['action'] + ').*$' if es_dict['action'] else ''

        domain_df = self.domain_exp[ self.domain_exp.subject.str.contains(subject_pattern) & \
                                     self.domain_exp.object.str.contains(object_pattern) & \
                                     self.domain_exp.action.str.contains(action_pattern) ]

        print("pattern:",subject_pattern,object_pattern,action_pattern)
        print("domain_df:\n",domain_df)

        row, _ = domain_df.shape

        domain = None
        if row == 1:
            check_flag, domain_row = self.verify_domain(domain_df, es_dict)
            if check_flag:
                domain = domain_row['domain']
        else:
            print("Check domain exp!")

        context = 'begin'
        if domain == 'cancel':
            domain = domain_row['object']
            context = 'cancel'
        elif domain:
            for es_type in es_dict.keys():
                keys = es_type.split("_")
                if len(keys) > 1:
                    if keys[1] == 'neg':
                        context = 'cancel'

        print("domain, context:",domain, context)

        return domain, context

    def read_parse_dict(self, parse_dict):
        es_dict = {}
        print("1. parse_dict:",parse_dict)
        for es_type in ['subject','object','action']:
            try:
                means = []
                for value in parse_dict[es_type]:
                    if type(value) == dict:
                        child_es_dict = self.read_parse_dict(value)
                        print("child_main_es_dict:",child_es_dict)
                        means.append("child_es_dict")
                    else:
                        means.append(self.get_mean(value))
            
                for mean in means:
                    main_mean = mean

                for key in parse_dict.keys():
                    if key.startswith(es_type + "_"):
                        for value in parse_dict[key]:
                            if type(value) == dict:
                                child_es_dict = self.read_parse_dict(value)
                                mean, context = self.infer_meaning(child_es_dict)
                            else:
                                mean = self.get_mean(value)

                            _, sub_key = key.split("_")
                            print("es_type, sub_key, mean:", es_type, sub_key, mean)
                            if sub_key == 'neg':
                                es_dict[es_type + "_neg"] = mean
                            elif sub_key == 'modifier':
                                if main_mean == None:
                                    main_mean = mean

            except KeyError as e:
                traceback.print_exc()
                main_mean = None

            es_dict[es_type] = main_mean
            print("es_dict(loop):",es_dict)

        return es_dict


    def read_intent(self, parse_dict, user_id):
        self.parse_dict = parse_dict
        self.user_id = user_id

        sub_context = context = response = "" 
        
        print("self.parse_dict:\n",self.parse_dict)
        print("self.pt_df:\n",self.pt_df)

        es_dict = self.read_parse_dict(self.parse_dict)
        print("es_dict:",es_dict)
        mean, context = self.infer_meaning(es_dict)
        print("meaning, context:", mean, context)

        return mean, context

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

    def set_user_context(self, user_id, domain, context, formatter):
        self.user_context = {
            'domain':domain,
            'context':context,
            'prev-formatter':formatter
        }
        self.save_user_context(user_id, self.user_context)

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

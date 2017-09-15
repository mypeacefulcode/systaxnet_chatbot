#-*- coding: utf-8 -*-

import sys
import pandas as pd
pd.set_option('display.width', 1000)
import numpy as np
import redis

class ExecutionStructure(object):
    def __init__(self, config, redisdb, logger):
        self.logger = logger
        self.config = config
        self.redisdb = redisdb

        # Make entities dataframe
        path = self.config['entities_config']['csv_path'] + '/'
        csv_file = path + self.config['entities_config']['csv_obj_entities_file']
        self.obj_entities = pd.read_csv(csv_file)

        csv_file = path + self.config['entities_config']['csv_mind_entities_file']
        self.mind_entities = pd.read_csv(csv_file)

        csv_file = path + self.config['entities_config']['csv_actions_file']
        self.actions = pd.read_csv(csv_file)

    def read_branchs(self, token_idx, tokens_idx, branchs):
        token_idx = int(token_idx)

        c_df = self.df.loc[(self.df['head_token_idx'] == token_idx) & (self.df['token_idx'] != token_idx)]
        rows, _ = c_df.shape

        if rows == 0:
            return tokens_idx, True
        else:
            for idx in c_df['token_idx']:
                tokens_idx.append(int(idx))
                _, flag = self.read_branchs(idx, tokens_idx, branchs)

                branch = _[:]
                if flag:
                    branchs.append(branch)
                tokens_idx.pop()

        return branchs, False

    def read_parse_tree(self, df):
        self.df = df
        self.df.to_csv('parse_tree.csv', index=False)

        self.es_dict = {}
        for kind in ['when','what','how','why']:
            self.es_dict[kind] = {}
            self.es_dict[kind] = {
                'depth':-1,
                'label':None,
                'tokens':[],
                'head_token_idx':-1,
                'words':None
            }
        self.es_df = pd.DataFrame(columns=['kind','obj_entities','mind_entities','obj_means','mind_means','action','branch','morphs','origin'])

        root_idxs = self.df.loc[self.df['label'] == 'ROOT']['token_idx'].tolist()
        branchs = []
        for root_idx in root_idxs:
            r_branchs, _ = self.read_branchs(root_idx, [int(root_idx)], [])
            branchs += r_branchs

        print("branchs:",branchs)
        br_df = pd.DataFrame(branchs).fillna(-1)
        br_df = br_df.astype(int)

        rows, cols = br_df.shape
        prev_dp = cols

        p_dependency = pd.DataFrame(columns=['label','head_token_idx','idxs'])
        self.what_stack = []
        for r_idx in range(rows):
            dp = self.get_diff_position(br_df, r_idx, r_idx+1)
            cul_cols = [i for i in range(dp,cols)]
    
            t = br_df[cul_cols][r_idx:r_idx+1].values[0][::-1]
            t = t[np.where(t>-1)]

            idxs, label, head_token_idx  = self.get_dependency(self.df.loc[t], dp)

            #print("idxs1:",idxs)
            if prev_dp == dp:
                pass
            elif prev_dp > dp or dp == 0:
                idxs = self.extract_dependency(p_dependency, idxs)
                p_dependency.drop(p_dependency.index, inplace=True)
            elif prev_dp < dp:
                pass

            #print("idxs2:",idxs)
            if label in ['DOBJ','NSUBJ']:
                self.set_es('what', dp, label, idxs, head_token_idx)
            elif label in ['ADVCL']:
                self.set_es('why', dp, label, idxs, head_token_idx)
            elif label == 'ROOT':
                self.set_es('how', dp, label, idxs, head_token_idx)
            else:
                p_dependency.loc[len(p_dependency.index)] = [label, head_token_idx, idxs]

            prev_dp = dp

        if self.es_dict['what']['depth'] == -1:
            self.extract_what()

        return self.es_dict

    def extract_dependency(self, df, idxs):
        r_idxs = []
        for index, row in df.iterrows():
            r_idxs += row['idxs']
        
        r_idxs += idxs[:]
        return r_idxs

    def extract_what(self):
        print('what_stack:',self.what_stack)
        if len(self.what_stack) > 0:
            for idxs, label, head_token_idx, depth in self.what_stack:
                f_idxs = idxs
                f_label = label
                f_head_token_idx = head_token_idx
                f_depth = depth

            self.set_es('what', f_depth, f_label, f_idxs, f_head_token_idx)

    def get_diff_position(self, br_df, idx1, idx2):
        diff_p = 0
        _, cols = br_df.shape
    
        for c_idx in range(cols-1, -1, -1): 
            if br_df[c_idx][idx1:idx1+1].values != br_df[c_idx][idx2:idx2+1].values:
                diff_p = c_idx
                
        return diff_p

    def set_es(self, kind, depth, label, idxs, head_token_idx):
        if self.es_dict[kind]['depth'] > 0 and self.es_dict[kind]['depth'] > depth:
            if kind == "why":
                if self.es_dict[kind]['head_token_idx'] in idxs:
                    idxs = self.es_dict[kind]['tokens'] + idxs[:]

        self.es_dict[kind]['depth'] = depth
        self.es_dict[kind]['label'] = label
        self.es_dict[kind]['tokens'] = idxs[:]
        self.es_dict[kind]['head_token_idx'] = head_token_idx

        r = [ row['text'] if row['label'] in ['SUFF', 'PRT'] else ' ' + row['text'] \
                    for index, row in self.df.loc[idxs].iterrows()]
        self.es_dict[kind]['words'] = ''.join(r)

        return True

    def get_dependency(self, df, depth):
        backwards = False
        idxs = []

        for index, row in df.iterrows():
            if backwards:
                idxs.insert(position,row['token_idx'])
            else:
                idxs.append(row['token_idx'])
            
            backwards, position = (True, len(idxs) - 1) if row['label'] in ['SUFF','PRT'] else (False, None)
        
            label = row['label']
            head_token_idx = row['head_token_idx']

            if row['label'] in ['DOBJ']:
                self.what_stack.append((idxs[:], label, head_token_idx, depth))

        return idxs, label, head_token_idx

    def right_answer(self, es_how, context, sub_context):
        if es_how['mind_means'].tolist() == [['아니다']]:
            r_value = False
        elif es_how['mind_means'].tolist() == [['맞다']]:
            r_value = True
        elif sub_context == 'select-order':
            print(type(es_how['origin'][0].strip()))
            print(es_how['origin'][0].strip())
            if es_how['origin'][0].strip() in ['2 번','1 번']:
                r_value = True
            else:
                r_value = False
        else:
            r_value = None
            pass
            """
            if self.user_context['context'] == 'cancel-order':
                if self.user_context['sub-context'] == 'begin':
                if es_how['mind_means'].tolist() == [['아니다']]:
                    r_value = False
            """

        return r_value

    def get_next_context(self, context, sub_context):
        next_context = ''
        if context == 'cancel-order':
            if sub_context == 'begin':
                next_context = 'select-order'
            elif sub_context == 'select-order':
                next_context = 'finish'

        return next_context

    def get_context(self, user_id):
        es_how = self.es_df[self.es_df['kind'] == 'how']
        es_what = self.es_df[self.es_df['kind'] == 'what']
        sub_context = context = response = "" 
        
        for key, values in self.config['context'].items():
            if values['means'] == es_how['obj_means'].apply(sorted).apply(tuple).tolist():
                context  = key

        self.get_status(user_id)

        if context == '' and self.user_context['context'] == '':
            entities = es_how['obj_means'].tolist()
            for entity in entities:
                if 'need-to-object' in self.config['means'][entity[0]]['attribute']:
                    print('------')
                    print(es_what['obj_means'][0])
                    t = [entity[0]] + es_what['obj_means'][0]
                    t.sort()
                    for key, values in self.config['context'].items():
                        print(values['means'], [tuple(t)])
                        if values['means'] == [tuple(t)]:
                            context  = key

        print("context :",context )

        if context != "":
            if es_how['action'].tolist() == [['하다']] or es_how['action'].tolist() == [[]]:
                sub_context = "begin"
        elif self.user_context['context'] != "":
            context = self.user_context['context']
            sub_context = self.user_context['sub-context']

            flag = self.right_answer(es_how, context, sub_context)
            if flag == False:
                response = 'cancel'
            elif flag == True:
                next_context = self.get_next_context(context, sub_context)
                sub_context = next_context
            else:
                sub_context = 'unknown'
        else:
            pass

        print("sub_cotext:",sub_context)
        print("obj_means:",es_how['obj_means'].tolist())
        print("actions:",es_how['action'].tolist())
        print("response:",response)
        print("user_context:",self.user_context)

        return context , sub_context, response

    def get_status(self, user_id):
        name = "CONTEXT-" + user_id
        self.user_context = self.redisdb.hgetall(name)

        if self.user_context == {}:
            self.user_context = {
                    'context':'',
                    'sub-context':'',
                    'prev-formatter':''
            }

    def save_user_context(self, user_id, context, response):
        name = "CONTEXT-" + user_id

        if context['sub-context'] == 'begin' and response == 'cancel':
            context['context'] = ''
            context['sub-context'] = ''
        elif response == 'cancel':
            context['sub-context'] = 'begin'

        self.redisdb.hmset(name, context)

    def read_intent(self, es_dict, user_id):
        self.es_dict = es_dict
        self.user_id = user_id

        for key, values in self.es_dict.items():
            if len(values['tokens']) > 0:
                morphs=[]
                for morph_str in values['morphs'].split('|'):
                    t = morph_str.split('/')
                    morphs.append("/".join([t[0],t[2]]) if t[1] == 'None' else "/".join([t[1],t[2]]))

                df = self.obj_entities[self.obj_entities['entity'].isin(morphs)]
                obj_entities = df['entity'].tolist()
                obj_means = df['means'].tolist()
                df = self.mind_entities[self.mind_entities['entity'].isin(morphs)]
                mind_entities = df['entity'].tolist()
                mind_means = df['means'].tolist()
                df = self.actions[self.actions['words'].isin(morphs)]
                actions = df['action'].tolist()

                self.es_df.loc[len(self.es_df.index)] = [key, obj_entities, mind_entities, obj_means, mind_means, actions, \
                                                            values['tokens'], morphs, values['words']]

        print(self.es_df)
        self.es_df.to_csv('es_df.csv', index=False)
        self.context, self.sub_context, response  = self.get_context (user_id)

        print("last context: {}, {}".format(self.context, self.sub_context))

        return self.context, self.sub_context, response

    def make_formatter(self, context, sub_context, response, check_dict):
        validation_value = ''
        for result in check_dict['results']:
            condition, value = list(iter(result.items()))[0]
            if value == False:
                validation_value = self.config['validation_formatter'][condition]
                break

        if validation_value != '':
            formatter = ' '.join([context, validation_value]) 
        else:
            formatter = ' '.join([context, sub_context, response]) 

        return formatter

    def set_user_context(self, user_id, context, sub_context, response, formatter):
        self.user_context = {
            'context':context,
            'sub-context':sub_context,
            'prev-formatter':formatter
        }
        self.save_user_context(user_id, self.user_context, response)

    def check_domain(self, context, sub_context, user_id):
        results = []
        for condition in self.config['context'][context]['conditions']:
            results.append({condition:getattr(self, condition)(context, sub_context)})
            
        check_dict = {
            'results' : results
        }
        return check_dict

    def exists_order(self, context, sub_context):
        return True

    def before_delivery(self, context, sub_context):
        return True

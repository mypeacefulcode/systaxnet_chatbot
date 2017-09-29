#-*- coding: utf-8 -*-

import sys, traceback
import pandas as pd
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None
import numpy as np
import redis

class ExecutionStructure(object):
    def __init__(self, config, redisdb, logger):
        self.logger = logger
        self.config = config
        self.redisdb = redisdb

        self.kinds = ['when','what','how','why']

        # Make entities dataframe
        path = self.config['entities_config']['csv_path'] + '/'
        csv_file = path + self.config['entities_config']['csv_entities_file']
        self.entities = pd.read_csv(csv_file)

        csv_file = path + self.config['entities_config']['parse_label_file']
        self.parse_label = pd.read_csv(csv_file)

    def verify_branchs(self, branchs):
        check_label = token_idx = None
        idx = 0
        new_branchs = []
        for branch in branchs:
            label = self.pt_df[self.pt_df['token_idx'] == branch[-1:]]['label'].tolist().pop()
            if set([label, check_label]) == set(['PRT','SUFF']) and branch[-2:-1] == check_idx:
                if label == 'PRT':
                    last_idx = branch[-1:]
                    use_branch = idx - 1
                else:
                    last_idx = branchs[idx-1][-1:]
                    use_branch = idx
                new_branchs.pop()
                branch = branchs[use_branch] + last_idx
            elif label in ['PRT','SUFF']:
                check_idx = branch[-2:-1]
                check_label = label
            else:
                token_idx = -1
                check_label = ""

            new_branchs.append(branch)
            idx += 1

        return new_branchs

    def read_branchs(self, token_idx, tokens_idx, branchs):
        token_idx = int(token_idx)

        c_df = self.pt_df.loc[(self.pt_df['head_token_idx'] == token_idx) & (self.pt_df['token_idx'] != token_idx)]
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

    def read_parse_tree(self, df):
        self.pt_df = df
        self.pt_df['a_pos'] = pd.Series(index=self.pt_df.index)
        self.pt_df['exec_pos'] = pd.Series(index=self.pt_df.index)
        self.pt_df.to_csv('parse_tree.csv', index=False)

        self.es_dict = {}
        for kind in self.kinds:
            self.es_dict[kind] = {}
            self.es_dict[kind] = {
                'depth':-1,
                'label':None,
                'tokens':[],
                'head_token_idx':-1,
                'words':None
            }
        self.es_df = pd.DataFrame(columns=['kind','obj_entities','mind_entities','obj_means','mind_means','action','branch','morphs','origin'])

        root_idxs = self.pt_df.loc[self.pt_df['label'] == 'ROOT']['token_idx'].tolist()
        branchs = []
        for root_idx in root_idxs:
            r_branchs, _ = self.read_branchs(root_idx, [int(root_idx)], [])
            if type(r_branchs[0]) == int:
                r_branchs = [r_branchs]
            branchs += r_branchs

        print("branchs:",branchs)
        branchs = self.verify_branchs(branchs)
        print("verify branchs:",branchs)
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

            idxs, label, head_token_idx  = self.get_branch_info(self.pt_df.loc[t], dp)

            if prev_dp == dp:
                pass
            elif prev_dp > dp or dp == 0:
                idxs = self.concat_branch(p_dependency, idxs)
                p_dependency.drop(p_dependency.index, inplace=True)
            elif prev_dp < dp:
                pass

            if label in ['DOBJ','NSUBJ','NSUBJPASS']:
                self.set_es('what', dp, label, idxs, head_token_idx)
            elif label in ['ADVCL']:
                self.set_es('why', dp, label, idxs, head_token_idx)
            elif label == 'ROOT':
                self.set_es('how', dp, label, idxs, head_token_idx)
            else:
                p_dependency.loc[len(p_dependency.index)] = [label, head_token_idx, idxs]

            prev_dp = dp

        #if self.es_dict['what']['depth'] == -1:
        #    self.get_what()

        return self.es_dict

    def concat_branch(self, df, idxs):
        r_idxs = []
        for index, row in df.iterrows():
            r_idxs += row['idxs']
        
        r_idxs += idxs[:]
        return r_idxs

    def get_what(self):
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
                    for index, row in self.pt_df.loc[idxs].iterrows()]
        self.es_dict[kind]['words'] = ''.join(r)

        return True

    def get_branch_info(self, df, depth):
        idxs = []

        for index, row in df[::-1].iterrows():
            idxs.append(row['token_idx'])
        
            if len(idxs) <= 1:
                label = row['label']
                head_token_idx = row['head_token_idx']

            if row['label'] in ['DOBJ']:
                self.what_stack.append((idxs[:], label, head_token_idx, depth))

        return idxs, label, head_token_idx

    def verify_answer(self, exec_dict, context, sub_context):
        if exec_dict['execution_entity'] == ['아니다']:
            r_value = False
        elif exec_dict['execution_entity'] == ['맞다']:
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

        return next_context

    def read_dependency(self, branch):
        parse_dict = {
            'what':[],
            'how':[],
            'when':[],
            'why':[]
        }
        prev_label = ''
        dependency = []

        try:
            print("read dependency -> branch:",branch)
            for idx in branch[::-1]:
                df = self.pt_df[self.pt_df['token_idx'] == idx]
                label = df['label'].tolist().pop()
                head_token_idx = df['head_token_idx'].tolist().pop()
                head_df = self.pt_df[self.pt_df['token_idx'] == head_token_idx]
                head_label = head_df['label'].tolist().pop()

                label_type = self.parse_label[self.parse_label['label'] == label]['type'].tolist().pop()
                if label_type != 'pass':
                    if label in ['DEP','RCMOD','NN']:
                        dependency.insert(0,idx)
                    else:
                        if len(dependency) > 0:
                            dependency.insert(0,idx)
                            parse_dict[label_type].insert(0,dependency)
                            dependency = []
                        else:
                            parse_dict[label_type].insert(0,idx)
                prev_label = label
        except IndexError:
            traceback.print_exc()
            print("Unknown parse label:", label)

        if len(dependency) > 0:
            parse_dict[label_type] = [parse_dict[label_type] + dependency]

        return parse_dict

    def read_context(self, user_id, exec_dict):
        context = ""
        # Normal case
        for key, values in self.config['context'].items():
            if set(values['means']).issubset(set(exec_dict['execution_entity'])):
                context  = key
            elif 'cancel' in exec_dict['action_entity']:
                if set(values['means']).issubset(set(exec_dict['execution_entity'] + ['cancel'])):
                    context  = key

        if context == "":
            for key, values in self.config['context'].items():
                if set(values['means']).issubset(set(exec_dict['action_entity'])):
                    context  = key

        if context == "":
            for key, values in self.config['context'].items():
                if set(values['means']).issubset(set(exec_dict['purpose_entity'])):
                    if '하다' in exec_dict['execution_entity']:
                        context  = key

        if context == "":
            for key, values in self.config['context'].items():
                if set(values['means']).issubset(set(exec_dict['purpose_entity'] + exec_dict['action_entity'])):
                    if exec_dict['execution_entity'] == []:
                        exec_dict['action_entity'].append('하다')
                        context  = key

        print("context :",context )
        self.get_status(user_id)

        response = ""
        if context != "":
            if '하다' in exec_dict['action_entity'] or exec_dict['action_entity'] == []:
                sub_context = "begin"
            else:
                sub_context = 'unknown'
        elif self.user_context['context'] != "":
            context = self.user_context['context']
            sub_context = self.user_context['sub-context']

            flag = self.verify_answer(exec_dict, context, sub_context)
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

    def make_execution_structure(self, kind, idxs, exec_dict, analyzer):
        """
        exec_dict = {
            "execution_entity":"",
            "action_entity":"",
            "purpose_entity":"",
            "reason_entity":"",
            "time_entity":""
        }
        """
        print("es_idxs:",idxs)
        prev_label = ""
        child_entity = {}
        self.pt_df = self.pt_df.fillna('')

        for idx in idxs:
            if type(idx) == list:
                for i in idx[::-1]:
                    t_df = self.pt_df[self.pt_df['token_idx']==i]
                    row = t_df.ix[t_df.index[0]]
                    token = row['text'] + "/" + row['a_pos']

                    e = self.entities[self.entities['word'] == token]
                    e = e.fillna('None')
                    if e.size == 0:
                        continue

                    e = e.ix[e.index[0]]
                    mean = e['means'].split(',')

                    if set(mean).issubset(set(self.config['special_obj'])):
                        p_e = e
                        p_mean = mean
                    elif set(mean).issubset(set(self.config['special_act'])):
                        p_e = e
                        p_mean = mean
                e_idx = i
                e = e if p_e.size == 0 else p_e
                mean = mean if p_e.size == 0 else p_mean
            else:
                e_idx = idx

                t_df = self.pt_df[self.pt_df['token_idx']==e_idx]
                row = t_df.ix[t_df.index[0]]
                token = row['text'] + "/" + row['a_pos']

                e = self.entities[self.entities['word'] == token]
                e = e.fillna('None')

                if e.size == 0:
                    continue

                e = e.ix[e.index[0]]
                mean = e['means'].split(',')

            if e.size > 0:
                label = self.pt_df[self.pt_df['token_idx'] == e_idx]['label'].tolist().pop()
                pos = self.pt_df[self.pt_df['token_idx'] == e_idx]['pos'].tolist().pop()

                head_exec_pos = ""
                head_label = ""
                if label != "ROOT":
                    head_token_idx = self.pt_df[self.pt_df['token_idx'] == e_idx]['head_token_idx'].tolist().pop()
                    head_label = self.pt_df[self.pt_df['token_idx'] == head_token_idx]['label'].tolist().pop()
                    head_pos = self.pt_df[self.pt_df['token_idx'] == head_token_idx]['a_pos'].tolist().pop()
                    head_exec_pos = self.pt_df[self.pt_df['token_idx'] == head_token_idx]['exec_pos'].tolist().pop()
                else:
                    root_label = self.pt_df[self.pt_df['token_idx'] == e_idx]['label'].tolist().pop()
                    root_pos = self.pt_df[self.pt_df['token_idx'] == e_idx]['a_pos'].tolist().pop()

                exec_pos = ""
                child_means = child_entity[e_idx] if e_idx in child_entity.keys() else []
                mean = child_means + mean
                print("child_entity:",child_entity)
                print("child_means:",child_means)
                print("mean:",mean)
                print("label:",label)
                print("head_label:",head_label)
                if label == 'ROOT':
                    if 'need-to-obj' in e['attr']:
                        exec_dict['action_entity'] = mean + exec_dict['action_entity']
                        exec_pos = "action_entity"
                    else:
                        if root_pos.upper() in ["VERB","ADJ"]:
                            exec_dict['action_entity'] += mean
                            exec_pos = "action_entity"
                        else:
                            exec_dict['execution_entity'] += mean
                            exec_pos = "execution_entity"
                elif label == 'SUFF':
                    if head_label == 'ROOT':
                        exec_dict['action_entity'] += mean
                        exec_pos = "action_entity"
                    elif head_label == 'AUX' and head_exec_pos in ["execution_entity", "action_entity"]:
                        #exec_dict['action_entity'] = mean + exec_dict['action_entity']
                        exec_dict['action_entity'] += mean 
                        exec_pos = "action_entity"
                elif label in ['NN','NSUBJ','DOBJ']:
                    if head_label == 'ROOT':
                        exec_dict['execution_entity'] = mean + exec_dict['execution_entity']
                        exec_pos = "execution_entity"
                    elif head_label in ['NN','NSUBJ','DOBJ']:
                        if head_exec_pos == "":
                            if head_token_idx in child_entity.keys():
                                child_entity[head_token_idx] += mean
                            else:
                                child_entity[head_token_idx] = mean
                        else:
                            exec_dict[head_exec_pos] = mean + exec_dict['execution_entity']
                            exec_pos = head_exec_pos
                    else:
                        exec_dict['purpose_entity'] += mean
                        exec_pos = "purpose_entity"
                elif label in ['AUX']:
                    if head_label == 'ROOT' and head_pos in ['VERB','ADJ']:
                        exec_dict['action_entity'] = mean + exec_dict['action_entity']
                        exec_pos = "action_entity"
                    elif head_label == 'ROOT':
                        exec_dict['execution_entity'] = mean + exec_dict['execution_entity']
                        exec_pos = "execution_entity"
                else:
                    print("++++Pass label++++\n",label)

                self.pt_df.exec_pos[self.pt_df.token_idx == e_idx] = exec_pos

        return exec_dict

    def read_intent(self, es_dict, user_id, analyzer):
        self.es_dict = es_dict
        self.user_id = user_id

        sub_context = context = response = "" 
        
        exec_dict = {
            "execution_entity":[],
            "action_entity":[],
            "purpose_entity":[],
            "reason_entity":[],
            "time_entity":[]
        }

        self.analyze_pos(analyzer)

        print("self.es_dict:\n",self.es_dict)
        print("self.pt_df:\n",self.pt_df)

        branch = es_dict['how']['tokens']
        parse_dict = self.read_dependency(branch)
        print(parse_dict)
        for key, value in parse_dict.items():
            if key in ['how','what']:
                exec_dict = self.make_execution_structure(key,value, exec_dict, analyzer)
        print(exec_dict)

        branch = es_dict['what']['tokens']
        parse_dict = self.read_dependency(branch)
        print(parse_dict)
        for key, value in parse_dict.items():
            if key in ['how','what']:
                exec_dict = self.make_execution_structure(key,value, exec_dict, analyzer)

        print(exec_dict)
        self.context, self.sub_context, response  = self.read_context(user_id, exec_dict)

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

        print("formatter:",formatter)
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

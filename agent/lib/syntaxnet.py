#-*- coding: utf-8 -*-

import sys, traceback
import pandas as pd
import googleapiclient.discovery
import itertools, time, re

class Syntaxnet(object):
    def __init__(self, logger, entities):
        """Returns the encoding type that matches Python's native strings."""
        if sys.maxunicode == 65535:
            self.encoding = 'UTF16'
        else:
            self.encoding = 'UTF32'

        self.service = googleapiclient.discovery.build('language', 'v1')
        self.logger = logger
        self.entities = entities

    def analyze_syntax(self, text):
        body = {
            'document': {
                'type': 'PLAIN_TEXT',
                'content': text,
            },
            'encoding_type': self.encoding
        }

        try:
            request = self.service.documents().analyzeSyntax(body=body)
            response = request.execute()
        except googleapiclient.errors.HttpError as e:
            response = {'error': e}

        return response

    def token_to_string(self, response):
        result = ''
        idx = 0
        for token in response['tokens']:
            result += '[{}] {}: {}: {} ({})\n'.format(idx, token['partOfSpeech']['tag'], token['text']['content'],
                                               token['dependencyEdge']['headTokenIndex'], token['dependencyEdge']['label'])
            idx += 1

        return result

    def verify_parse_tree(self, df):
        bug_tokens = [("UNKNOWN","할께"),("NUM","하나")]

        for pos, text in bug_tokens:
            t_df = df[(df['pos'] == pos) & (df['text'] == text)]
            if t_df.size > 0 and text == "할께":
                token_idx = t_df['token_idx'].tolist().pop()
                bug_df = df[(df['head_token_idx'] == token_idx) & (df['token_idx'] != token_idx)]
                child_idxs = bug_df['token_idx'].tolist()

                for child_idx in child_idxs:
                    child_df = df[df['token_idx'] == child_idx]
                    if child_df['label'].tolist().pop() == 'NN':
                        df.label[df.token_idx == child_idx] = 'DOBJ'
            elif t_df.size > 0 and text == "하나":
                token_idx = t_df['token_idx'].tolist().pop()
                child_idx = token_idx - 1
                lemma = df[df['token_idx'] == child_idx ]['text'].tolist().pop()
                pos = df[df['token_idx'] == child_idx]['pos'].tolist().pop()
                e_key = lemma + '/' + pos

                e = self.entities[self.entities['word'] == e_key]
                row, _ = e.shape
                if row == 1 and e['d-verb'].tolist().pop() == 'T':
                    df.pos[df.token_idx == token_idx] = 'VERB'
        return df

    def token_to_dataframe(self, response):
        d = {
                'token_idx':[],
                'pos':[],
                'text':[],
                'head_token_idx':[],
                'label':[]
            }

        idx = 0
        dec_idx = 0
        for token in response['tokens']:
            token_idx = idx
            # not used
            if token['dependencyEdge']['label'] == '-------':
                dec_idx += 1
                token['partOfSpeech']['tag'] = d['pos'][idx-dec_idx]
                token['text']['content'] = d['text'][idx-dec_idx] + token['text']['content']
                token['dependencyEdge']['headTokenIndex'] = d['head_token_idx'][idx-dec_idx]
                token['dependencyEdge']['label'] = d['label'][idx-dec_idx]
                for key in d.keys():
                    d[key].pop()

                token_idx -= 1

            d['token_idx'].append(token_idx)
            d['pos'].append(token['partOfSpeech']['tag'])
            d['text'].append(token['text']['content'])
            d['head_token_idx'].append(token['dependencyEdge']['headTokenIndex'])
            d['label'].append(token['dependencyEdge']['label'])

            idx += 1

        df = pd.DataFrame.from_dict(d)
        df = df.set_index(df['token_idx'])
        df = self.verify_parse_tree(df)

        return df

    def verify_segmentation(self, response, sentence, verify_dict):
        prev_word = segment_str = ''
        prev_offset = 0
        word_list = []
        loop_cnt = 0
        last_loop = False
        for token in response['tokens']:
            loop_cnt += 1
            pos = token['partOfSpeech']['tag']
            word = token['text']['content']
            offset = token['text']['beginOffset']

            if loop_cnt == len(response['tokens']):
                word_list.append(word)
                last_loop = True

            if prev_offset + len(prev_word) != offset or last_loop:
                x = start_i = 0
                word_idxs = [i for i in range(len(word_list))]
                for r in [3,2]:
                    for i in range(start_i, len(word_list) - r + 1):
                        idx = i + x
                        s_word = ''.join(word_list[idx:idx+r])

                        df = verify_dict.loc[verify_dict['word'] == s_word]
                        row, _ = df.shape

                        if row == 1:
                            for sub_i in range(idx,idx+r):
                                word_idxs[sub_i] = idx

                            x = idx + r
                            start_i = x

                morphs = ''
                prev_idx = loop_idx = 0
                for idx in word_idxs:
                    if prev_idx != idx:
                        morphs += ' '
                    morphs += str(word_list[loop_idx])
                    prev_idx = idx
                    loop_idx += 1

                if re.match('.+원함$',morphs):
                    w_list = []
                    for w in morphs.split('원함'):
                        w = w if w != '' else '원함'
                        w_list.append(w)
                    morphs = ' '.join(w_list)

                segment_str += ' ' + morphs
                word_list = []

            word_list.append(word)
            prev_offset = offset + len(word)

        return segment_str

    def save_respons(self, mongodb, response, corpus, service_type):
        doc = {
            'corpus' : corpus,
            'created_date' : time.time(),
            'service' : service_type,
            'type' : 'L',
            'parse_tree' : response
        }
        try:
            mongodb.corpusdb.service_log.insert(doc)
        except:
            self.logger(traceback.print_exc())

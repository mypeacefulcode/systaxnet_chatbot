#-*- coding: utf-8 -*-

import sys, traceback
import pandas as pd
import googleapiclient.discovery

class Syntaxnet(object):
    def __init__(self, logger):
        """Returns the encoding type that matches Python's native strings."""
        if sys.maxunicode == 65535:
            self.encoding = 'UTF16'
        else:
            self.encoding = 'UTF32'

        self.service = googleapiclient.discovery.build('language', 'v1')
        self.logger = logger

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
        bug_tokens = [("UNKNOWN","할께")]

        for pos, text in bug_tokens:
            t_df = df[(df['pos'] == pos) & (df['text'] == text)]
            if t_df.size > 0:
                token_idx = t_df['token_idx'].tolist().pop()
                bug_df = df[(df['head_token_idx'] == token_idx) & (df['token_idx'] != token_idx)]
                child_idxs = bug_df['token_idx'].tolist()

                for child_idx in child_idxs:
                    child_df = df[df['token_idx'] == child_idx]
                    if child_df['label'].tolist().pop() == 'NN':
                        df.label[df.token_idx == child_idx] = 'DOBJ'

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

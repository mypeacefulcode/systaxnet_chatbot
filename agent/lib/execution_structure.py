#-*- coding: utf-8 -*-

import pandas as pd

class ExecutionStructure(object):
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config

        # Make entities dataframe
        path = self.config['entities_config']['csv_path'] + '/'
        csv_file = path + self.config['entities_config']['csv_entities_file']
        self.entities = pd.read_csv(csv_file)

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
        df.to_csv('parse_tree.csv', index=False)
        print(df)

        self.df = df
        root_idx = df.loc[df['label'] == 'ROOT']['token_idx']
        branchs, _ = self.read_branchs(root_idx, [int(root_idx)], [])

        print("R:",branchs)

from itertools import groupby
from mapper import mapper
import pandas as pd
import numpy as np
import config
import os

class Xgb_Swinga:
    '''
    Purpose: XGB + CCI + STOCHRSI
    '''
    def __init__(self, data):
        self.data = data.loc[:, :]

    def prep_df(self, instructions):

        df = self.data.copy()

        indicator = mapper[instructions['strategy'][0]['indicator']]['map_to']

        df['decision'] = np.select(
            [
                (df[indicator] >= instructions['strategy'][0]['buy_at']),
                (df[indicator] < instructions['strategy'][0]['sell_at'])
            ],
            [
                1,
                0
            ],
            default=np.nan
        )

        df['decision'].fillna(method='ffill', inplace=True)

        df['askclose_lead'] = df['askclose'].shift(-1)
        # need the lead askhigh and asklows to check if stop losses or take profits will be hit
        df['askhigh_lead'] = df['askhigh'].shift(-1)
        df['asklow_lead'] = df['asklow'].shift(-1)

        grouped = [list(g) for k, g in groupby(df.decision.tolist())]
        df['change_no'] = np.repeat(range(len(grouped)),[len(x) for x in grouped])+1

        agg_cols = [col for col in list(df) if col not in config.EXCLUDE_FROM_AGG]
        aggs = {col: 'first' for col in agg_cols}

        decisions = (df
                     .groupby(['decision', 'change_no'], as_index=False)
                     .agg({
                         'date': ['first', 'last'],
                         'askclose': 'first',
                         'askclose_lead': 'last',
                         'askhigh_lead': 'max',
                         'asklow_lead': 'min',
                         **aggs
                     })
                    )

        decisions.columns = ['decision', 'change_no',
                             'start_date', 'stop_date',
                            'start_price', 'stop_price',
                            'max_askhigh', 'min_asklow'] + agg_cols

        decisions.sort_values('start_date', inplace=True)
        decisions.reset_index(drop=True, inplace=True)

        decisions['target'] = np.select(
            [
                (decisions['decision'] == 0) & \
                (decisions['stop_price'] < decisions['start_price']),

                (decisions['decision'] == 1) & \
                (decisions['stop_price'] > decisions['start_price'])
            ],
            [
                1,
                1
            ],
            default=0
        )

        return decisions

    def get_pred(self, model, latest_decision, threshold):

        latest_decision['pred'] = model.score(latest_decision)

        return latest_decision[latest_decision.pred>threshold].reset_index(drop=True)

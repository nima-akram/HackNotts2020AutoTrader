#!/usr/bin/env python3
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import numpy as np
import datetime
import fxcmpy
import pickle
import time
import math
import sys
import os

import json

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from trading_strategies import Xgb_Swinga
import config
import market
import leverage

def xgb_score(market, starting_margin, instructions):
    '''
    Purpose: A backtesting function specifically curated for xgboost backtesting.
             Sticking to the same structure become too complex and unreadable
    '''

    all_trades_df = pd.DataFrame()

    # instrument='NAS100'
    # market=m

    for instrument in config.NAIVE_LIST:

        print(f'Getting predictions for: {instrument}')

        df = market.market_five_data[market.market_five_data.instrument == instrument]

        strategy = Xgb_Swinga(df)
        decisions = strategy.prep_df(instructions)

        decisions = decisions[decisions.decision!=-1].reset_index(drop=True)

        print(f'% Successful trades with pure trading strategy: {decisions.target.mean()}')

        pred_df=decisions.copy()

        pred_df['percentage_move'] = (pred_df['stop_price'] / \
                                        pred_df['start_price'])-1
        pred_df['percentage_move'] = np.select(
            [
                (pred_df['decision'] == 1) & (pred_df['percentage_move']>0),
                (pred_df['decision'] == 0) & (pred_df['percentage_move']<=0),
                (pred_df['decision'] == 0) & (pred_df['percentage_move']>0),
                (pred_df['decision'] == 1) & (pred_df['percentage_move']<=0),
            ],
            [
                pred_df['percentage_move'] * 1,
                pred_df['percentage_move'] * -1,
                pred_df['percentage_move'] * -1,
                pred_df['percentage_move'] * 1
            ],
            default=-0.00000001
        )

        all_trades_df = all_trades_df.append(pred_df)

    all_trades_df = (all_trades_df
                    .sort_values(by='stop_date', ascending=True)
                    .reset_index(drop=True)
                    )

    all_trades_df = all_trades_df[[
        'stop_date',
        'instrument',
        'start_price',
        'stop_price',
        'decision',
        'percentage_move'
    ]]

    all_trades_df.columns = [
        'time',
        'symbol',
        'askopen',
        'askclose',
        'buy_or_sell',
        'percentage_move'
    ]

    # figure out if sl | tp would have been hit
    # starting_margin=50_000
    usable_margin = starting_margin
    margin_history = pd.DataFrame()
    for i in range(all_trades_df.shape[0]):

        trade_size = usable_margin*0.001

        pnl = trade_size * \
        all_trades_df.loc[i].percentage_move * \
        leverage.dynamic[instrument]

        usable_margin+=pnl

        margin_history = margin_history.append(
            pd.DataFrame({
                'date': [all_trades_df.loc[i].time],
                'margin': [usable_margin]
            }))

    # adding raw pnl per trade back to main closed trades df
    all_trades_df['pnl'] = margin_history['margin'].reset_index(drop=True) - \
                           margin_history['margin'].reset_index(drop=True).shift(1)
    # due to the way raw pnl was calculated, first record is empty
    # so needs to be repopulated
    all_trades_df['pnl'].iloc[0] = margin_history['margin'].reset_index(drop=True).loc[0] - \
                                  starting_margin
    # this column is now unnecessary
    all_trades_df.drop('percentage_move', axis=1, inplace=True)

    backtesting_stats = {
        'margin_history': margin_history,
        'closed_trades': all_trades_df
    }

    return backtesting_stats

def main(strat, output_directory, backtest_steps,
         margin, training_data, instructions):

    # sort out output file location set up for app
    if not os.path.exists(output_directory): os.makedirs(output_directory)
    timestamp_file = (str(datetime.datetime.now().replace(microsecond=0))
                        .replace(' ', '_')
                        .replace(':', '_')
                     )
    output_file_name = f"{strat}_{timestamp_file}-simple.pkl"

    m = market.Market(
        instructions['time_granularity'],
        backtest_steps=False,
        selected_instrument=instructions['instrument'],
        training_data=training_data,
        from_date=instructions['from_date'],
        to_date=instructions['to_date']
    )

    backtesting_stats = xgb_score(m, margin, instructions)

    return backtesting_stats

if __name__ == '__main__':

    main(
        strat='YourStrat',
        output_directory='score/myapp/data',
        backtest_steps=False,
        margin=50_000,
        training_data=True,
        instructions=False
    )

# strat='Xgb_Swinga'
# output_directory='score/data'
# backtest_steps=False
# margin=50_000
# training_data=True
# input_json='rsi_input.json'

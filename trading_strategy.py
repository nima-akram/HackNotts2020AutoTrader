from google.cloud import bigquery_storage_v1beta1
from google.cloud import bigquery
from itertools import groupby
import datetime as dt
import pandas as pd
import numpy as np
import fxcmpy
import pickle
import time
# import os, sys
# module_path = os.path.abspath(os.path.join('..'))
# if module_path not in sys.path:
#     sys.path.append(module_path)
import config

class CompileData(object):

    def __init__(self, training_data=False):
        '''
        Purpose: Initiate the Bayesian Optimisation Model Object
        Input:
            target: the target variable name, single string
            cols_to_exclude: specify columns (by name in list) that you want to exclude, if empty, []
            categorical_cols: specify the categorical column names, for category encoding
            to_split: True/False split a train test set
            split_size: float between 0 and 1 for the percentage size of dataset to split into test
        '''
        self.training_data = training_data

    def compile_trading_strategy(self, con, instruments, period, chart_patterns, number=False, from_date=False, to_date=False):

        if self.training_data:
            #, all_m1_data
            print(instruments)
            all_data = self.data_collection(
                con=con,
                instruments=instruments,
                period=period,
                from_date=from_date,
                to_date=to_date
            )
        else:
            all_data = self.data_collection(
                con=con,
                instruments=instruments,
                period=period,
                number=number
            )

        # transform the data and compute the indicators
        merged_df = pd.DataFrame()
        for key in all_data.keys():
            print(key)

            merged_df = merged_df.append(all_data[key]).reset_index(drop=True)

        # all_m1_data_merged = pd.DataFrame()
        # if self.training_data:
        #     for key in all_m1_data.keys():
        #         all_m1_data_merged = all_m1_data_merged.append(all_m1_data[key]).reset_index(drop=True)

            # return merged_df
            #, all_m1_data_merged

        # else:

        return merged_df

    def data_collection(self, con, instruments, period, number=False, from_date=False, to_date=False):
        '''
        Purpose: Collect historical price data for specified instruments
                 at multiple time points
        Input:
            instruments: list of names of instrument to collect (fxcm)
            period: (list) the time granularity of the data
            start, stop: the date range of data points to retrieve (max 1000)
        '''

        all_data = {}
        # all_m1_data = {}

        if self.training_data:

            for instrument in instruments:

                print(instrument)
                bq_client = bigquery.Client()
                bq_storageclient = bigquery_storage_v1beta1.BigQueryStorageClient()

                # Download query results.
                asset = config.DARWINEX_TRANSLATION[instrument]
                query_string = f"""
                SELECT
                *
                FROM `lyrical-catfish-252315.dwx_training.{asset}_{period}`
                WHERE date >= '{from_date}' AND date <= '{to_date}'
                ORDER BY date DESC
                """

                results = (
                    bq_client.query(query_string)
                    .result()
                )

                data = results.to_dataframe(bqstorage_client=bq_storageclient)
                data = data.sort_values('date').reset_index(drop=True)

                # get 1 minute data synchronised to the main data so
                # that stop loss and take profit can be more accurately calculated
                # in backtesting
                # min_date = str(data.date.min().replace(tzinfo=None))
                #
                # query_string = f"""
                # SELECT
                # date, askopen, askclose, askhigh, asklow
                # FROM
                # `lyrical-catfish-252315.dwx_training.{asset}_m1`
                # WHERE date >= '{min_date}'
                # ORDER BY date DESC
                # """
                #
                # results = (
                #     bq_client.query(query_string)
                #     .result()
                # )
                #
                # data_m1 = results.to_dataframe(bqstorage_client=bq_storageclient)
                # data_m1 = data_m1.sort_values('date').reset_index(drop=True)
                #
                # data_m1['instrument'] = instrument
                #
                data['instrument'] = instrument
                all_data[instrument] = data
                #
                # all_m1_data[instrument] = data_m1

            return all_data
            #, all_m1_data

        else:

            for instrument in instruments:

                data = con.get_candles(
                    instrument=instrument,
                    period=period,
                    number=number
                )
                data['date'] = data.index
                data.reset_index(drop=True, inplace=True)
                data.drop(['bidopen', 'bidclose', 'bidhigh', 'bidlow'],
                          inplace=True, axis=1)

                data['instrument'] = instrument
                all_data[instrument] = data

            return all_data

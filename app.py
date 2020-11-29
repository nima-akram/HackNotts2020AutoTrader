import streamlit as st

import json
from mapper import mapper
import backtest as bt

from datetime import datetime
from datetime import timedelta
import plotly.express as px
import streamlit as st
import pandas as pd
import numpy as np
import pickle
import os

# get relevant data
def get_data(output_directory):
    '''
    Purpose: Get all the data from the output directories
    '''

    backtests = {}
    backtest_files = [file for file in os.listdir(output_directory) if ".pkl" in file]

    for file in backtest_files:

        with open(os.path.join(output_directory, file), 'rb') as fp:
            data = pickle.load(fp)

        backtests[file] = data

    return backtests

def format_closed_trades(data):
    '''
    Purpose: Function specifically intended to clean up the "closed_trades" dataframe
    '''

    closed_trades = data[[
        'time',
        'symbol',
        'askopen',
        'askclose',
        'buy_or_sell',
        'pnl'
    ]]
    closed_trades.columns = [
        'Trade Close Time',
        'Instrument',
        'Ask Open',
        'Ask Close',
        'Buy or Sell?',
        'Profit / Loss'
    ]
    closed_trades.reset_index(drop=True, inplace=True)

    return closed_trades

def calculate_max_drawdown(data):
    '''
    Purpose: Given a list/series/np array, calculate the maximum drawdown in value
    '''

    mdd_arr = np.array(data.values)

    i = np.argmax(np.maximum.accumulate(mdd_arr) - mdd_arr) # end of the period
    j = np.argmax(mdd_arr[:i]) # start of period

    return abs(mdd_arr[i] - mdd_arr[j]) * -1

def summary_statistics(closed_trades, margin_history):
    '''
    Purpose: Get all the closed trades and calculate high level statistics.
    Return: Dictionary of the following summary statistics
            - Starting Margin
            - Ending Margin
            - Highest Margin
            - Lowest Margin
            - Total Returns
            - Percentage Return (End compared to Start)
            - Max Drawdown
            - Number of Successful Trades
            - Number of Unsuccessful Trades
            - % of Successful Trades
            - % Number of Days Profitable
            - % Number of Weeks Profitable
    '''

    summary_stats = {}
    df = closed_trades.copy()
    mhist = margin_history.copy()

    # calculate summary stats from margin_history (history of margins over time)
    summary_stats['Start Time'] = str(mhist.date.iloc[0])
    summary_stats['End Time'] = str(mhist.date.iloc[-1])
    summary_stats['Starting Margin'] = mhist.margin.iloc[0]
    summary_stats['Ending Margin'] = round(mhist.margin.iloc[-1], 2)
    summary_stats['Highest Margin'] = round(mhist.margin.max(), 2)
    summary_stats['Lowest Margin'] = round(mhist.margin.min(), 2)
    summary_stats['Total Returns'] = round(summary_stats['Ending Margin'] - \
                                     summary_stats['Starting Margin'], 2)
    summary_stats['% Return'] = round(((summary_stats['Ending Margin'] / \
                                          summary_stats['Starting Margin']) - 1)*100, 2)
    summary_stats['Max Drawdown'] = round(calculate_max_drawdown(mhist.margin), 2)
    summary_stats['% Max Drawdown'] = round((((abs(summary_stats['Max Drawdown']) / \
                                         summary_stats['Starting Margin'])) * -1)*100, 2)

    # add extra columns to allow for summary statistics calculations
    df['Day'] = df['Trade Close Time'].apply(lambda x: x.date())
    df['Week'] = df['Day'].apply(lambda x: x.strftime('%W'))
    df['profit'] = np.where(df['Profit / Loss'] > 0, 1, 0)
    df['loss'] = np.where(df['Profit / Loss'] < 0, 1, 0)

    # calculate very high level summary stats from the closed trades
    summary_stats['Total Number of Trades'] = df['profit'].count()
    summary_stats['Number of Successful Trades'] = df['profit'].sum()
    summary_stats['Number of Unsuccessful Trades'] = df['loss'].sum()
    summary_stats['% Successful Trades'] = round((summary_stats['Number of Successful Trades'] / \
                                           summary_stats['Total Number of Trades']) * 100, 2)

    # calculate daily and weekly stats
    daily_summary = (df
                     .groupby('Day')
                     .agg({
                        'Profit / Loss': 'sum'
                     })
                    )
    daily_summary['profit'] = np.where(daily_summary['Profit / Loss'] > 0, 1, 0)
    summary_stats['% Profitable Days'] = round(daily_summary['profit'].mean()*100, 2)

    weekly_summary = (df
                     .groupby('Week')
                     .agg({
                        'Profit / Loss': 'sum'
                     })
                    )
    weekly_summary['profit'] = np.where(weekly_summary['Profit / Loss'] > 0, 1, 0)
    summary_stats['% Profitable Weeks'] = round(weekly_summary['profit'].mean()*100, 2)

    return summary_stats

def get_profit_loss_graph(margin_history):

    df = margin_history.copy()
    df.columns = ['Date', 'Equity']
    df['Type'] = 'Running Margin'

    # Add Starting Margin to plot
    start_margin_df = df.copy()
    start_margin_df['Equity'] = df.Equity.iloc[0]
    start_margin_df['Type'] = 'Starting Margin'

    df = df.append(start_margin_df)

    fig = px.line(
        df,
        x="Date",
        y="Equity",
        color="Type",
        title='Trading Strategy Profit / Loss Line'
    )

    return fig

def performance_segmented(closed_trades):
    '''
    Purpose: Get statistics for how the strategy performaed by instrument
    '''

    df = closed_trades.copy()
    df['Day'] = df['Trade Close Time'].apply(lambda x: x.date())
    df['Week'] = df['Day'].apply(lambda x: x.strftime('%W'))
    df['Day of Week'] = df['Day'].apply(lambda x: x.weekday())

    # translate integer day of week to actual day of week (e.g. Monday)
    df['Day of Week'] = df['Day of Week'].map({
        0: 'Monday',
        1: 'Tuesday',
        2: 'Wednesday',
        3: 'Thursday',
        4: 'Friday',
        5: 'Saturday',
        6: 'Sunday'
    })

    df['Hour of Day'] = df['Trade Close Time'].apply(lambda x: x.hour)

    df['profit'] = np.where(df['Profit / Loss'] > 0, 1, 0)
    df['loss'] = np.where(df['Profit / Loss'] < 0, 1, 0)

    cols_to_group_on = ['Instrument', 'Day', 'Week', 'Day of Week', 'Hour of Day']
    summary_dfs = {}

    for col in cols_to_group_on:
        summary_df = (df
            .groupby(col, as_index=False)
            .agg({
                'Buy or Sell?': 'count',
                'Profit / Loss': 'sum',
                'profit': 'sum',
                'loss': 'sum'
            })
        )
        summary_df.columns = [col, 'Number of Trades', 'Total Profit / Loss',
                              'Total Winning Trades', 'Total Losing Trades']

        summary_dfs[col] = summary_df

    return summary_dfs

def app():

    st.title('Automated trading strategies')

    # Select strategy
    indicators = tuple([k for k in mapper.keys()])
    strategy = st.selectbox(
    'Select strategy',indicators)

    # Select instrument
    instruments = (
    	'NAS100',
    	'US30',
    	'UK100',
    	'GER30',
    	'SPX500',
    	'ESP35',
    	'EUSTX50',
    	'JPN225',
    	'AUS200',
    	'EUR/USD',
    	'GBP/USD',
    	'EUR/GBP'
    )
    instrument = st.selectbox('Select instrument',instruments)

    # Select time granuality
    time_granularity = st.selectbox(
    	'Select intervals (minutes)',(
    	'15',
    	'5',
    	'1',))

    col1, col2 = st.beta_columns(2)

    # Select bounds
    with col1:
        rsi_buy = st.number_input('Buy at', min_value=float(mapper[strategy]['min']), max_value= float(mapper[strategy]['max']), value=float(mapper[strategy]['min']), step=0.1)

    with col2:
        rsi_sell = st.number_input('Sell at', min_value=float(mapper[strategy]['min']), max_value=float(mapper[strategy]['max']), value=float(mapper[strategy]['max']), step=0.1)

    # Select date range
    with col1:
        from_date = st.date_input('Start date', value=(datetime.today().date())-timedelta(days=21)).strftime("%Y-%m-%d")

    with col2:
        to_date = st.date_input('End date').strftime("%Y-%m-%d")

    # Build output JSON
    params = {
    	  'instrument': instrument,
    	  'time_granularity': 'm' + time_granularity,
    	  'from_date': from_date,
    	  'to_date': to_date,
    	  'strategy': [{
    	    'indicator': strategy,
    	    'buy_at': rsi_buy,
    	    'sell_at': rsi_sell
    	  }]
    	}

    st.write('The output strategy in JSON format for Backtesting ', params)

    # Save json to file
    if st.button('Run'):

        with st.spinner('Wait for it...'):
            backtest = bt.main(
        	    strat='YourStrat',
        	    output_directory='score/myapp/data',
        	    backtest_steps=False,
        	    margin=50_000,
        	    training_data=True,
        	    instructions=params
        	)

        # title of app
        st.success('Backtesting Live Results are here!')

        # get margin_history and closed_trades for selected backtest
        closed_trades = format_closed_trades(
            backtest['closed_trades'])
        margin_history = backtest['margin_history']

        # summary statistics for the entire backtest
        st.subheader('Summary Performance Statistics')

        print(closed_trades)
        print('\n\t', margin_history)
        summary_stats = summary_statistics(closed_trades, margin_history)
        for key, value in summary_stats.items():
            st.text(f"{key}: {value}")

        # profit / loss graph
        st.subheader('Profit and Loss Graph')

        fig = get_profit_loss_graph(margin_history)
        st.plotly_chart(fig)

        # performance per selected segment
        summary_dfs = performance_segmented(closed_trades)
        for key, value in summary_dfs.items():
            st.subheader(f'Performance by {key}')
            st.write(value)

        # detailed view of every single closed trade
        st.subheader('Detailed Performance')
        st.write(closed_trades)

if __name__ == '__main__':

    app()

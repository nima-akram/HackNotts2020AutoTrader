#5 minute, 15 minute, 1 hour
#Dataframe to store trades
#Dataframe to store orders
import pandas as pd
import config
import trading_strategy
import leverage
import fxcmpy
import random
import datetime
import time

class Market:

    def __init__(self, granularity, selected_instrument, backtest_steps=10_000, training_data=True, from_date=False, to_date=False):
        '''
        Purpose: collate all data from big query darwinex database
        Input:
            granularity - 'm1' | 'm5' | 'm15'
        '''
        margin = 50_000
        self.trader = Trader(margin)
        self.current_market_step = 0
        self.active_trades = pd.DataFrame(columns=["tradeId", "symbol", "usedMargin", "amount", "buy_or_sell", "askopen", "askclose", "stop", "limit", "profitloss", "time"])
        self.active_entry_orders = pd.DataFrame(columns=["orderId", "symbol", "amount", "buy", "sell", "stop", "limit", "time", "expiry"])

        trading_model = trading_strategy.CompileData(training_data=training_data)

        if training_data == False:
            con = fxcmpy.fxcmpy(config_file='fxcm.cfg')
        else:
            con = None

        self.market_five_data = trading_model.compile_trading_strategy(
            con=con,
            instruments=[i for i in config.NAIVE_LIST if i == selected_instrument],
            period=granularity,
            chart_patterns=config.CHART_PATTERNS,
            number=backtest_steps,
            from_date=from_date,
            to_date=to_date
            )

        # save granularity to self so that it can be used as the step incrementer during the loop
        self.granularity = int(granularity[1:])

        # self.market_five_data = t_model.calculate_momentum_indicators(self.market_five_data)

        if training_data == False:
            con.close()

        self.markets = {}

        self.instruments = self.market_five_data["instrument"].unique()
        self.market_five_data['date'] = pd.to_datetime(self.market_five_data['date'])

        self.start_date = min(self.market_five_data["date"])
        self.end_date = max(self.market_five_data["date"])


        for instrument in self.instruments:
            if min(self.market_five_data.loc[self.market_five_data["instrument"] == instrument, "date"]) > self.start_date:
                self.start_date = min(self.market_five_data.loc[self.market_five_data["instrument"] == instrument, "date"])
            if max(self.market_five_data.loc[self.market_five_data["instrument"] == instrument, "date"]) < self.end_date:
                self.end_date = max(self.market_five_data.loc[self.market_five_data["instrument"] == instrument, "date"])


        # mask = (self.market_five_data['date'] > self.start_date) & (self.market_five_data['date'] <= self.end_date)

        # self.market_five_data = self.market_five_data.loc[mask]

        for instrument in self.instruments:
            self.markets[instrument] = self.market_five_data.loc[self.market_five_data["instrument"] == instrument, :]
            self.markets[instrument] = self.markets[instrument].reset_index(drop=True)

        self.trade = None
        self.order = None
        self.current_time = self.start_date

        # initialising an empty closed trades dataframe that will record details of each trade made
        self.closed_trades = pd.DataFrame()

    def create_market_buy_order(self, margin, symbol, entry=False):
        df = self.get_current_market_data(symbol)

        if len(df) > 0:
            #add to active trades
            self.trade = Trade(
                symbol=symbol, amount=margin, buy_or_sell="B",
                askopen=df["askclose"].iloc[-1],
                askclose=df["askclose"].iloc[-1], time=df["date"].iloc[-1]
            )
            tradeId = random.randint(1000000, 1999999)

            used_margin = self.trader.margin * 0.001
            #["tradeId", "symbol", "usedMargin", "amount", "buy_or_sell", "askopen", "askclose", "stop", "limit", "profitloss", "time"]
            self.trade.trade_id = tradeId

            self.current_trade = self.trade.get_trade()
            trade_series = pd.Series(self.current_trade, index = self.active_trades.columns)
            self.active_trades = self.active_trades.append(trade_series, ignore_index=True)
        # else:
            # print(str(symbol) + " market closed. Could not place trade.")

    def create_market_sell_order(self, margin, symbol):
        df = self.get_current_market_data(symbol)

        if len(df) > 0:
            #add to active trades
            # print("MAKING A TRADE")
            self.trade = Trade(symbol=symbol, amount=margin, buy_or_sell="S", askopen=df["askclose"].iloc[-1],
            askclose=df["askclose"].iloc[-1], time=df["date"].iloc[-1])

            tradeId = random.randint(1000000, 1999999)

            used_margin = self.trader.margin * 0.001
            #["tradeId", "symbol", "usedMargin", "amount", "buy_or_sell", "askopen", "askclose", "stop", "limit", "profitloss", "time"]
            self.trade.trade_id = tradeId
            self.current_trade = self.trade.get_trade()
            trade_series = pd.Series(self.current_trade, index = self.active_trades.columns)
            self.active_trades = self.active_trades.append(trade_series, ignore_index=True)
        # else:
            # print(str(symbol) + " market closed. Could not place trade.")

    def create_entry_order(self, is_buy, symbol, amount, stop_rate, trade_entry, expiry, limit_rate):
        df = self.get_current_market_data(symbol)

        if len(df) > 0:
            self.order = Order(symbol=symbol, amount=amount, is_buy=is_buy, trade_entry=trade_entry, time=self.current_time, stop=stop_rate, limit=limit_rate, expiry=expiry)

            orderId = random.randint(1000000, 1999999)

            self.order.order_id = orderId
            self.current_order = self.order.get_order()
            order_series = pd.Series(self.current_order, index = self.active_entry_orders.columns)
            self.active_entry_orders = self.active_entry_orders.append(order_series, ignore_index=True)
        # else:
            # print("entry order could not be place as there is no data.")

    def change_trade_stop_limit(self, trade_id, is_stop, rate):
        #if self.trade:
        if is_stop:
            self.active_trades.loc[self.active_trades["tradeId"] == trade_id, "stop"] = rate
        else:
            self.active_trades.loc[self.active_trades["tradeId"] == trade_id, "limit"] = rate

    def calculate_profit(self, askopen, askclose, amount, signal, instrument):
        if signal == "B":
            profit = (askclose/askopen)-1
            pnl = profit * amount * leverage.dynamic[instrument]
            self.trader.margin += pnl

        elif signal == "S":
            profit = ((askclose/askopen)-1)*-1
            pnl = profit * amount * leverage.dynamic[instrument]
            self.trader.margin += pnl

        return pnl

    def get_current_market_data(self, symbol, time='m5'):
        '''
        A function that will return the current market data for the symbol passed.
        '''
        df = self.markets[symbol]

        if (time == 'm5') | (time == 'm15'):
            try:
                return df.loc[df["date"] < self.current_time, :]
            except:
                # print("No data for the market")
                return pd.DataFrame()

        elif time == 'H1':
           try:
                return df.loc[df["date"].dt.minute == 0, :]
           except:
                # print("No data for the market")
                return pd.DataFrame()

    def close_trade(self, instrument, correct, askclose):
        '''
        Purpose: To manually close a trade based on a trade call being invalidated or ending
        Input:
            instrument - the symbol being traded
            correct - whether the call was correct in hindsight
            askclose - what the closing price was so that profit/loss can be calculated
        '''

        # find the trade in the active_trades dataframe
        trade = self.active_trades[self.active_trades.symbol == instrument].reset_index(drop=True).loc[0]
        askopen = trade.askopen
        amount = trade.amount

        if askopen >= askclose:
            signal = 'B'
        else:
            signal = 'S'

        # calculate profit n loss
        if correct == 1:
            pnl = self.calculate_profit(askopen, askclose, amount, signal, instrument)
            self.active_trades = self.active_trades[self.active_trades.tradeId != trade.tradeId].reset_index(drop=True)
            trade['pnl'] = pnl
            self.closed_trades = self.closed_trades.append(trade)
        else:
            pnl = self.calculate_profit(askclose, askopen, amount, signal, instrument)
            self.active_trades = self.active_trades[self.active_trades.tradeId != trade.tradeId].reset_index(drop=True)
            trade['pnl'] = pnl
            self.closed_trades = self.closed_trades.append(trade)

    def step(self):
        '''
        Every step the program needs to be looking back at the dataframes to see what is happening with the current trades. For example it needs
        to be looking at whether it met its stop loss, take profit, expiration date etc. The trade then needs to be updates accordingly and profits
        should be calculated.
        '''
        #go through the current active trades
        for row in range(len(self.active_trades)):

            symbol = self.active_trades.loc[row, "symbol"]
            stop = self.active_trades.loc[row, "stop"]
            limit = self.active_trades.loc[row, "limit"]
            signal = self.active_trades.loc[row, "buy_or_sell"]
            trade_id = self.active_trades.loc[row, "tradeId"]
            askopen = self.active_trades.loc[row, "askopen"]
            amount = self.active_trades.loc[row, "amount"]

            #gets the current data for the market
            df = self.get_current_market_data(symbol)
            trade = self.active_trades.loc[row]

            #if the df contains something
            if len(df) > 0:

                askclose = df["askclose"].iloc[-1]
                #print("ask close: " + str(askclose))
                #the signal is to buy on the active trade
                if signal == "B":
                    # print("ask low: " + str(df["asklow"].iloc[-1]) + " and stop: " + str(stop))
                    # print("ask high: " + str(df["askhigh"].iloc[-1]) + " and limit: " + str(limit))
                    #check if the trade hit stop loss or trade limit
                    if df["asklow"].iloc[-1] < stop:
                        #if it has, calculate profit and close the trade.
                        pnl = self.calculate_profit(askopen, stop, amount, signal, symbol)
                        self.active_trades = self.active_trades.drop(row)
                        trade['pnl'] = pnl
                        self.closed_trades = self.closed_trades.append(trade)

                    elif df["askhigh"].iloc[-1] > limit:
                        #if it has, calculate profit and close the trade.
                        pnl = self.calculate_profit(askopen, limit, amount, signal, symbol)
                        self.active_trades = self.active_trades.drop(row)
                        trade['pnl'] = pnl
                        self.closed_trades = self.closed_trades.append(trade)
                elif signal == "S":
                    if df["askhigh"].iloc[-1] > stop:
                        #if it has, calculate profit and close the trade.
                        pnl = self.calculate_profit(askopen, stop, amount, signal, symbol)
                        self.active_trades = self.active_trades.drop(row)
                        trade['pnl'] = pnl
                        self.closed_trades = self.closed_trades.append(trade)
                    elif df["asklow"].iloc[-1] < limit:
                        #if it has, calculate profit and close the trade.
                        pnl = self.calculate_profit(askopen, limit, amount, signal, symbol)
                        self.active_trades = self.active_trades.drop(row)
                        trade['pnl'] = pnl
                        self.closed_trades = self.closed_trades.append(trade)

        for row in range(len(self.active_entry_orders)):
            trade_entry_buy = self.active_entry_orders.loc[row, "buy"]
            trade_entry_sell = self.active_entry_orders.loc[row, "sell"]
            expiry = self.active_entry_orders.loc[row, "expiry"]
            symbol = self.active_entry_orders.loc[row, "symbol"]
            stop = self.active_entry_orders.loc[row, "stop"]
            limit = self.active_entry_orders.loc[row, "limit"]

            df = self.get_current_market_data(symbol)

            if len(df) > 0:

                asklow = df["asklow"].iloc[-1]
                askhigh = df["askhigh"].iloc[-1]

                if trade_entry_buy:
                    if askhigh > trade_entry_buy:
                        self.create_market_buy_order(margin=self.trader.margin * 0.001, symbol=symbol, entry=True)
                        self.change_trade_stop_limit(trade_id=self.trade.trade_id, is_stop=True, rate=stop)
                        self.change_trade_stop_limit(trade_id=self.trade.trade_id, is_stop=False, rate=limit)
                        self.active_entry_orders = self.active_entry_orders.drop(row)

                elif trade_entry_sell:
                    if asklow < trade_entry_sell:
                        self.create_market_sell_order(margin=self.trader.margin * 0.001, symbol=symbol)
                        self.change_trade_stop_limit(trade_id=self.trade.trade_id, is_stop=True, rate=stop)
                        self.change_trade_stop_limit(trade_id=self.trade.trade_id, is_stop=False, rate=limit)
                        self.active_entry_orders = self.active_entry_orders.drop(row)

                if expiry < self.current_time:
                    if len(self.active_entry_orders) > 0:
                        try:
                            self.active_entry_orders = self.active_entry_orders.drop(row)
                        except:
                            print("No entry orders.")


        self.current_time = self.current_time + datetime.timedelta(minutes=self.granularity)
        self.active_trades = self.active_trades.reset_index(drop=True)
        self.active_entry_orders = self.active_entry_orders.reset_index(drop=True)

        if self.current_time == self.end_date:
            return True
        else:
            return False

class Trade:
    def __init__(self, symbol, amount, buy_or_sell, askopen, askclose, time, stop=None, limit=None):
        self.symbol = symbol
        self.amount = amount
        self.buy_or_sell = buy_or_sell
        self.askopen = askopen
        self.askclose = askclose
        self.stop = stop
        self.limit = limit
        self.profitloss = 0
        self.time = time
        self.trade_id = None

    def get_trade(self):
        return [self.trade_id, self.symbol, self.askopen * self.amount, self.amount, self.buy_or_sell, self.askopen, self.askclose, self.stop, self.limit, self.profitloss, self.time]

class Order:
    def __init__(self, symbol, amount, is_buy, trade_entry, time, expiry, stop=None, limit=None):

        self.order_id = None
        self.symbol = symbol
        self.amount = amount
        self.stop = stop
        self.limit = limit
        self.time = time
        self.expiry = expiry
        self.buy = None
        self.sell = None

        if is_buy == True:
            self.buy = trade_entry
        else:
            self.sell = trade_entry

    def get_order(self):
        #["orderId", "symbol", "amount", "buy", "sell", "stop", "limit", "time", "expiry"]
        return [self.order_id, self.symbol,self.amount, self.buy, self.sell, self.stop, self.limit, self.time, self.expiry]

class Trader:
    def __init__(self, margin):
        self.margin = margin

# market = Market()
# trader = Trader(50_000)
# done = False

# print(market.start_date)
# count = 0
# while not done:
#     if len(market.get_current_market_data('US30')) > 0:
#         if count == 0:
#             count += 1
#             market.create_entry_order(is_buy=True,symbol='US30', amount=trader.margin * 0.001, stop_rate=200,trade_entry=24000, expiry=datetime.datetime.now(), limit_rate=25000)
#             print(market.active_entry_orders)
#         if len(market.active_trades) > 0:
#             print(market.active_trades)
#             done = True

#     market.step()

# print(market.active_trades[market.active_trades["symbol"] == "UK100"])

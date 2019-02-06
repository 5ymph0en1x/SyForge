# Configuration
# pip3 install -r requirements.txt
# Execution
# python covar.py


from datetime import datetime
from dateutil import parser
from oandapyV20 import API
from statsmodels.tsa.stattools import coint, adfuller
import matplotlib.pyplot as plt
import numpy as np
import oandapyV20.endpoints.instruments as instruments
import os
import pandas as pd
import schedule
import statsmodels.api as sm
import time


# Script Config
time_units_back = 250
export_graph = False

# OANDA Config
accountID = "<Account ID>"
access_token = "<Account Token>"
api = API(access_token=access_token, environment="practice")

# 28 Pairs Analyzed
pairs = ['EURUSD', 'GBPUSD', 'USDCHF', 'EURJPY', 'GBPJPY', 'USDJPY', 'EURGBP',
         'AUDUSD', 'NZDUSD', 'USDCAD', 'EURAUD', 'GBPAUD', 'EURNZD', 'GBPNZD',
         'GBPCAD', 'GBPCHF', 'EURCAD', 'AUDCAD', 'AUDCHF', 'AUDNZD', 'AUDJPY',
         'NZDCHF', 'NZDCAD', 'NZDJPY', 'CADCHF', 'CADJPY', 'CHFJPY', 'EURCHF']

# Directories creation
graph_dir = 'graph'
if not os.path.exists(graph_dir):
    os.makedirs(graph_dir)

data_dir = 'data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


# Do Not Touch
class trade_type:
    BUY = 0
    SELL = 1


class timeframe:
    Current = 0
    M1 = 1
    M5 = 5
    M15 = 15
    M30 = 30
    H1 = 60
    H4 = 240
    Daily = 1440
    Weekly = 10080
    Monthly = 43200


class ohlc:
    open = 0.0
    high = 0.0
    low = 0.0
    close = 0.0
    symbol = ''
    timestamp = None


class Spread:
    i1 = None
    i2 = None
    x1 = None
    x2 = None
    Z = None
    b = None
    stationary = None
    coi_pvalue = None
    stn_pvalue = None
    x1_signal = None
    x2_signal = None
    trade_signal = False
    x1_symbol = None
    x2_symbol = None


def get_data(symbols, total_candle):

    df = []
    iter = -1

    for i in range(len(symbols)):
        df.append(pd.DataFrame(columns=['datetime', 'symbol', 'open', 'high', 'low', 'close']))

    for i in symbols:
        candles_call = instruments.InstrumentsCandles(instrument=i,
                                                      params={"count": total_candle,
                                                              "granularity": "M1"})
        candles_data = api.request(candles_call)
        reg = 0
        iter += 1
        for j in range(0, total_candle, 1):
            candle_time = datetime.strptime(str(parser.parse(candles_data['candles'][j]['time'][:19])),
                                            '%Y-%m-%d %H:%M:%S')
            candle_sym_raw = candles_data['instrument']
            candle_sym = candle_sym_raw[:3] + candle_sym_raw[4:]
            candle_open = candles_data['candles'][j]['mid']['o']
            candle_high = candles_data['candles'][j]['mid']['h']
            candle_low = candles_data['candles'][j]['mid']['l']
            candle_close = candles_data['candles'][j]['mid']['c']
            data = [candle_time, candle_sym, candle_open, candle_high, candle_low, candle_close]
            df[iter].loc[reg] = data
            reg += 1

    return df


def check_for_stationarity(X, cutoff=0.05):
    pvalue = adfuller(X)[1]
    if pvalue < cutoff:
        #        print ('p-value = ' + str(pvalue) + ' The series ' + X.name +' is likely stationary.')
        return True, pvalue
    else:
        #        print ('p-value = ' + str(pvalue) + ' The series ' + X.name +' is likely non-stationary.')
        return False, pvalue


def find_cointegrated_pairs(df):
    n = df.shape[1]
    score_matrix = np.zeros((n, n))
    pvalue_matrix = np.ones((n, n))
    keys = df.keys()
    pairs_temp = []
    for i in range(n):
        for j in range(i + 1, n):
            S1 = df[keys[i]]
            S2 = df[keys[j]]
            result = coint(S1, S2)
            score = result[0]
            pvalue = result[1]
            score_matrix[i, j] = score
            pvalue_matrix[i, j] = pvalue
            if pvalue < 0.05:
                pairs_temp.append((keys[i], keys[j]))
    return score_matrix, pvalue_matrix, pairs_temp


def get_Spread(index, filtTime_df):
    spread = Spread()

    # verify the cointegrated pairs
    X1 = pd.Series(filtTime_df.iloc[:, index[0]])
    X2 = pd.Series(filtTime_df.iloc[:, index[1]])
    X1.name = pairs[index[0]]
    X2.name = pairs[index[1]]

    # reindex X1 and X2
    x1 = X1.reset_index()
    x1 = x1.drop(labels='index', axis=1)
    x2 = X2.reset_index()
    x2 = x2.drop(labels='index', axis=1)

    # ************************ Calculate Beta and Spread ***************************

    # compute Beta
    x1 = sm.add_constant(x1)
    results = sm.OLS(x2, x1).fit()

    # remove constant column
    x1 = x1[pairs[index[0]]]
    x2 = x2[pairs[index[1]]]

    # results.params

    b = results.params[pairs[index[0]]]
    Z = x2 - b * x1
    Z.name = 'Spread'

    spread.i1 = index[0]
    spread.i2 = index[1]
    spread.x1 = x1
    spread.x2 = x2
    spread.b = b
    spread.Z = Z

    return spread


'''
*******************************************************************************
This function is to filter the dataframe based on the start and end datetime
input parameter :
    start date : YYYY-MM-DD
    end date : YYY-MM-DD
    time: class session
    dataframe
*******************************************************************************
'''


def Filter_datetime(start_day, end_day, time, pre_df):
    # convert column to datetime format
    pre_df['dt_MY'] = pd.to_datetime(pre_df.dt_MY)

    start_date = datetime.strptime(start_day, '%Y-%m-%d')
    end_date = datetime.strptime(end_day, '%Y-%m-%d')

    start = time[0]
    end = time[1]
    start_time = datetime.strptime(start, '%H:%M:%S')
    end_time = datetime.strptime(end, '%H:%M:%S')

    # filter dataframe based on start and end datetime
    df = pre_df.loc[(pre_df.dt_MY.dt.date >= start_date.date()) \
                    & (pre_df.dt_MY.dt.date <= end_date.date()) \
                    & (pre_df.dt_MY.dt.time >= start_time.time()) \
                    & (pre_df.dt_MY.dt.time < end_time.time())]

    return df


'''
*******************************************************************************
This function is to compile close price and datetime from a list of dataframe
to a single dataframe.
input parameter :
    dataframe
*******************************************************************************
'''


def Data_Cleaning(price_df):
    close_df = pd.DataFrame()

    for df in price_df:

        if close_df.empty:
            temp_df = pd.DataFrame(df[['datetime', 'dt_MY', 'close']])

        else:
            temp_df = pd.DataFrame(df.close)

        temp_df.rename(columns={'close': df.iloc[0].symbol[:6]}, inplace=True)
        close_df = close_df.join(temp_df, how='outer')

    return close_df


'''
*******************************************************************************
This function is to find pair of currency that having cointegration and 
calculate the spread between them.
input parameter :
    dataframe
    list of symbols
*******************************************************************************
'''


def Prepare_Data(df_full, symb):
    df = df_full[symb]
    scores, pvalues, pairs_temp = find_cointegrated_pairs(df)
    # find pair index
    indexs = []
    for pair in pairs_temp:
        sub_index = []
        # print('Pair: ', pair)
        for symbol in pair:
            # print('Symbol: ', symbol)
            sub_index.append(symb.index(symbol))

        indexs.append(sub_index)

    print('Pair(s) having cointegration are ...')
    for i in range(len(pairs_temp)):
        print('Pair:', pairs_temp[i], ' Index:', indexs[i])

        # get spread
    datas = []
    for index in indexs:
        #        spread = get_Spread(index[0], index[1], filtTime_df)
        spread = get_Spread(index, df)
        datas.append(spread)

    return datas


'''
*******************************************************************************
This function is to measure the cointegration and check the stationary for the 
spread.
input parameter :
    dataframe
    list of symbols
*******************************************************************************
'''


def Analyze_Data(data, symbols):
    # check for cointegration
    score, pvalue, _ = coint(data.x1, data.x2)

    # test for stationary
    stationary, station_pvalue = check_for_stationarity(data.Z)

    data.stationary = stationary
    data.coi_pvalue = pvalue
    data.stn_pvalue = station_pvalue

    num = '{:2.3f}'

    if pvalue < 0.05 and stationary and data.b > 0 and data.b < 3:
        #    if pvalue < 0.05 and stationary and data.b > 0 and data.b < 3:
        #    if pvalue < 0.05 and data.b > 0 and stationary:

        data.trade_signal = True
        data.x1_symbol = symbols[data.i1]
        data.x2_symbol = symbols[data.i2]

        if zscore(data.Z).iloc[-1] > 0:
            data.x1_signal = trade_type.BUY
            data.x2_signal = trade_type.SELL
        elif zscore(data.Z).iloc[-1] < 0:
            data.x1_signal = trade_type.SELL
            data.x2_signal = trade_type.BUY

        text1 = 'Cointegration between ' + data.x1_symbol + ' and ' + data.x2_symbol + ' with p-value =' + num.format(
            data.coi_pvalue)
        text2 = 'Beta (b) is ' + num.format(data.b)
        text3 = 'Spread is stationary with pvalue ' + num.format(data.stn_pvalue)
        text4 = 'spread max = ' + num.format(zscore(data.Z).max())
        text5 = 'spread min = ' + num.format(zscore(data.Z).min())
        text6 = 'current spread value =' + num.format(zscore(data.Z).iloc[-1])
        print(text1, '\n', text2, '\n', text3, '\n', text4, '\n', text5, '\n', text6)

    return data


'''
*******************************************************************************
This function is to plot Z-Score graph
input parameter :
    dataframe
*******************************************************************************
'''


def ZPlot_Graph(data):
    # plot the z-scores
    zscore(data.Z).plot()
    plt.axhline(zscore(data.Z).mean(), color='black')
    plt.axhline(1.0, color='red', linestyle='--')
    plt.axhline(2.0, color='red', linestyle='--')
    plt.axhline(-1.0, color='green', linestyle='--')
    plt.axhline(-2.0, color='green', linestyle='--')
    plt.legend(['Spread z-score', 'Mean', '+1', '-1'])
    plt.title(' between pairs ' + pairs[data.i1] + ' and ' + pairs[data.i2])
    imageFile = pairs[data.i1] + ' - ' + pairs[data.i2]
    plt.savefig(graph_dir + '/' + imageFile + '_Z.png')
    plt.clf()


def SpreadPlot_Graph(data):
    # plot the spread
    data.Z.plot()
    plt.axhline(data.Z.mean(), color='black')
    plt.title(' between pairs ' + pairs[data.i1] + ' and ' + pairs[data.i2])
    imageFile = pairs[data.i1] + ' - ' + pairs[data.i2]
    plt.savefig(graph_dir + '/' + imageFile + '_Spread.png')
    plt.clf()


def zscore(series):
    return (series - series.mean()) / np.std(series)


'''
*******************************************************************************
                        MAIN PROGRAM
*******************************************************************************
'''


def fire():
    # initialization ***************************************************************
    print('Starts Compacting Data')
    total_c = time_units_back

    symbols = []
    for pair in pairs:
        symbol = pair[:3] + '_' + pair[3:]
        symbols.append(symbol)

    # logic begin here ************************************************************

    dfs = get_data(symbols, total_c)

    for df in dfs:
        df['dt_MY'] = df.datetime.dt.tz_localize('UTC').dt.tz_convert('Europe/Paris')

    closed_df = Data_Cleaning(dfs)
    closed_df.to_csv(data_dir + '/' + 'forex_data.csv', index=False)

    print('Starts Computing CoVariance')

    # Read data from csv file
    df = pd.read_csv(data_dir + '/' + 'forex_data.csv')

    datas = Prepare_Data(df, pairs)

    directions = []
    direction_final = []

    for data in datas:
        data = Analyze_Data(data, symbols)

        if data.coi_pvalue < 0.05 and data.b > 0 and data.stationary:
            if export_graph is True:
                ZPlot_Graph(data)
                SpreadPlot_Graph(data)
            if data.x1_signal == trade_type.BUY:
                directions.append([data.x1_symbol, 1])
                directions.append([data.x2_symbol, -1])
            elif data.x1_signal == trade_type.SELL:
                directions.append([data.x1_symbol, -1])
                directions.append([data.x2_symbol, 1])

    for pair in pairs:
        direction = 0
        sym_temp = pair[:3] + '_' + pair[3:]
        sym = pair
        for i in range(len(directions)):
            if sym_temp == directions[i][0]:
                direction += directions[i][1]
        direction_final.append([sym, direction])

    with open(data_dir + '/' + 'direction.txt', 'w+') as filehandle:
        filehandle.writelines("%s\n" % direction for direction in direction_final)
    filehandle.close()


def main():
    schedule.every().hour.at(":12").do(fire)
    schedule.every().hour.at(":27").do(fire)
    schedule.every().hour.at(":42").do(fire)
    schedule.every().hour.at(":57").do(fire)

    # fire()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print('  Cancelling Schedule')
        pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("  ----- My only friend, the end ! -----")
        pass

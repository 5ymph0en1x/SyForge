# Configuration
# pip3 install oandapyV20
# pip3 install pandas
# pip3 install python-dateutil
# pip3 install requests
# pip3 install simplejson
# Execution
# python SyForge.py

import covar
from datetime import *
import json
import requests
from datetime import timezone
from dateutil import parser
from multiprocessing import Process
import oandapyV20
from oandapyV20 import API
from oandapyV20.contrib.requests import MarketOrderRequest
from oandapyV20.contrib.requests import TakeProfitDetails, StopLossDetails
from oandapyV20.contrib.requests import TrailingStopLossOrderRequest
import oandapyV20.endpoints.orders as orders
from oandapyV20.endpoints.pricing import PricingInfo
from oandapyV20.endpoints.pricing import PricingStream
import oandapyV20.endpoints.positions as positions
import oandapyV20.endpoints.trades as trades
from oandapyV20.exceptions import V20Error
import os
import pandas as pd


# Bot Parameters
sl_tp_prc = 0.001
Trailing = False
trail_point = 5.0

# OANDA Config
accountID = "<Account ID>"
access_token = "<Account Token>"

# 1Forge Config
forge_key = "<1Forge Key>"

# Do Not Touch
list_pairs = ['EURUSD', 'GBPUSD', 'USDCHF', 'EURJPY', 'GBPJPY', 'USDJPY', 'EURGBP',
              'AUDUSD', 'NZDUSD', 'USDCAD', 'EURAUD', 'GBPAUD', 'EURNZD', 'GBPNZD',
              'GBPCAD', 'GBPCHF', 'EURCAD', 'AUDCAD', 'AUDCHF', 'AUDNZD', 'AUDJPY',
              'NZDCHF', 'NZDCAD', 'NZDJPY', 'CADCHF', 'CADJPY', 'CHFJPY', 'EURCHF']

data_dir = 'data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

api = API(access_token=access_token, environment="practice")
# stream = PricingStream(accountID=accountID, params={"instruments": pairs_traded})
trades_list = trades.TradesList(accountID)

def count_spe_trades(symbol):
    rv = api.request(trades_list)
    trades_details = rv['trades']
    j = 0
    for i in trades_details:
        if i['instrument'] == symbol:
            if float(i['initialUnits']) > 0:
                j += 1
            elif float(i['initialUnits']) < 0:
                j -= 1
    return j


def count_spe_profit(symbol):
    rv = api.request(trades_list)
    trades_details = rv['trades']
    j = 0
    for i in trades_details:
        if i['instrument'] == symbol:
            j += float(i['unrealizedPL'])
    return j


def getForgePrice():
    strPair = ','.join(list_pairs)
    r = requests.get("https://forex.1forge.com/1.0.3/quotes?pairs=" + strPair + "&api_key=" + forge_key)
    data = r.content.decode("utf-8")
    dataJSON = json.loads(data)
    prices = dataJSON
    price_list = []
    for i in prices:
        symbol = i.get('symbol')
        qt_ok = i.get('timestamp')
        ask = i.get('ask')
        bid = i.get('bid')
        info = 'time=' + str(qt_ok) + ' symbol=' + symbol + ' ask=' + str(ask) + ' bid=' + str(bid)
        info_list = dict(i.split('=') for i in info.split())
        price_list.append(info_list)
    return price_list

def getOandaInstru(list_pairs):
    # strPair = ','.join(list_pairs)
    list_pairs_formated = []
    for i in list_pairs:
        temp = i[:3] + '_' + i[3:]
        list_pairs_formated.append(temp)
    strPair = ','.join(list_pairs_formated)
    return strPair

def getOandaStream():
    pairs_traded = getOandaInstru(list_pairs)
    stream = PricingStream(accountID=accountID, params={"instruments": pairs_traded})
    return stream

def getOandaPrice():
    i = getOandaInstru(list_pairs)
    info = PricingInfo(accountID=accountID, params={"instruments": i})
    r = api.request(info)
    # time = r["time"]
    prices = r["prices"]
    price_list = []
    for j in prices:
        symbol_raw = j['instrument']
        symbol = symbol_raw[:3] + symbol_raw[4:]
        quote_time = j['time']
        qt = parser.parse(quote_time)
        qt_ok = qt.replace(tzinfo=timezone.utc).timestamp()
        ask = j['asks'][0]['price']
        bid = j['bids'][0]['price']
        info = 'time=' + str(qt_ok) + ' symbol=' + symbol + ' ask=' + str(ask) + ' bid=' + str(bid)
        info_list = dict(i.split('=') for i in info.split())
        price_list.append(info_list)
    return price_list


def vectors(prices_t1, prices_t2):
    vectors_list = []
    for i in range(0, 27):
        if prices_t1[i].get('symbol') == prices_t2[i].get('symbol'):
            symbol = prices_t2[i].get('symbol')
            time = prices_t2[i].get('time')
            ask_direction = 0
            bid_direction = 0
            direction = 0
            # if float(prices_t2[i].get('ask')) > float(prices_t1[i].get('ask')):
            #     ask_direction = 1
            # elif float(prices_t2[i].get('ask')) < float(prices_t1[i].get('ask')):
            #     ask_direction = -1
            if float(prices_t2[i].get('bid')) > float(prices_t1[i].get('bid')):
                direction = (float(prices_t2[i].get('bid')) - float(prices_t1[i].get('bid'))) / float(prices_t1[i].get('bid'))
            elif float(prices_t2[i].get('bid')) < float(prices_t1[i].get('bid')):
                direction = (float(prices_t2[i].get('bid')) - float(prices_t1[i].get('bid'))) / float(prices_t1[i].get('bid'))
            # if ask_direction == bid_direction:
            #     direction = ask_direction
            ask = prices_t2[i].get('ask')
            bid = prices_t2[i].get('bid')
            info = 'time=' + str(time) + ' symbol=' + str(symbol) + ' ask=' + str(ask) + ' bid=' + str(bid) \
                + ' direction=' + str(direction)
            if direction != 0:
                print(info)
            info_list = dict(i.split('=') for i in info.split())
            vectors_list.append(info_list)
    # print(vectors_list)
    return vectors_list


def compare_vectors(oanda_vectors, forge_vectors):
    dir_raw = []
    with open(data_dir + '/' + 'direction.txt', 'r') as filehandle:
        filecontents = filehandle.readlines()

        for line in filecontents:
            # remove linebreak which is the last character of the string
            current_place = line[:-1]

            # add item to the list
            dir_raw.append(current_place)
    i = 0
    direction = []
    for pair in range(len(list_pairs)):
        direction.append(pd.DataFrame(columns=['data']))
    for dir_filt in dir_raw:
        dir_split = dir_filt.split(",")
        direction[i].loc[0] = dir_split[0]
        direction[i].loc[1] = dir_split[1]
        i += 1
    # print(str(direction[7].get('data')[0].split('[')[1]))

    for i in range(0, 27):
        if oanda_vectors[i].get('symbol') == forge_vectors[i].get('symbol')\
                and oanda_vectors[i].get('symbol') == str(direction[i].get('data')[0].split('[')[1].split("'")[1]):
            # print(oanda_vectors[i].get('symbol'))
            if float(oanda_vectors[i].get('direction')) > 0 and float(forge_vectors[i].get('direction')) < 0 \
                    and float(direction[i].get('data')[1].split(']')[0]) < 0:
                instru_raw = oanda_vectors[i].get('symbol')
                instru = instru_raw[:3] + '_' + instru_raw[3:]
                args = [instru, -1]
                orderlaunch(args)
                txt_msg = "SELL: " + oanda_vectors[i].get('symbol')
                tb.send_message(chatid, txt_msg)
                print("SELL: ", oanda_vectors[i].get('symbol'))
            elif float(oanda_vectors[i].get('direction')) < 0 and float(forge_vectors[i].get('direction')) > 0 \
                    and float(direction[i].get('data')[1].split(']')[0]) > 0:
                instru_raw = oanda_vectors[i].get('symbol')
                instru = instru_raw[:3] + '_' + instru_raw[3:]
                args = [instru, 1]
                orderlaunch(args)
                txt_msg = "BUY: " + oanda_vectors[i].get('symbol')
                tb.send_message(chatid, txt_msg)
                print("BUY: ", oanda_vectors[i].get('symbol'))

    filehandle.close()


def close(pair_to_close):
    print("Close existing position...")
    r = positions.PositionDetails(accountID=accountID,
                                  instrument=pair_to_close)

    try:
        openPos = api.request(r)

    except V20Error as e:
        print("V20Error: {}".format(e))

    else:
        toClose = {}
        for P in ["long", "short"]:
            if openPos["position"][P]["units"] != "0":
                toClose.update({"{}Units".format(P): "ALL"})

        # print("prepare to close: %s", json.dumps(toClose))
        r = positions.PositionClose(accountID=accountID,
                                    instrument=pair_to_close,
                                    data=toClose)
        rv = None
        try:
            if toClose:
                rv = api.request(r)
                print("close: response: %s", json.dumps(rv, indent=2))

        except V20Error as e:
            print("V20Error: {}".format(e))


def orderlaunch(args):

    pair_targeted, direction = args

    info = PricingInfo(accountID=accountID, params={"instruments": pair_targeted})
    mkt_order = None

    if direction is 0:
        return False

    elif direction is 1:
        raw_current_price = api.request(info)
        bid_current = float(raw_current_price['prices'][0]['bids'][0]['price'])
        decim = str(bid_current)[::-1].find('.')
        if decim < 4:
            decim = 3
        if decim >= 4:
            decim = 5
        stop_loss = round(bid_current - bid_current * sl_tp_prc, decim)
        take_profit = round(bid_current + bid_current * sl_tp_prc, decim)
        if float(count_spe_trades(pair_targeted)) < 0:
            close(pair_targeted)
        mkt_order = MarketOrderRequest(
            instrument=pair_targeted,
            units=1000,
            takeProfitOnFill=TakeProfitDetails(price=take_profit).data,
            stopLossOnFill=StopLossDetails(price=stop_loss).data)

    elif direction is -1:
        raw_current_price = api.request(info)
        ask_current = float(raw_current_price['prices'][0]['asks'][0]['price'])
        decim = str(ask_current)[::-1].find('.')
        if decim < 4:
            decim = 3
        if decim >= 4:
            decim = 5
        stop_loss = round(ask_current + ask_current * sl_tp_prc, decim)
        take_profit = round(ask_current - ask_current * sl_tp_prc, decim)
        if float(count_spe_trades(pair_targeted)) > 0:
            close(pair_targeted)
        mkt_order = MarketOrderRequest(
            instrument=pair_targeted,
            units=-1000,
            takeProfitOnFill=TakeProfitDetails(price=take_profit).data,
            stopLossOnFill=StopLossDetails(price=stop_loss).data)

    # create the OrderCreate request
    r = orders.OrderCreate(accountID, data=mkt_order.data)

    try:
        # create the OrderCreate request
        rv = api.request(r)
        print(json.dumps(rv, indent=2))
    except oandapyV20.exceptions.V20Error as err:
        print(r.status_code, err)
        return False
    else:
        try:
            if Trailing is True:
                key = 'orderFillTransaction'
                if key in rv:
                    trade_id = rv['orderFillTransaction']['tradeOpened']['tradeID']
                    bid_current = float(rv['orderFillTransaction']['fullPrice']['bids'][0]['price'])
                    decim = str(bid_current)[::-1].find('.')
                    trail_point_ok = 0
                    if decim < 4:
                        trail_point_ok = trail_point * (1 / (10 ** (3 - 1)))
                    if decim >= 4:
                        trail_point_ok = trail_point * (1 / (10 ** (5 - 1)))
                    ordr = TrailingStopLossOrderRequest(tradeID=trade_id, distance=trail_point_ok)
                    r = orders.OrderCreate(accountID, data=ordr.data)
                    rva = api.request(r)
                    print(json.dumps(rva, indent=2))
        except oandapyV20.exceptions.V20Error as err:
            print(r.status_code, err)
            return False
        else:
            return True


def main():

    print('Starts Scanning...')

    minute_cached = 0
    ticker = 0
    oanda_prices_t1 = []
    oanda_prices_t2 = []
    forge_prices_t1 = []
    forge_prices_t2 = []
    stream = getOandaStream()

    try:
        R = api.request(stream)
        for i in R:
            if minute_cached is not datetime.now().time().minute:
                key = 'type'
                if key in i:
                    if i['type'] == 'PRICE':
                        oanda_prices_t1 = oanda_prices_t2
                        forge_prices_t1 = forge_prices_t2
                        oanda_prices_t2 = getOandaPrice()
                        forge_prices_t2 = getForgePrice()
                        # print(oanda_prices_t2[1].get('time'))
                        ticker += 1
                        if ticker >= 2:
                            # direction = open(data_dir + '/' + 'direction.txt', 'r')
                            oanda_vectors = vectors(oanda_prices_t1, oanda_prices_t2)
                            forge_vectors = vectors(forge_prices_t1, forge_prices_t2)
                            compare_vectors(oanda_vectors, forge_vectors)
                            ticker = 0
                            for j in list_pairs:
                                instru = j[:3] + '_' + j[3:]
                                profit = count_spe_profit(instru)
                                if float(profit) > 0:
                                    close(instru)
                            minute_cached = datetime.now().time().minute
                            print("Minute Data Updated")

    except V20Error as e:
        print("Error: {}".format(e))


if __name__ == '__main__':
    try:
        # main()
        # '''
        p1 = Process(target=main)
        p1.start()
        p2 = Process(target=covar.main)
        p2.start()
        p1.join()
        p2.join()
        # '''
    except KeyboardInterrupt:
        print("  ----- This is the end ! -----")
        pass

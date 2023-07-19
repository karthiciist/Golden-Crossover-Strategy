import requests
import pandas as pd
import time
import datetime
import numpy as np
from ta.trend import ADXIndicator
import yfinance as yf
from fyers_api import accessToken, fyersModel
from stockstats import StockDataFrame
import math
import pyodbc
from flask import Flask, render_template, request, redirect, send_file, url_for
from flask import Flask
from flask import request
import webbrowser
import http.client
import configparser

app = Flask(__name__)

redirect_url = "http://127.0.0.1:8098/process_authcode_from_fyers"
response_t = "code"
state = "sample_state"

config_obj = configparser.ConfigParser()
config_obj.read(".\configfile.ini")
dbparam = config_obj["mssql"]

server = dbparam["Server"]
db = dbparam["db"]


# To get client_id and client_secret from user and pass to fyers api
@app.route("/getauthcode", methods=['POST'])
def getauthcode():
    global client_id
    client_id = request.form.get('client_id')
    global client_secret
    client_secret = request.form.get('client_secret')
    session = accessToken.SessionModel(
        client_id=client_id,
        secret_key=client_secret,
        redirect_uri=redirect_url,
        response_type=response_t
    )

    response = session.generate_authcode()
    webbrowser.open(response)
    return response


# Fyres api will call back this methid with auth code. This method will use that auth code to generate access token
@app.route("/process_authcode_from_fyers")
def process_authcode_from_fyers():
    try:
        authcode = request.args.get('auth_code')
        session = accessToken.SessionModel(
            client_id=client_id,
            secret_key=client_secret,
            redirect_uri=redirect_url,
            response_type=response_t,
            grant_type="authorization_code"
        )
        session.set_token(authcode)
        response = session.generate_token()
        global access_token
        access_token = response["access_token"]
        print("access token ", access_token)
        global refresh_token
        refresh_token = response["refresh_token"]
        return render_template('authorized.html')
    except Exception as e:
        return {"status": "Failed", "data": str(e)}


def get_history(symbol):
    clientid = "RVLYRDO3H5-100"
    # access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE2ODk0OTY5OTMsImV4cCI6MTY4OTU1MzgxMywibmJmIjoxNjg5NDk2OTkzLCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCa3M2Mmh5T18zNUdoREpjd1BWcWJKSHlkSnhqeGVseUpOUkJ6ekRmamtzX3A5NTF0MlVpWlRndl9meWdKNG16am9wOEtIcUhOWnM3eWpUa3k5QlRVbl9jXzlFOXV0MFJGd1Y0TGJib1liRzN0N1hkQT0iLCJkaXNwbGF5X25hbWUiOiJERUVOQURIQVlBTEFOIEtBUlRISSBTUklOSVZBU0FOIiwib21zIjoiSzEiLCJmeV9pZCI6IlhEMjA3ODkiLCJhcHBUeXBlIjoxMDAsInBvYV9mbGFnIjoiTiJ9.QHbYSuZiIm4WeXGMAJC-ExJX04lXi9tAwlkDKZtd2h0"

    fyers = fyersModel.FyersModel(client_id=clientid, token=access_token)
    today = time.strftime("%Y-%m-%d")
    data = {
        "symbol": symbol,
        "resolution": "3",
        "date_format": "1",
        "range_from": "2023-07-05",
        "range_to": today,
        "cont_flag": "1"
    }

    response = fyers.history(data=data)
    return response


def calculate_vwap(symbol):
    history = get_history(symbol)
    df = pd.DataFrame(history['candles'], columns = ['epoch', 'open', 'high', 'low', 'close', 'volume'])
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    volume = df['volume']
    cumulative_volume = volume.cumsum()
    cumulative_typical_price = (typical_price * volume).cumsum()
    vwap = cumulative_typical_price / cumulative_volume
    last_element = vwap[vwap.size-1]
    return last_element


def calculate_rsi(symbol, period=14):
    history = get_history(symbol)
    df = pd.DataFrame(history['candles'], columns = ['epoch', 'open', 'high', 'low', 'close', 'volume'])
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    last_element = rsi[rsi.size-1]
    return last_element


def calculate_vwma(symbol):
    history = get_history(symbol)
    df = pd.DataFrame(history['candles'], columns = ['epoch', 'open', 'high', 'low', 'close', 'volume'])
    volume = df['volume']
    close = df['close']
    length = 20
    vwma = np.sum(close[-length:] * volume[-length:]) / np.sum(volume[-length:])
    return vwma


def calculate_supertrend(symbol, period=10, multiplier=3):
    # Download historical price data
    history = get_history(symbol)
    df = pd.DataFrame(history['candles'], columns = ['epoch', 'open', 'high', 'low', 'close', 'volume'])

    # Calculate the average true range (ATR)
    high = df['high']
    low = df['low']
    close = df['close']
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()

    # Calculate the basic upper and lower bands
    basic_upper_band = (high + low) / 2 + multiplier * atr
    basic_lower_band = (high + low) / 2 - multiplier * atr

    # Calculate the final upper and lower bands (considering previous period)
    final_upper_band = basic_upper_band.copy()
    final_lower_band = basic_lower_band.copy()
    for i in range(period, len(df)):
        if close[i - 1] > final_upper_band[i - 1]:
            final_upper_band[i] = basic_upper_band[i]
            final_lower_band[i] = basic_lower_band[i]
        elif close[i - 1] < final_lower_band[i - 1]:
            final_upper_band[i] = basic_upper_band[i]
            final_lower_band[i] = basic_lower_band[i]
        else:
            final_upper_band[i] = final_upper_band[i - 1]
            final_lower_band[i] = final_lower_band[i - 1]

    # Calculate the SuperTrend indicator
    supertrend = (final_upper_band + final_lower_band) / 2
    last_element = supertrend[supertrend.size-1]
    return last_element


def get_volume(symbol, number):
    history = get_history(symbol)
    df = pd.DataFrame(history['candles'], columns = ['epoch', 'open', 'high', 'low', 'close', 'volume'])
    volume = df['volume']
    last_element = volume[volume.size-number]
    return last_element


def get_current_price(symbol):
    history = get_history(symbol)
    df = pd.DataFrame(history['candles'], columns=['epoch', 'open', 'high', 'low', 'close', 'volume'])
    nifty_stockstats_df = StockDataFrame(df)
    nifty_stockstats_df[['epoch', 'close_20_sma', 'close_7_smma', 'close_9_ema']]
    nifty_stockstats_df = nifty_stockstats_df.round()
    current_price = nifty_stockstats_df['close'].iloc[-1]
    return current_price


def get_option_chain_dataframe(symbol):
    url = 'https://www.nseindia.com/api/option-chain-indices?symbol=' + symbol

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36 Edg/103.0.1264.37',
        'accept-encoding': 'gzip, deflate, br', 'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8'}

    session = requests.Session()
    request = session.get(url, headers=headers)
    cookies = dict(request.cookies)

    response = session.get(url, headers=headers, cookies=cookies).json()
    rawdata = pd.DataFrame(response)
    rawop = pd.DataFrame(rawdata['filtered']['data']).fillna(0)
    data = []
    for i in range(0, len(rawop)):
        calloi = callcoi = cltp = putoi = putcoi = pltp = 0
        stp = rawop['strikePrice'][i]
        if (rawop['CE'][i] == 0):
            calloi = callcoi = 0
        else:
            calloi = rawop['CE'][i]['openInterest']
            callcoi = rawop['CE'][i]['changeinOpenInterest']
            cltp = rawop['CE'][i]['lastPrice']
        if (rawop['PE'][i] == 0):
            putoi = putcoi = 0
        else:
            putoi = rawop['PE'][i]['openInterest']
            putcoi = rawop['PE'][i]['changeinOpenInterest']
            pltp = rawop['PE'][i]['lastPrice']
        opdata = {
            #             'CALL OI': calloi, 'CALL CHNG OI': callcoi, 'CALL LTP': cltp, 'STRIKE PRICE': stp,
            #             'PUT OI': putoi, 'PUT CHNG OI': putcoi, 'PUT LTP': pltp
            'CALL LTP': cltp, 'STRIKE PRICE': stp, 'PUT LTP': pltp
        }

        data.append(opdata)
    optionchain = pd.DataFrame(data)
    return optionchain


def time_in_range(start, end, current):
    """Returns whether current is in the range [start, end]"""
    return start <= current <= end


def is_it_trade_time():
    start_first_time_window = datetime.time(9, 45, 0)
    end_first_time_window = datetime.time(12, 30, 0)
    current_first_time_window = datetime.datetime.now().time()
    first_time_window = time_in_range(start_first_time_window, end_first_time_window, current_first_time_window)
    if first_time_window == False:
        start_second_time_window = datetime.time(12, 30, 0)
        end_second_time_window = datetime.time(16, 00, 0)
        current_second_time_window = datetime.datetime.now().time()
        second_time_window = time_in_range(start_second_time_window, end_second_time_window, current_second_time_window)

    if first_time_window:
#         print ("Time is morning trade time")
        return True
    elif second_time_window:
#         print ("Time is noon trade time")
        return True
    else:
#         print ("Not a trade time")
        return False


def update_db(dict_to_db):
    table = dbparam["golden_crossover_log_table"]

    symbol = str(dict_to_db.get('symbol'))
    timestamp = str(dict_to_db.get('timestamp'))
    current_price = str(dict_to_db.get('current_price'))
    vwap = str(dict_to_db.get('vwap'))
    rsi = str(dict_to_db.get('rsi'))
    volume1 = str(dict_to_db.get('volume1'))
    volume2 = str(dict_to_db.get('volume2'))
    st = str(dict_to_db.get('st'))
    vwma = str(dict_to_db.get('vwma'))
    call_strikeprice = str(dict_to_db.get('call_strikeprice'))
    put_strikeprice = str(dict_to_db.get('put_strikeprice'))
    is_trade_time = str(dict_to_db.get('is_trade_time'))
    buy_signal = str(dict_to_db.get('buy_signal'))

    # conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
    #                       r'Server=localhost\MSSQLSERVER01;'
    #                       'Database=algotrade;'
    #                       'Trusted_Connection=yes;')  # integrated security

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cursor = conn.cursor()

    SQLCommand = (
        "INSERT INTO " + table + " (symbol, timestamp, current_price, vwap, rsi, volume1, volume2, st, vwma, call_strikeprice, put_strikeprice, is_trade_time, buy_signal) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);")
    Values = [symbol, timestamp, current_price, vwap, rsi, volume1, volume2,
              st, vwma, call_strikeprice, put_strikeprice, is_trade_time, buy_signal]
    print(SQLCommand)
    # Processing Query
    cursor.execute(SQLCommand, Values)

    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


@app.route("/run_golden_crossover_strategy", methods=['POST'])
def run_golden_crossover_strategy():
    while (True):
        try:
            time.sleep(60)
            to_db_dict = {}
            # Check if it is trade time
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            to_db_dict["timestamp"] = now

            # symbol = "NIFTY"
            to_db_dict["symbol"] = "NIFTY"

            if is_it_trade_time():
                print("Its trade time")
                to_db_dict["is_trade_time"] = "Y"
                # to_db_dict = {}

                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                to_db_dict["timestamp"] = now

                current_month = datetime.datetime.now().strftime("%b").upper()
                current_year = datetime.datetime.now().year
                current_year = int(str(current_year)[-2:])

                symbol = "NSE:NIFTY" + str(current_year) + str(current_month) + "FUT"
                to_db_dict["symbol"] = symbol

                history = get_history(symbol)

                df = pd.DataFrame(history['candles'], columns=['epoch', 'open', 'high', 'low', 'close', 'volume'])
                nifty_stockstats_df = StockDataFrame(df)

                nifty_stockstats_df[['epoch', 'close_20_sma', 'close_7_smma', 'close_9_ema']]
                nifty_stockstats_df = nifty_stockstats_df.round()

                current_price = nifty_stockstats_df['close'].iloc[-1]
                print("current_price -", current_price)
                to_db_dict["current_price"] = current_price

                rsi = calculate_rsi(symbol, period=14)
                vwap = calculate_vwap(symbol)
                vwma = calculate_vwma(symbol)
                st = calculate_supertrend(symbol, period=10, multiplier=3)
                volume1 = get_volume(symbol, 1)
                volume2 = get_volume(symbol, 2)
                current_price = get_current_price(symbol)

                to_db_dict["rsi"] = rsi
                to_db_dict["vwap"] = vwap
                to_db_dict["vwma"] = vwma
                to_db_dict["st"] = st
                to_db_dict["volume1"] = volume1
                to_db_dict["volume2"] = volume2

                if current_price > vwap:
                    print("Taking call path")
                    path = "CALL"
                    if 50 >= rsi <= 75:
                        if volume1 > 90000:
                            if volume2 > 90000:
                                if st < current_price:
                                    if vwma > current_price:
                                        optionchain = get_option_chain_dataframe("NIFTY")
                                        # print(optionchain.to_markdown())
                                        call_strikeprice_df = \
                                        optionchain[optionchain["CALL LTP"].ge(119) & optionchain["CALL LTP"].lt(151)][
                                            "STRIKE PRICE"]
                                        call_strikeprice_list = call_strikeprice_df.tolist()
                                        # call_strikeprice_list.append(19600)
                                        call_strikeprice = min(call_strikeprice_list)

                                        to_db_dict["call_strikeprice"] = call_strikeprice

                                        print('Strike price -', call_strikeprice)
                                        if not call_strikeprice_list:
                                            # If no strike price available, abort
                                            print("No call strike price available")
                                        else:
                                            print("Buy call")
                                            to_db_dict["buy_signal"] = "Y"

                                            telegram_msg = "Buy " + path + " option of " + symbol + " with " + call_strikeprice + ". %0A RSI - " + rsi + "%0A vwap - " + vwap + "%0A vwma - " + vwma + "%0A st - " + st + "%0A volume1 - " + volume1 + "%0A volume2 - " + volume2
                                            telegram_response = send_to_telegram(telegram_msg)
                                            to_db_dict["telegram_notified"] = telegram_response


                                    else:
                                        print("vwma is lesser than current_price")
                                else:
                                    print("st is lesser than current_price")
                            else:
                                print("second candle is lesser than 12500")
                        else:
                            print("first candle is lesser than 12500")
                    else:
                        print("RSI value does not match")
                else:
                    print("Taking put path")
                    path = "PUT"
                    if 40 < rsi > 25:
                        if volume1 > 90000:
                            if volume2 > 90000:
                                if st < current_price:
                                    if vwma > current_price:
                                        optionchain = get_option_chain_dataframe("NIFTY")
                                        # print(optionchain.to_markdown())
                                        put_strikeprice_df = \
                                        optionchain[optionchain["PUT LTP"].ge(119) & optionchain["PUT LTP"].lt(151)][
                                            "STRIKE PRICE"]
                                        put_strikeprice_list = put_strikeprice_df.tolist()
                                        put_strikeprice = min(put_strikeprice_list)

                                        to_db_dict["put_strikeprice"] = put_strikeprice

                                        to_db_dict["strike_price"] = put_strikeprice
                                        if not put_strikeprice_list:
                                            print("No put strike price available")
                                        else:
                                            print("Buy put")
                                            to_db_dict["buy_signal"] = "Y"

                                            telegram_msg = "Buy " + path + " option of " + symbol + " with " + put_strikeprice + ". %0A RSI - " + rsi + "%0A vwap - " + vwap + "%0A vwma - " + vwma + "%0A st - " + st + "%0A volume1 - " + volume1 + "%0A volume2 - " + volume2
                                            telegram_response = send_to_telegram(telegram_msg)
                                            to_db_dict["telegram_notified"] = telegram_response

                                    else:
                                        print("vwma is lesser than current_price")
                                else:
                                    print("st is lesser than current_price")
                            else:
                                print("second candle is lesser than 12500")
                        else:
                            print("first candle is lesser than 12500")
                    else:
                        print("RSI value does not match")

            else:
                print("not a trade time")
                to_db_dict["is_trade_time"] = "N"
            update_db(to_db_dict)
        except Exception as e:
            print(e)
            continue


def send_to_telegram(text):
    try:
        conn = http.client.HTTPSConnection("api.telegram.org")
        payload = ''
        headers = {}
        text = text.replace(" ", "%20")
        conn.request("POST", "/bot6386426510:AAHfwLLcNx9yOyqw2IKFxNFqJht4PCT49XA/sendMessage?chat_id=-876428015&text=" + text, payload, headers)
        res = conn.getresponse()
        data = res.read()
        print(data.decode("utf-8"))
        return "Y"
    except Exception as e:
        print(e)
        return "N"



# @cross_origin("*")
@app.route('/gui')
def gui():
    return render_template('index.html')


@app.route('/showdbgoldencrossoverdb')
def showdbgoldencrossoverdb():
    table = dbparam["golden_crossover_log_table"]

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    # conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
    #                       r'Server=localhost\MSSQLSERVER01;'
    #                       'Database=algotrade;'
    #                       'Trusted_Connection=yes;')  # integrated security

    cursor = conn.cursor()
    SQLCommand = "SELECT * from " + table
    cursor.execute(SQLCommand)
    results = cursor.fetchall()
    print(results)

    p = []
    # df = pd.DataFrame(results, columns=['symbol', 'timestamp', 'is_trade_time', 'close_20_sma', 'close_7_smma', 'close_9_ema', 'current_price', 'current_ema9_price', 'call_or_put', 'strike_price', 'smma_greater_than_sma', 'hammer_formed', 'adx_value', 'adx_value'])

    tbl = "<tr><td>symbol</td><td>timestamp</td><td>current_price</td><td>vwap</td><td>rsi</td><td>volume1</td><td>volume2</td><td>st</td><td>vwma</td><td>call_strikeprice</td><td>put_strikeprice</td><td>buy_signal</td><td>is_trade_time</td></tr>"
    p.append(tbl)

    for row in results:
        a = "<tr><td>%s</td>" % row[0]
        p.append(a)
        b = "<td>%s</td>" % row[1]
        p.append(b)
        c = "<td>%s</td>" % row[2]
        p.append(c)
        d = "<td>%s</td>" % row[3]
        p.append(d)
        e = "<td>%s</td>" % row[4]
        p.append(e)
        f = "<td>%s</td>" % row[5]
        p.append(f)
        g = "<td>%s</td>" % row[6]
        p.append(g)
        h = "<td>%s</td>" % row[7]
        p.append(h)
        i = "<td>%s</td>" % row[8]
        p.append(i)
        j = "<td>%s</td>" % row[9]
        p.append(j)
        k = "<td>%s</td>" % row[10]
        p.append(k)
        l = "<td>%s</td>" % row[11]
        p.append(l)
        m = "<td>%s</td>" % row[12]
        p.append(m)


    contents = '''<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
    <html>
    <head>
    <meta content="text/html; charset=ISO-8859-1"
    http-equiv="content-type">
    <title>Python Webbrowser</title>
    </head>
    <body>
    <table>
    %s
    </table>
    </body>
    </html>
    ''' % (p)

    filename = 'webbrowser.html'

    def main(contents, filename):
        output = open(filename, "w")
        output.write(contents)
        output.close()

    main(contents, filename)
    webbrowser.open(filename)

    return "Please check the other tab opened for DB view"
    # return render_template('showdb.html')


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8098, debug=False)
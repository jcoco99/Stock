# -*- coding: utf-8 -*-
import datetime
import time
import numpy
import os
import pandas as pd
from sqlalchemy import create_engine

caller_dates = []
caller_opens = []
caller_highs = []
caller_lows = []
caller_closes = []
caller_vols = []
caller_chgs = []
caller_marketcap = []
caller_instnetbuy = []
caller_instcumbuy = []
caller_comparesign = []
caller_money = []


def init_db():
    global engine
    engine = create_engine("mssql+pyodbc://DESKTOP-JMQ2MJT\SQLEXPRESS/stock?driver=SQL+Server", echo=False)

def clear_vals():
    global caller_dates
    global caller_opens
    global caller_highs
    global caller_lows
    global caller_closes
    global caller_vols
    global caller_chgs
    global caller_money
    global caller_marketcap
    global caller_instnetbuy
    global caller_instcumbuy
    global caller_comparesign
    # l[:] = []

    caller_dates[:] = []
    caller_opens[:] = []
    caller_highs[:] = []
    caller_lows[:] = []
    caller_closes[:] = []
    caller_vols[:] = []
    caller_chgs[:] = []
    caller_money[:] = []
    caller_marketcap[:] = []
    caller_instcumbuy[:] = []
    caller_instnetbuy[:] = []
    caller_comparesign[:] = []


def get_datetime(x):
    logdate = str(int(x['logdate']))
    year = int(logdate[:4])
    month = int(logdate[4:6])
    day = int(logdate[6:8])

    return datetime.datetime(year, month, day)


# 단일 종목 분봉 다운로드 --------------------------------------------
def getStockMin(stockcode, cpStockChart):
    print('stockcode', stockcode)
    file_name = 'c:/daily_data/{}.h5'.format(stockcode)
    # SetInputValue
    cpStockChart.SetInputValue(0, stockcode)
    cpStockChart.SetInputValue(1, ord('1'))  
    cpStockChart.SetInputValue(2, 20200131)
    cpStockChart.SetInputValue(3, 20180401)
    cpStockChart.SetInputValue(4, 10000000)
    cpStockChart.SetInputValue(5, (0, 2, 3, 4, 5, 6, 8, 9, 13, 20, 21, 37))
    cpStockChart.SetInputValue(6, ord('D'))
    cpStockChart.SetInputValue(9, ord('1'))


    # BlockRequest
    ret1 = cpStockChart.BlockRequest()
    numData = cpStockChart.GetHeaderValue(3)  # 다운로드된 데이터 행 갯 수
    clear_vals()
    while numData > 1:
        print('numData', numData )
        firstdate = cpStockChart.GetDataValue(0, 0)
        # GetHeaderValue
        numFiled = cpStockChart.GetHeaderValue(1) # 다운로드된 데이터 열 갯수
        # GetDataValue
        for i in range(numData):
            #cpStockChart
            caller_dates.append(cpStockChart.GetDataValue(0, i))
            caller_opens.append(cpStockChart.GetDataValue(1, i))        # 2 시가
            caller_highs.append(cpStockChart.GetDataValue(2, i))        # 3 고가
            caller_lows.append(cpStockChart.GetDataValue(3, i))         # 4 저가
            caller_closes.append(cpStockChart.GetDataValue(4, i))       # 5 종가
            caller_chgs.append(cpStockChart.GetDataValue(5, i))          # 6 전일대비
            caller_vols.append(cpStockChart.GetDataValue(6, i))         # 8 거래량
            caller_money.append(cpStockChart.GetDataValue(7, i))        # 9 거래대금
            caller_marketcap.append(cpStockChart.GetDataValue(8, i))    # 13 시가총액
            caller_instnetbuy.append(cpStockChart.GetDataValue(9, i))   # 20 기관순매수
            caller_instcumbuy.append(cpStockChart.GetDataValue(10, i))  # 21 기관누적순매수
            caller_comparesign.append(cpStockChart.GetDataValue(11, i))  # 37 대비부호

        time.sleep(0.5)
        ret1 = cpStockChart.BlockRequest()
        numData = cpStockChart.GetHeaderValue(3)  # 다운로드된 데이터 행 갯 수
    caller_codes = [stockcode] * len(caller_dates)
    print('codes', len(caller_codes))
    print('dates', len(caller_dates))

    chartData = {'logdate': caller_dates, 'stockcode': caller_codes, 'priceopen': caller_opens, 'pricehigh': caller_highs,
                 'pricelow': caller_lows, 'priceclose': caller_closes, 'volume': caller_vols, 'change': caller_chgs,
                 'amount': caller_money, 'marketCap': caller_marketcap, 'instnetbuy': caller_instnetbuy,
                 'instcumbuy': caller_instcumbuy}
    df = pd.DataFrame(chartData,
                      columns=['logdate', 'stockcode', 'priceopen', 'pricehigh', 'pricelow', 'priceclose', 'volume', 'change', 'amount', 'marketCap', 'instnetbuy',
                               'instcumbuy'])

    df["Datetime"] = df.apply(get_datetime, axis=1)
    df = df.set_index(df["Datetime"])
    df = df.drop(columns='Datetime')
    df = df.sort_index()
    print('start file writing..')
    # df.to_hdf(file_name, stockcode, data_columns=['Date'], format='table', mode='a')
    df.to_sql(name='logday', con=engine, if_exists='append', index=False)

    return

# -------------------  경과 출력 ----------------------------------------
# tstart = datetime.datetime.now() 미리 있어야 함

def showprogress(totalnum, idx, tstart) :
    current_time2 = datetime.datetime.now()
    tt = current_time2 - tstart
    duetime = datetime.datetime.now() + (tt / (idx)) * (totalnum - idx)

    days = int(tt.seconds) / 86400
    hours = int(tt.seconds) / 3600 % 24
    minutes = int(tt.seconds) / 60 % 60
    str_elapsed = "%d일 %d시간 %d분 경과" %(days, hours, minutes)

    str_duetime = duetime.strftime('%m/%d %H:%M:%S')

    dt = duetime - current_time2  # 남은 시간
    days = int(dt.seconds) / 86400
    hours = int(dt.seconds) / 3600 % 24
    minutes = int(dt.seconds) / 60 % 60
    str_last = "%d일 %d시간 %d분 남음" %(days, hours, minutes)

    print("%s/%s: %s,  %s 종료예정, %s" %(idx, totalnum, str_elapsed, str_duetime, str_last ) )


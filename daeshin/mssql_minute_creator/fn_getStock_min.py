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
caller_money = []
caller_time = []


def init_db():
    global engine
    engine = create_engine("mssql+pyodbc://DESKTOP-JMQ2MJT\SQLEXPRESS/stock?driver=SQL+Server", echo=False)
    # engine = create_engine('mysql://root:dongjoo@1@localhost:3306/stock', echo=False)


def clear_vals():
    global caller_dates
    global caller_opens
    global caller_highs
    global caller_lows
    global caller_closes
    global caller_vols
    global caller_money
    global caller_time

    caller_dates[:] = []
    caller_opens[:] = []
    caller_highs[:] = []
    caller_lows[:] = []
    caller_closes[:] = []
    caller_vols[:] = []
    caller_money[:] = []
    caller_time[:] = []


def get_datetime(x):
    logdate = str(int(x['logdate']))
    logminute = str(int(x['logtime']))
    year = int(logdate[:4])
    month = int(logdate[4:6])
    day = int(logdate[6:8])
    hour = int(logminute[:-2])
    minute = int(logminute[-2:])

    return datetime.datetime(year, month, day, hour, minute)

def checkExist(stockcode):
    f = pd.read_sql_query('SELECT count(*) FROM Logminute where stockcode=\'{}\''.format(stockcode),
                          engine)
    cnt = f.ix[0, 0]
    if cnt > 0:
        return True
    else:
        return False


# 단일 종목 분봉 다운로드 --------------------------------------------
def getStockMin(stockcode,  cpStockChart):
    print('stockcode', stockcode)
    if checkExist(stockcode) is True:
        return

    # SetInputValue
    cpStockChart.SetInputValue(0, stockcode)
    cpStockChart.SetInputValue(1, ord('1'))  
    cpStockChart.SetInputValue(2, 20200131)
    cpStockChart.SetInputValue(3, 20180401)
    #cpStockChart.SetInputValue(3, 20100129)
    cpStockChart.SetInputValue(4, 10000000)
    cpStockChart.SetInputValue(5, (0, 1, 2, 3, 4, 5, 8, 9))
    cpStockChart.SetInputValue(6, ord('m'))
    cpStockChart.SetInputValue(9, ord('1'))


    # BlockRequest
    ret1 = cpStockChart.BlockRequest()
    numData = cpStockChart.GetHeaderValue(3)  # 다운로드된 데이터 행 갯 수
    clear_vals()
    while numData > 0 :
        firstdate = cpStockChart.GetDataValue(0, 0)
        # GetHeaderValue
        numFiled = cpStockChart.GetHeaderValue(1) # 다운로드된 데이터 열 갯수
        # GetDataValue
        for i in range(numData):
            #cpStockChart
            caller_dates.append(cpStockChart.GetDataValue(0, i))
            caller_time.append(cpStockChart.GetDataValue(1, i))         # 1 시간
            caller_opens.append(cpStockChart.GetDataValue(2, i))        # 2 시가
            caller_highs.append(cpStockChart.GetDataValue(3, i))        # 3 고가
            caller_lows.append(cpStockChart.GetDataValue(4, i))         # 4 저가
            caller_closes.append(cpStockChart.GetDataValue(5, i))       # 5 종가
            caller_vols.append(cpStockChart.GetDataValue(6, i))         # 8 거래량
            caller_money.append(cpStockChart.GetDataValue(7, i))        # 9 거래대금



        time.sleep(0.5)
        ret1 = cpStockChart.BlockRequest()
        numData = cpStockChart.GetHeaderValue(3)  # 다운로드된 데이터 행 갯 수

    caller_codes = [stockcode] * len(caller_dates)
    print('codes', len(caller_codes))
    print('dates', len(caller_dates))

    if len(caller_dates) == 0:
        # 서버가 점검중일수도 있고, 무튼 어떤 이유에서 작동을 안한거임 그냥 리턴함 ( A06276K도 안됐음 일단 진행)
        return

    chartData = {'logdate': caller_dates, 'logtime': caller_time, 'stockcode': caller_codes, 'priceopen': caller_opens, 'pricehigh': caller_highs,
                 'pricelow': caller_lows, 'priceclose': caller_closes, 'volume': caller_vols,
                 'amount': caller_money}
    df = pd.DataFrame(chartData,
                      columns=['logdate', 'logtime', 'stockcode', 'priceopen', 'pricehigh', 'pricelow', 'priceclose', 'volume', 'amount'])

    df["Datetime"] = df.apply(get_datetime, axis=1)
    df = df.set_index(df["Datetime"])
    df = df.drop(columns='Datetime')
    df = df.sort_index()
    print('start file writing..')
    # df.to_hdf(file_name, stockcode, data_columns=['Date'], format='table', mode='a')
    df.to_sql(name='LogMinute', con=engine, if_exists='append', index=False)
    print('done file writing..')
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


# -*- coding: utf-8 -*-
import win32com.client
import datetime
import fn_getStock_min as ct

cpstockcode = win32com.client.Dispatch("CpUtil.CpStockCode")
cpcodemgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
cpfuturecode = win32com.client.Dispatch("CpUtil.CpFutureCode")

cpStockChart = win32com.client.Dispatch("CpSysDib.StockChart")

NTstocks = cpstockcode.GetCount()    # 전체 stock 갯수
print('다운로드 가능 종목 수:', NTstocks)

tstart = datetime.datetime.now() # 시작 시간

st_start = 1  # 시작 stock num
ct.init_db()

for istock in range(st_start, NTstocks):
    stockcode = cpstockcode.GetData(0, istock)  # 종목 코드
    ct.getStockMin(stockcode, cpStockChart) # 1개 종목 다운로드
    ct.showprogress(NTstocks-st_start, istock+1-st_start, tstart) # 진행상황 표시
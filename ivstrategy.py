# -*- coding: utf-8 -*-
"""
Created on Mon Apr. 24 11:21:22 2017
@author: William Zou
"""
import logging
import pandas as pd
import numpy as np

datapath = r"../Options/Barchart_Options_Data/"
logpath = r'./strategy.log'

logging.basicConfig(filename=logpath,
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

class strategy():
    def __init__(self, date):
        self.date = date
        self.etfstore = self._open_etf_data(date)

    def _open_etf_data(self, date):
        return pd.HDFStore(datapath + 'ETF_options_data_{}.h5'.format(date), "r")

    def _close_etf_data(self):
        self.etfstore.close()

    def _cal_skew(self, df):
        # skew = implied volatility Put(OTM)- implied volatility Call(ATM)
        df["Rate"] = df.Strike/df.Underlying_Price
        # OTM put, strike between 0.80 and 0.95, ATM call, strike between 0.95 and 1.05
        otmp = df[(df.Rate > 0.8) & (df.Rate <= 0.95) & (df.Type == "Put")][["Expiry","ImpliedVolatility"]]
        atmc = df[(df.Rate > 0.95) & (df.Rate <= 1.05) & (df.Type == "Call")][["Expiry","ImpliedVolatility"]]
        otmp["IV"] = otmp.ImpliedVolatility.str.strip("%").astype(float) #/ 100
        atmc["IV"] = atmc.ImpliedVolatility.str.strip("%").astype(float) #/ 100

        volotmp = otmp.groupby("Expiry").mean()
        volatmc = atmc.groupby("Expiry").mean()
        skew = volotmp - volatmc
        return skew

    def find_skews(self):
        skews = []
        for key in self.etfstore.keys():
            df = self.etfstore[key]
            skew = self._cal_skew(df)
            skews.append({"symbol":key, "skew":skew})
        return skews

    # def find_long_short(self):
    #     skews = self.find_skews()
    #     optiondate = pd.Timestamp(self.date) + pd.Timedelta("15days")
    #     datestr = optiondate.strftime("%m/%d/%y")
    #     floatskews = []
    #     for dict in skews:
    #         skew = dict["skew"].ix[datestr, 0]
    #         floatskews.append(skew)
    #     return floatskews

# -*- coding: utf-8 -*-
"""
Created on Mon Apr. 17 13:23:34 2017
@author: William Zou
@reference: Brian Christopher, CFA [Blackarbs LLC]
"""

import pandas as pd
import numpy as np
import requests
import json
from tqdm import tqdm

import logging
import time

class barchart_options():
    # --------------------------------------------
    def __init__(self, symbol):
        self.symbol = symbol
        self.useRaw = ""    # if need show raw, set it to 1

        self.fields_cols = ['expirationDate','optionType','strikePrice','askPrice','bidPrice','lastPrice','priceChange','volatility','theoretical','delta','gamma','rho','theta','vega','openInterest','volume']
        self.df_cols = ['Ask', 'Bid', 'Delta', 'Expiry', 'Gamma', 'Last', 'Open Int', 'Type', 'Change', 'Rho', 'Strike',
                'TheoreticalValue', 'Theta', 'Vega', 'ImpliedVolatility', 'Volume']
        # self.base_api_url = r'https://core-api.barchart.com/v1/options/chain?fields=strikePrice,lastPrice,theoretical,volatility,delta,gamma,rho,theta,vega,volume,openInterest,optionType,daysToExpiration,expirationDate,symbolCode,symbolType&symbol=' + symbol + r'&groupBy=optionType&gt(volatility,0)=&meta=field.shortName,field.description,field.type&raw=1'
        self.base_api_url = r'https://core-api.barchart.com/v1/options/chain?fields={FIELDS}&symbol=' + symbol + r'&groupBy=&gt(volatility,0)=&meta=&raw={RAW}&expirationDate={DATE}'
        # dete format: "2017-05-19"
        self.base_json = json.loads(self.__get_base_src())
        self.last_price_url = r'https://core-api.barchart.com/v1/quotes/get?symbols=' + symbol + r'&fields=lastPrice&meta=&raw=1'
        self.last_price_json = json.loads(self.__get_last_price_src())
        self.reidx_cols = ['Symbol', 'Expiry', 'Type', 'Strike', 'Ask', 'Bid', 'Last', 'Change',
                           'Underlying_Price', 'ImpliedVolatility', 'TheoreticalValue', 'Delta',
                           'Gamma', 'Rho', 'Theta', 'Vega', 'Open Int', 'Volume']

    # --------------------------------------------
    # get basic options data source
    # --------------------------------------------
    def __create_base_url(self):
        url = self.base_api_url.format(FIELDS="", RAW="", DATE="")
        return url

    def __get_base_src(self):
        url = self.__create_base_url()
        logging.info(url)
        with requests.session() as S:
            res = S.get(url)
        return res.text

    def __get_last_price_src(self):
        with requests.session() as S:
            res = S.get(self.last_price_url)
        return res.text

    # --------------------------------------------
    # extract expiry dates
    # --------------------------------------------
    def _extract_expiry_dates(self):
        return self.base_json['meta']['expirations']

    # --------------------------------------------
    # get option source data
    # --------------------------------------------
    def __create_data_url(self, date):
        fields = ",".join(self.fields_cols)
        url = self.base_api_url.format(FIELDS=fields,RAW=self.useRaw,DATE=date)
        return url

    def _get_data_src(self, date):
        url = self.__create_data_url(date)
        # logging.info(url)
        with requests.session() as S:
            res = S.get(url)
        return res.text

    def __get_underlying_last_price(self):
        if self.useRaw == "1":
            return self.last_price_json['data'][0]['raw']['lastPrice']
        else:
            return self.last_price_json['data'][0]['lastPrice']

    # --------------------------------------------
    # create basic options dfs
    # --------------------------------------------
    def _create_data_df(self, expiry):
        src = ""
        jsrc = {}
        for i in range(5):
            src = self._get_data_src(expiry)
            if len(src) == 0:
                continue
            jsrc = json.loads(src)
            # sometimes cannot get the correct data
            if not('data' in jsrc.keys()):
                continue
            else:
                calls = jsrc['data']
                if len(calls) == 0 or not isinstance(calls, list):
                    continue
                else:
                    break
        calls = jsrc['data']
        if self.useRaw == "1":
            calls = [x['raw'] for x in calls]
        raw_df = pd.DataFrame(calls)
        raw_df.columns = self.df_cols
        raw_df['Symbol'] = self.symbol
        raw_df['Underlying_Price'] = self.__get_underlying_last_price()

        mask = raw_df.isin(["N/A", "NA"])
        raw_df = raw_df.where(~mask, other=np.nan)
        raw_df = raw_df.convert_objects(convert_numeric=True)
        # raw_df = raw_df.fillna(0)
        DATA = raw_df.reindex(columns=self.reidx_cols)
        return DATA

    # --------------------------------------------
    # unit test
    # --------------------------------------------
    def unit_test(self, num=-1):
        logging.basicConfig(filename=r'./debug.log',
                            format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                            level=logging.INFO)

        etfstore = pd.HDFStore(r'debug.h5')
        try:
            expirys = self._extract_expiry_dates()
            appended_data = []
            logging.info(expirys)
            # \\\
            if num != -1:
                appended_data.append(self._create_data_df(expirys[num]))
            else:
                for expiry in tqdm(expirys):
                    print (expiry)
                    mrg = self._create_data_df(expiry)
                    appended_data.append(mrg)
            # \\\
            data = pd.concat(appended_data)
            logging.info(data.describe())
            etfstore.open()
            etfstore.put(self.symbol, data, format='table')
            etfstore.close()
        except Exception as e:
            logging.error("ERROR: {}".format(e), exc_info=True)
        return data
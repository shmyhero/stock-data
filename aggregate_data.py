# -*- coding: utf-8 -*-
"""
Created on Mon Apr. 17 13:26:24 2017
@author: William Zou
@reference: Brian Christopher, CFA [Blackarbs LLC]
"""

import time
import pandas as pd
import numpy as np
from copy import copy
from tqdm import tqdm
import logging

from barchart_scraper import barchart_options
p = print
# ------------------------------------------ \\\\
# PLTM, MINT, GUNR, DGT,'IYK','IYC', IPW,'GAF', don't have options
ETFS =  {
       'Large Cap'             :['SPY','IVV','VOO','IWB'],
       'Mid Cap'               :['MDY','IJH','VO','IWR'],
       'Small Cap'             :['IWM','IJR','VB'],
       'Global Equity'         :['VEU','ACWI','VXUS'],
       'AsiaPac Equity'        :['EWT','EWY','EWA','EWS','AAXJ','FXI','EWH','EWM','EPI','INDA','RSX'],
       'Europe Equity'         :['FEZ','EZU','VGK','HEDJ','EWU','EWI','EWP','EWQ','EWL','EWD'],
       'Emerging | Frontier'   :['EWZ','EWW','ECH','FM','EEM','VWO'],
       'Real Estate'           :['RWO','RWX','RWR','IYR','VNQ'],
       'Consumer Discretionary':['XLY','XRT','FXD','VCR','RTH'],
       'Consumer Staples'      :['XLP','FXG','VDC','ECON'],
       'Energy'                :['XLE','XOP','VDE','IYE','IXC','OIH'],
       'Financials'            :['XLF','KBE','KIE','IYG','KRE'],
       'Healthcare'            :['XLV','XBI','IBB'],
       'Industrial'            :['XLI','IYT','VIS','IYJ'],
       'Materials'             :['XLB','XHB','XME','IGE','MOO','LIT'],
       'Technology'            :['XLK','SMH','HACK','FDN'],
       'Telecom'               :['IYZ','IXP','VOX'],
       'Utilities'             :['IDU','XLU','VPU'],
       'Oil | Gas'             :['UNG','BNO','OIL'],
       'Precious Metals'       :['GLD','SLV','IAU'],
       'Bonds'                 :['BND','AGG','JNK','LQD'],
       'T-Bond'                :['TLT','IEF','IEI','SHY','BIL'],
       'Precious Metals Miners':['SIL','GDX','GDXJ']
        }
# ------------------------------------------ \\\\
today = pd.datetime.today().date().strftime('%m-%d-%y')

datapath = r"../Options/Barchart_Options_Data/"
logpath = datapath + r'Barchart_OptionsData_LogFiles/'

logging.basicConfig(filename=logpath + 'BRC_Options_Log_{}.log'.format(today),
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)
# ------------------------------------------ \\\\
etfstore = pd.HDFStore(datapath + 'ETF_options_data_{}.h5'.format(today),'w', complevel=9, complib='blosc')
etf_missing_symbols = []

etf_val_count = 0
for i in ETFS.values(): etf_val_count += len(i)
etf_sym_count = etf_val_count
N = copy(etf_sym_count)
# ------------------------------------------ \\\\
for category, symbols in tqdm(ETFS.items()):
    logging.info("---------- {} ---------".format(category))
    # \\\
    for i, symbol in tqdm(enumerate(symbols, start=1)):
        N -= 1
        # \\\
        if not pd.isnull(symbol):
            try:
                logging.info("symbol:{}".format(symbol))
                brc = barchart_options(symbol)
                expirys = brc._extract_expiry_dates()
                appended_data = []
                # \\\
                for expiry in tqdm(expirys):
                    mrg = brc._create_data_df(expiry)
                    appended_data.append(mrg)
                # \\\
                data = pd.concat(appended_data)
                logging.info(data.describe())
                etfstore.put(symbol, data, format='table')
            except Exception as e:
                logging.error("ERROR: {}".format(e), exc_info=True)
                etf_missing_symbols.append(symbol)
                continue
            pct_total_left = (N / etf_sym_count)
            logging.info('{}..[done] | {} of {} ETF symbols collected | {:>.2%}'.
                         format(symbol, i, len(symbols), pct_total_left))
            p('{}..[done] | {} of {} ETF symbols collected | {:>.2%}'.
                         format(symbol, i, len(symbols), pct_total_left))
            time.sleep(np.random.choice(np.random.uniform(0.5,1.5, [3]), p=[.7, .2, .1]))
# ------------------------------------------ \\\\
# try once more
for symbol in etf_missing_symbols:
    try:
        logging.info("retry symbol:{}".format(symbol))
        brc = barchart_options(symbol)
        expirys = brc._extract_expiry_dates()
        appended_data = []
        # \\\
        for expiry in tqdm(expirys):
            mrg = brc._create_data_df(expiry)
            appended_data.append(mrg)
        # \\\
        data = pd.concat(appended_data)
        logging.info(data.describe())
        etfstore.put(symbol, data, format='table')
    except Exception as e:
        logging.error("ERROR: {}".format(e), exc_info=True)
        logging.error("ERROR: {}".format(symbol))
        continue
    time.sleep(np.random.choice(np.random.uniform(0.5,1.5, [3]), p=[.7, .2, .1]))

etfstore.close()
N_etf_errors = len(etf_missing_symbols)
etf_error_rate = N_etf_errors / etf_sym_count
logging.info('etf missing symbols:\n{}'.format(etf_missing_symbols))
logging.info('etf error rate: {} errors / {} symbols = {:3.2%}'
             .format(N_etf_errors, etf_sym_count, etf_error_rate))
# \\\\ ___________________________________________________________________ \\\\
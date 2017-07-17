# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
Created on Mon May. 17 18:00:00 2017
@author: William Zou
"""
import time
import pandas as pd
import numpy as np
import logging

from schedjob import schedjob
from barchart_scraper import BarchartFutures



def loaddata():
    try:
        # ------------------------------------------ \\\\
        today = pd.datetime.today().date().strftime('%m-%d-%y')

        datapath = r"./Barchart_VIXFutures_Data/"
        logpath = datapath + r'LogFiles/'

        logging.basicConfig(filename=logpath + 'BRC_VIXFuture_Logs.log',
                            format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                            level=logging.INFO)
        # ------------------------------------------ \\\\
        etfstore = pd.HDFStore(datapath + 'ETF_VIXFutures_data_{}.h5'.format(today),'w', complevel=9, complib='blosc')

        logging.info("---------- {} ---------".format(today))
        # ------------------------------------------ \\\\
        brc = BarchartFutures()
        data = brc._create_data_df()
        logging.info(data)
        etfstore.put('VIX', data, format='table')
            
        logging.info('VIX .. [done]')

        etfstore.close()
        logging.shutdown()
    except Exception as e:
        if etfstore:
            etfstore.close()
        logging.shutdown()

if __name__ == '__main__':
    try:
        today = pd.datetime.today().date().strftime('%m-%d-%y')
        # run at 21:30, as the system is 8 hours later, so try to run at 13:30
        start_time = pd.Timestamp(today+' 13:30:00')
        now = pd.Timestamp(pd.datetime.now())
        delay = (start_time-now).seconds
        if (delay < 0):
            delay = delay + 86400
        print('will start job after {} seconds later.'.format(delay))
        schedulejob = schedjob(loaddata, argument=(), delay=delay, repeat=-1, interval=86400)
        schedulejob.run()
    except Exception as e:
        schedulejob.stop()
        
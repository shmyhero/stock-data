# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
Created on Mon May. 11 16:56:44 2017
@author: William Zou
"""

from schedjob import schedjob
import aggregate_data

def loaddata():
    aggregate_data.load_data()

if __name__ == '__main__':
    try:
        schedulejob = schedjob(loaddata, argument=(), delay=80400, repeat=-1, interval=84000)
        schedulejob.run()
    except Exception as e:
        schedulejob.stop()
        
# -*- coding: utf-8 -*-
"""
Created on Mon May. 11 16:08:17 2017
@author: William Zou
"""

import time
import sched

class schedjob():
    def __init__(self, command, argument=(), delay=0, repeat=-1, interval=86400):
        '''
            command: a python function
            argument: parameters for this command, a tuple
            dalay: delay for the first time run()
            repeat: times repeat
            interval: interval between two command
        '''
        self.command = command
        self.argument = argument
        self.delay = delay
        self.repeat = repeat
        self.interval = interval
        self.schedule = sched.scheduler(time.time, time.sleep)
        self.repeatcount = 0
    
    def run(self, blocking=True):
        '''
        Execute events until the queue is empty.
        If blocking is False executes the scheduled events due to
        expire soonest (if any) and then return the deadline of the
        next scheduled call in the scheduler.

        When there is a positive delay until the first event, the
        delay function is called and the event is left in the queue;
        otherwise, the event is removed from the queue and executed
        (its action function is called, passing it the argument).  If
        the delay function returns prematurely, it is simply
        restarted.

        It is legal for both the delay function and the action
        function to modify the queue or to raise an exception;
        exceptions are not caught but the scheduler's state remains
        well-defined so run() may be called again.

        A questionable hack is added to allow other threads to run:
        just after an event is executed, a delay of 0 is executed, to
        avoid monopolizing the CPU when other threads are also
        runnable.
        '''
  
        print("run")
        self.schedule.enter(self.delay, 0, self.execute_command, ())
        self.schedule.run(blocking)
        
    def execute_command(self):
        print("execute command")
        self.command(*self.argument)
        self.repeatcount = self.repeatcount + 1
        if self.repeat == -1 or self.repeatcount < self.repeat:
            self.schedule.enter(self.interval, 0, self.execute_command, ())
        else:
            print("job finished.")
    
    def stop(self):
        print("stop job")
        events = self.schedule._queue
        for event in events:
            print(event)
            self.schedule.cancel(event)
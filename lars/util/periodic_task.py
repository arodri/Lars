import random
import threading
from threading import Timer

class PeriodicTask(object):
    def __init__(self,interval,callback, daemon=True, jitter=0, **kwargs):
        self.interval = interval
        self.callback = callback
        self.daemon = daemon
        self.kwargs = kwargs
        self.jitter = interval*jitter

    def run(self):
        self.callback(**self.kwargs)
        interval = self.interval+(self.jitter*random.uniform(-1,1))
        t = Timer(interval,self.run)
        t.daemon = self.daemon
        t.start()


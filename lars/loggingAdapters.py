import logging

class  WorkflowLoggerAdapter(logging.LoggerAdapter):

        def process(self,msg,kwargs):
                return "%s (id=%s): %s" % (self.extra["hostname"],self.extra["record_id"],msg), kwargs

        def setContext(self,context):
                self.extra = context

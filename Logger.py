import logging
import datetime
#singleton = false

#class SingletonLogger(object):
#    singleton = False
# The class "constructor" - It's actually an initializer 
#    def __init__(self):
#        self.singleton = True

def getLogger():   
#    SingletonLogger  
#    if(singleton):
#        return logger
#    else: 
    logger = logging.getLogger('loyalty_application')


    LOG_FOLDER = "/var/log/hdfs/lbd/loyaltyFcaBank/log/"
    
    if not len(logger.handlers):   
        logger.setLevel(logging.DEBUG)
        # create file handler which logs even debug messages

        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        logname  = 'debug_loyalty_' + now + '.log'
        logger.logname = logname
        logger.logpath = LOG_FOLDER + logname

        fh = logging.FileHandler(filename=logger.logpath, mode="w")

        fh.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        # create console handler with a higher log level
        wh = logging.StreamHandler()
        wh.setLevel(logging.WARNING)
        # create console handler with a higher log level
        cr = logging.StreamHandler()
        cr.setLevel(logging.CRITICAL)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        wh.setFormatter(formatter)
        cr.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)
        logger.addHandler(wh)
        logger.addHandler(cr)
    return logger



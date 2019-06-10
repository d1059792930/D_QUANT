# -*- coding: utf-8 -*-

import logging.handlers
import sys
import time
import os
rq = time.strftime('%Y-%m-%d', time.localtime(time.time()))
class QuantLogger:
    #错误日志按模块名称分成对应的文件夹，下面包括error,info,debug
    def __init__(self, name):
        # 业务日志的配置
        self.logger = logging.getLogger(name)

        self.logger.setLevel(logging.INFO)

        format = logging.Formatter('[%(asctime)s[%(name)s] %(levelname)s: %(message)s')
        if not os.path.exists(sys.path[2] + '/logs/' + name):
            os.mkdir(sys.path[2] + '/logs/' + name)
            print(1)
        handler = logging.handlers.TimedRotatingFileHandler(sys.path[2] + '/logs/' + name + '/info-'+rq+'.log', 'D',encoding="utf-8")
        handler.setFormatter(format)

        self.logger.addHandler(handler)

        # 错误日志的配置
        self.errorLogger = logging.getLogger("ERROR")
        self.errorLogger.setLevel(logging.ERROR)
        errorFormatter = logging.Formatter('[%(asctime)s[' + name + '] %(levelname)s: %(message)s')
        errorHandler = logging.handlers.TimedRotatingFileHandler(sys.path[2] + '/logs/' + name + '/error-'+rq+'.log', 'D',encoding="utf-8")
        errorHandler.setFormatter(errorFormatter)

        self.errorLogger.addHandler(errorHandler)

        # 调试日志的配置
        self.debugLogger = logging.getLogger("DEBUG")
        self.debugLogger.setLevel(logging.DEBUG)
        debugFormatter = logging.Formatter('[%(asctime)s[' + name + '] %(levelname)s: %(message)s')
        debugHandler = logging.handlers.TimedRotatingFileHandler(sys.path[2] + '/logs/' + name + '/debug-'+rq+'.log', 'D',encoding="utf-8")
        debugHandler.setFormatter(debugFormatter)

        self.debugLogger.addHandler(debugHandler)

    def info(self, message, *args):
        self.logger.info(message, *args)

    def error(self, message, *args):
        self.errorLogger.error(message, *args, exc_info=True)

    def debug(self, message, *args):
        self.debugLogger.debug(message, *args)


###########################################################################################
# Logging Functionality
###########################################################################################
# Imports
import logging
import os
from logging import handlers
import sys
import copy


#############################################################################################

class CustomLogger:
    """
    Logger class Implementation.
    """

    def __init__(self, logger_name, logger_file_path='/tmp/'):
        """
        Constructor for logger
        :param logger_name: logger name
        :param logger_file_path: logger file path.
        """
        # logger name
        self.__name = logger_name
        # path where the file needs to be created if we are logging into file handler.
        self.__path = logger_file_path
        # final log path for FileLogging
        self.__logfile = os.path.join(logger_file_path, logger_name + ".log")
        # logger
        self.logger = None
        # set the log format
        self._logformat = "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(processName)s:%(process)d - %(" \
                          "message)s "

    def logger_console(self):
        """
        Logging on to console.
        :return:
        """
        # set the format for the console handler
        console_formatter = logging.Formatter(self._logformat)
        # Streamhandler object => redirecting to sysout
        console_handler = logging.StreamHandler(sys.stdout)
        # set the format for the console handler
        console_handler.setFormatter(console_formatter)
        # logger obj
        self.logger = logging.getLogger(self.__name)
        # add the newly created handler.
        self.logger.addHandler(console_handler)

    def logger_coloredconsole(self):
        """
        return the coloured console logger.
        :return:
        """
        logging.setLoggerClass(ColoredLogger)
        self.logger = logging.getLogger(self.__name)

    def logger_filehandler(self):
        """
        Logging on to a file
        :return:
        """
        # set the format for the FIle handler
        file_formatter = logging.Formatter(self._logformat)
        # FileHandler object => redirecting to log file instead of console.
        file_handler = logging.FileHandler(self.__logfile)
        # set the format for the console handler
        file_handler.setFormatter(file_formatter)
        # logger obj
        self.logger = logging.getLogger(self.__name)
        # add the newly created handler.
        self.logger.addHandler(file_handler)

    def logger_rotatefilehandler(self):
        """
        Logging on to a file which rotates
        :return:
        """
        # set the format for the FIle handler
        file_formatter = logging.Formatter(self._logformat)
        # FileHandler object => redirecting to log file instead of console.
        rotatingfile_handler = logging.handlers.TimedRotatingFileHandler(self.__logfile, 'D', 1, 10)
        # set the format for the console handler
        rotatingfile_handler.setFormatter(file_formatter)
        # logger obj
        self.logger = logging.getLogger(self.__name)
        # add the newly created handler.
        self.logger.addHandler(rotatingfile_handler)


class ColoredLogger(logging.Logger):
    """
    Coloured logging on to the console for error identification.
    """

    def __init__(self, name):
        """
        Constructor
        :param name:
        """
        # Call the super class constructor
        logging.Logger.__init__(self, name, logging.DEBUG)
        # set the formatter
        color_formatter = self.ColoredFormatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %("
                                                "processName)s:%(process)d - %(message)s")
        # set the handler
        console_handler = logging.StreamHandler()
        # set the formatter
        console_handler.setFormatter(color_formatter)
        # add it to the list of handler.
        self.addHandler(console_handler)

    # Formatting class
    class ColoredFormatter(logging.Formatter):
        """
        Format using colour the  console output based on level
            Formatting doc
            print("\033[1;32;40m Bright Green  \n")

            The format is;
                    \033[  Escape code, this is always the same
                    1 = Style, 1 for normal.
                    32 = Text colour, 32 for bright green.
                    40m = Background colour, 40 is for black.
        """
        # change only the text colour
        COLOR_SEQ = "\033[1;{0}m"
        # reset the colour
        RESET_SEQ = "\033[0m"
        # colour codes
        BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(30, 38)

        def __init__(self, msg):
            """
            Constructor for formatting
            :param msg:
            """
            # call the superclass constructor.
            logging.Formatter.__init__(self, msg)
            # class variable for assigning colours to different levels.
            self.COLORS = {
                'DEBUG': self.BLUE,
                'INFO': self.WHITE,
                'WARNING': self.YELLOW,
                'ERROR': self.RED,
                'CRITICAL': self.MAGENTA
            }

        # override from base class
        def format(self, record):
            """
            Method used to actually colour format the record
            :param record: the record which needs to be formatted
            :return: colour formatted record.
            """
            # perform a deep copy of the record.
            color_record = copy.deepcopy(record)
            # if the records level is present in the keys of COLORS dictionary then
            if record.levelname in self.COLORS:
                # format the level name using the colour  reset the colour at the end.
                color_record.levelname = self.COLOR_SEQ.format(
                    self.COLORS[record.levelname]) + record.levelname + self.RESET_SEQ
            return logging.Formatter.format(self, color_record)


# custom queue handler copied from python3 as this is not available in python2
# Copied here so that this code runs on both
class QueueHandler(logging.Handler):
    """
    This handler sends events to a queue. Typically, it would be used together
    with a multiprocessing Queue to centralise logging to file in one process
    (in a multi-process application), so as to avoid file write contention
    between processes.
    This code is new in Python 3.2, but this class can be copy pasted into
    user code for use with earlier Python versions.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue

    def enqueue(self, record):
        """
        Enqueue a record.
        The base implementation uses put_nowait. You may want to override
        this method if you want to use blocking, timeouts or custom queue
        implementations.
        """
        self.queue.put_nowait(record)

    def prepare(self, record):
        """
        Prepares a record for queueing. The object returned by this
        method is enqueued.

        The base implementation formats the record to merge the message
        and arguments, and removes unpickleable items from the record
        in-place.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also puts the message into
        # record.message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info attribute, as it's no longer needed and, if not None,
        # will typically not be pickleable.
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        return record

    def emit(self, record):
        """
        Emit a record.
        Writes the LogRecord to the queue, preparing it first.
        """
        try:
            self.enqueue(self.prepare(record))
        except (KeyboardInterrupt, SystemExit, BrokenPipeError, ConnectionResetError):
            raise
        except Exception as e:
            self.handleError(record)



class LoggerFactory(object):
    """
    Factory for creating the right handlers as per the configurations.
    """

    @staticmethod
    def create_logger(logger_name, file_path, handler_list, log_level):
        """

        :return:
        """
        # create the logger object
        my_logger_obj = CustomLogger(logger_name, file_path)
        # loop through each of the handlers and add them.
        for handler in handler_list:
            # dynamically call the right function.
            func = getattr(my_logger_obj, 'logger_' + handler)
            # call the corresponding function.
            func()

        logger = my_logger_obj.logger
        # set the right logging level
        logger.setLevel(log_level)

        # finally return the logger
        return logger

import datetime
import logging
import logging.handlers
import sys, os
import traceback
import gzip
from   io import StringIO
import os, re

logs_dir = os.path.join(os.getcwd(), 'logs')

class UserException(Exception): pass

def rotator(source, dest, delay = None):
    try:
        taday = datetime.date.today()
        if delay == 'day':
            date_str = taday.strftime('%Y%m%d')
            dest = '%s.%s.gz' % (source , date_str)
        elif delay == 'week':
            date_str = (taday - datetime.timedelta(days = taday.weekday())).strftime('%Y%m%d')
            dest = '%s.%s.gz' % (source , date_str)
        else:
            dest = dest + '.gz'
        if not (os.path.exists(dest) and os.path.isfile(dest)):
            with open(source, "rb") as sf: data = sf.read()
            with gzip.open(dest, 'wb', compresslevel = 9) as f: f.write(data)
            os.remove(source)
    except Exception as err:
        log_error(err, 'faild rotate log file "%s"' % dest, trace = False)

# создание основного логера для возможности записи через них сообщений в лог файлы
main_logger = logging.getLogger("messages")
main_logger.setLevel(logging.ERROR)
main_fh = logging.handlers.TimedRotatingFileHandler(os.path.join(logs_dir, "messages.log"), when = 'H', interval = 1, backupCount = 14)
main_fh.setFormatter(logging.Formatter('%(asctime)s: %(message)s', datefmt = '%Y-%m-%d [%H:%M:%S]') )
main_fh.rotate   = lambda source, dest: rotator(source, dest, delay = 'day')
main_fh.extMatch = re.compile(r"^\d{8}(\.\w+)?$", re.ASCII)
main_logger.addHandler(main_fh)
# создание логера ошибок сервера
error_logger = logging.getLogger("errors")
error_logger.setLevel(logging.ERROR)
error_fh = logging.handlers.TimedRotatingFileHandler(os.path.join(logs_dir, "errors.log"), when = 'H', interval = 1, backupCount = 14)
error_fh.setFormatter(logging.Formatter('%(asctime)s: %(message)s', datefmt = '%Y-%m-%d [%H:%M:%S]') )
error_fh.rotate   = lambda source, dest: rotator(source, dest, delay = 'day')
error_fh.extMatch = re.compile(r"^\d{8}(\.\w+)?$", re.ASCII)
error_logger.addHandler(error_fh)
# создание логера ошибок от мобильных устройств
bug_logger = logging.getLogger("bugs")
bug_logger.setLevel(logging.ERROR)
error_fh = logging.handlers.TimedRotatingFileHandler(os.path.join(logs_dir, "bugs.log"), when = 'H', interval = 1, backupCount = 14)
error_fh.setFormatter(logging.Formatter('%(asctime)s: %(message)s', datefmt = '%Y-%m-%d [%H:%M:%S]') )
error_fh.rotate   = lambda source, dest: rotator(source, dest, delay = 'day')
error_fh.extMatch = re.compile(r"^\d{8}(\.\w+)?$", re.ASCII)
bug_logger.addHandler(error_fh)

# получение трассировки по коду до места возмуждения исключения
def error_text(msg = None):

    buf_exec_info = sys.exc_info()
    bufStream = StringIO()
    bufStream.seek(0)
    traceback.print_tb(buf_exec_info[2], file = bufStream)
    getVal   = bufStream.getvalue()
    if not getVal: return msg
    validVal = getVal.replace('%', '%%')
    validVal = validVal.strip().replace('  ', '') + '\n'
    if msg:
        msg = msg.replace('%', '%%')
        validVal = '%s%s' % ((lambda mes: mes if mes[-1] == '\n' else mes + ':\n')(msg), validVal)
    return validVal

# обработка текста исключения и запись в лог файл с опциональной возможностью трассировки
def log_error(err = None, text = None, trace = True, debug = True, logger = error_logger):
    msg = ''
    #if isinstance(text, str) and ('Forbidden' in text or 'chat not found' in text): return   # отфильтровывать все сообщения от пользователей, заблокировавших бота (по разным причинам) 
    if isinstance(err, Exception):           msg = '%s - %s' % (type(err).__name__, str(err)) # ! есди исключения нет, то и trace не обрабатывается !
    if   text and     msg:                   msg = text + ': ' + msg                          # возможность дописать пользовательский текст
    elif text and not msg:                   msg = text                                       # вывод только пользовательского текста
    if isinstance(err, Exception) and trace: msg = error_text(msg)                            # опциональное включение текста трассировки возбуждения исключения
    logger.error(msg)                                                                         # запись в лог файл errors.log полного текста сообщения об ошибке
    if debug: print(datetime.datetime.now().strftime('%Y-%m-%d [%H:%M:%S]') + ':', msg)

# обработка текста отчёта об ошибке из мабильного приложения и запись в лог файл
def log_bug(msg, debug = False):
    if debug: print(datetime.datetime.now().strftime('%Y-%m-%d [%H:%M:%S]') + ':', msg)
    bug_logger.error(msg) # запись в главынй лог текстового сообщения

def log_message(msg, debug = False):
    if debug: print(datetime.datetime.now().strftime('%Y-%m-%d [%H:%M:%S]') + ':', msg)
    main_logger.error(msg) # запись в главынй лог текстового сообщения

def log_qiwi(msg):
    qiwi_logger.error(msg) # запись в лог qiwi текстового сообщения

def log_ya(msg):
    ya_logger.error(msg)   # запись в лог yandex.money текстового сообщения

if __name__ == '__main__':

    # список файлов на удаление при проверке на следующий час
    print(main_fh.getFilesToDelete())

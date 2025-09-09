from datetime import datetime
import logging, os, sys, time

class common:
    def time():
        return time.time()

    def logger(file, level):
        logging.basicConfig(filename=file, level=level, format='%(thread)d:%(asctime)s - %(levelname)s - %(message)s')
        return logging,logging.getLogger(__name__)
    
    def filereader(filename):
        with open(filename, 'r') as file:
            sql_content = file.read()
            return sql_content
    
    def filedetails(file):
        file_path = file
        file_name = os.path.basename(file_path)
        return file_name

    def logloader():
        args = common.args(__file__)
        level = args[-1]
        sql_file = args[-2]
        logfilename = f"{sql_file.split('.')[0]}_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.log"
        logger, log = common.logger(logfilename, level)
        loggr = [logfilename, logger, log]
        return loggr
    
    def args(filename):
        levels = ["DEBUG", "WARNING", "CRITICAL" ,"INFO", "ERROR"]
        if len(sys.argv) != 8:
            print(f'Usage: python3 {filename} <username> <password> <host> <port> <dbname> <sql_file> <loglevel>')
            print(f'log level can be one of following: \n DEBUG \n WARNING \n CRITICAL \n ERROR \n INFO')
            sys.exit(1)
        username = sys.argv[1]
        password = sys.argv[2]
        hostname = sys.argv[3]
        port = sys.argv[4]
        db_name = sys.argv[5]
        sql_file = sys.argv[6]
        loglevel = sys.argv[7]
        if loglevel not in levels:
            print(f'log level must be one of following: \n >DEBUG \n >WARNING \n >CRITICAL \n >ERROR \n >INFO')
            sys.exit(1)
        if loglevel == "DEBUG":
            loglevel = logging.DEBUG
        elif loglevel == "WARNING":
            loglevel = logging.WARNING
        elif loglevel == "CRITICAL":
            loglevel = logging.CRITICAL
        elif loglevel == "ERROR":
            loglevel = logging.ERROR
        elif loglevel == "INFO":
            loglevel = logging.INFO
        arguments = [username, password, hostname, port, db_name, sql_file, loglevel]
        return arguments 

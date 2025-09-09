import oracledb
import sys
import pandas as pd
import common
import fastparquet as fp
import time

filename = common.common.filedetails(__file__)
arg = common.common.args(filename)
logging = common.common.logloader()[1]
log = common.common.logloader()[2]
logfilename = common.common.logloader()[0]
print(f'View execution logfile: {logfilename}')
sql_file = arg[-2]

class oracleExporter:
    def initiation():
        oracledb.init_oracle_client()
        log.debug('Logger set to %s', log) 
        log.info('Oracle Client version loaded: %s', oracledb.clientversion())
        dsn = f'{arg[0]}/{arg[1]}@{arg[2]}:{arg[3]}/{arg[4]}'
        try:
            conn = oracledb.connect(dsn)
            cursor = conn.cursor()
            log.info('Connection to DB %s successful.', arg[4])
            return cursor
        except oracledb.Error as e:
            log.error('Error connecting to DB %s:\n%s', arg[4] ,e)
            sys.exit(1)
        except oracledb.InterfaceError as ei:
            log.error('Exception connecting to DB %s:\n%s', arg[4], ei)
            sys.exit(1)

    def readsqlfile():
        try:
            with open(sql_file, 'r') as file:
                sqlfile = file.read()
            log.info('File %s read successfully.', sql_file)
            return sqlfile
        except FileNotFoundError:
            log.error('File not found.')
            sys.exit(1)
        except IOError as ie:
            log.error('Error reading file %s:\n%s', sql_file, ie)
            sys.exit(1)

    def runsql():
        cursor = oracleExporter.initiation()
        sql_content = oracleExporter.readsqlfile()
        statements = list(filter(None, sql_content.split(';')))
        log.info('Splitting SQL file to statements.')
        total_statements = len(statements)
        log.info('Total statements within Script: %s', total_statements)
        count = 1
        for statement in statements:
            try:
                log.info('Executing Statement: %s', count)
                start_time = time.time()
                cursor.execute(statement)
                end_time = time.time()
                elapsed_time = end_time - start_time
                elapsed_time = round(elapsed_time,2)
                log.debug('Statement %s executed in %s seconds', count, elapsed_time)
                parquet_file = f'{logfilename.split('.')[0]}_{count}.parquet'
                log.info('Parquet file writing to: %s', parquet_file)
                columns = [desc[0].lower() for desc in cursor.description]  
                log.debug('Statement %s executed of %s', count, total_statements)
                batch = 20000
                result = cursor.fetchmany(batch)
                if result:
                    try:
                        pandf = pd.DataFrame(result, columns=columns)
                        fp.write(parquet_file, pandf)
                        del pandf
                        while True:
                            result = cursor.fetchmany(batch)
                            if not result:
                                break
                            pandf = pd.DataFrame(result, columns=columns)
                            pandf = pandf.replace({None: 'NULL'})
                            fp.write(parquet_file, pandf, append=True)
                            del pandf
                    except Exception as e:
                        log.error('Error creating DataFrame or writing parquet: %s', e)
                        break
                count += 1
            except oracledb.Error as E:
                log.error('Error executing statement %s:\n%s', statement , E)
        cursor.close()
        
if __name__ == "__main__":
    oracleExporter.runsql()
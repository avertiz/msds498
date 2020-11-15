import etl
import RDS_config
import setupPostgreSQL
import psycopg2
import pandas as pd

if __name__ == '__main__':
    
    connection = psycopg2.connect(host = RDS_config.host,
                                  port = RDS_config.port,
                                  user = RDS_config.user,
                                  password = RDS_config.password,
                                  dbname= RDS_config.dbname)
                                  
    updates = etl.updateBorrowSubmissionsTable(connection = connection, new_or_old = 'new', request_times = 500)
    # updates = etl.updateLoansBotTable(new_or_old = 'new', request_times = 500, connection = connection)
    print(updates)
    # link = pd.read_sql("""SELECT permalink 
    #                      from loans_bot
    #                      WHERE created_utc = (SELECT min(created_utc)
    #                                             from loans_bot)
    #             """, connection)
    # print(link['permalink'].iloc[0])
    # setupPostgreSQL.createLoansBotTable(connection)
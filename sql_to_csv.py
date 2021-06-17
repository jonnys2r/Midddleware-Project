#%% 
" Part One - Import Packages "

import pyodbc
import pandas as pd

#%% 
" Part Two - Create connection to ms sql server"

def create_connection():

    global cnxn 
    global cursor

    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};PORT=1433;SERVER=s2rspbes.database.windows.net;PORT=1443;DATABASE=spbes_analytics;UID=spbessqlreadonly;PWD=*H?fu?_sf9VXGRby'

    cnxn =  pyodbc.connect(connection_string)
    cursor = cnxn.cursor()
    
#%% 
" Part Three - Get analytics engineering cell Table, all the data for installation Selbjornsfjord"

def download_sql():
    
    global df
    create_connection()

    sql_statement = "select [voltage_avg],[utc_int_yymmddhhmm_mbu_id], [bbu_id], [mbu_id] from analytics.engineering_cell "
    sql_statement += "inner join assets.mbu on assets.mbu.id = analytics.engineering_cell.mbu_id "
    sql_statement += "inner join assets.pbu on assets.pbu.id = assets.mbu.pbu_id "
    sql_statement += "inner join assets.installation on assets.installation.id = assets.pbu.installation_id "
    sql_statement += "where utc_datetime >= '2021-04-01' and utc_datetime <= '2021-04-30' "
    sql_statement += "and assets.installation.installation_name = 'Selbjornsfjord'"


    df = pd.read_sql(sql_statement, cnxn)

    print('rows:',len(df))
    print('columns:',list(df))

def create_csv():

    df.to_csv('cell.csv',mode='w+')
    
download_sql()
create_csv()
#%% 
" Part Four - Get analtics engineering bbu log table, all the data for installation Selbjornsfjord"

sql_statement = "select [utc_int_yymmddhhmm_mbu_id], [voltage_max] from analytics.engineering_mbu_bbu "
sql_statement += "inner join analytics.reference_date on analytics.reference_id.utc_int_yymmddhhmm_mbu_id = analytics.engineering_mbu_bbu.utc_int_yymmddhhmm_mbu_id "
sql_statement += "inner join assets.mbu on assets.mbu.id = analytics.reference_date.mbu_id "
sql_statement += "inner join assets.pbu on assets.pbu.id = assets.mbu.pbu_id "
sql_statement += "inner join assets.installation on assets.installation.id = assets.pbu.installation_id "
sql_statement += "where utc_datetime >= '2021-04-01' and utc_datetime <= '2021-04-30' "
sql_statement += "and assets.installation.installation_name = 'Selbjornsfjord'"

df_one = pd.read_sql(sql_statement, cnxn)

#%%
" Part Five - Get analytics engineering bbu table, all the data for installation Selbjornsfjord"

sql_statement = "select [mbu_id],[utc_int_yymmddhhmm_mbu_id],[current_avg] from analytics.engineering_bbu "
sql_statement += "inner join assets.mbu on assets.mbu.id = analytics.engineering_bbu.mbu_id "
sql_statement += "inner join assets.pbu on assets.pbu.id = assets.mbu.pbu_id "
sql_statement += "inner join assets.installation on assets.installation.id = assets.pbu.installation_id "
sql_statement += "where utc_datetime >= '2021-04-01' and utc_datetime <= '2021-04-30' "
sql_statement += "and assets.installation.installation_name = 'Selbjornsfjord'"

df_two = pd.read_sql(sql_statement, cnxn)
#%%
" Part Six -  Get analytics engineering mbu table, all the data for installation Selbjornsfjord"

sql_statement = "select [mbu_id],[utc_int_yymmddhhmm_mbu_id],[mbu_string_voltage_MAX], [mbu_current_AVG] from analytics.engineering_mbu "
sql_statement += "inner join analytics.reference_date on analytics.reference_id.utc_int_yymmddhhmm_mbu_id = analytics.engineering_mbu.utc_int_yymmddhhmm_mbu_id "
sql_statement += "inner join assets.mbu on assets.mbu.id = analytics.reference_date.mbu_id "
sql_statement += "inner join assets.pbu on assets.pbu.id = assets.mbu.pbu_id "
sql_statement += "inner join assets.installation on assets.installation.id = assets.pbu.installation_id "
sql_statement += "where utc_datetime >= '2021-04-01' and utc_datetime <= '2021-04-30' "
sql_statement += "and assets.installation.installation_name = 'Selbjornsfjord'"

df_three = pd.read_sql(sql_statement, cnxn)


#%%
" Part Seven - Get anlytics data table, all the data for installation Selbjornsfjord"

sql_statement = "select [installation_id],[mbu_id],[utc_int_yymmddhhmm_mbu_id] from analytics.reference_date "
sql_statement += "inner join assets.mbu on assets.mbu.id = analytics.reference_date.mbu_id "
sql_statement += "inner join assets.pbu on assets.pbu.id = assets.mbu.pbu_id "
sql_statement += "inner join assets.installation on assets.installation.id = assets.pbu.installation_id "

df_four = pd.read_sql(sql_statement, cnxn)

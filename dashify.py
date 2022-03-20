''' mysql/pandas test '''
from dash import Dash, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import pandas as pd 
import secrets 

# makes db connection object
def dbConn():
    return secrets.db_connect()

# generates DF for the passed table
def load_dataframe(table):
    ''' generates df from passed table '''
    print(f'{table = }')
    df = pd.read_sql(table,con=dbConn())
    print(f'{"*"*10}\n{df.head()}\n{"---"*10}')
    return df

# users table
usersDf = load_dataframe(table='users')

# lee county
#leeDf = load_dataframe(table='Lee_County')


# to dict test
def make_dict(arg=None):
    if arg:
        users_dict = usersDf.to_dict(arg)
        for x in users_dict:
            print(x)
        for i in usersDf.columns:
            print(f'{i = }')
            
    else:
        users_dict = usersDf.to_dict()
        for k,v in users_dict.items():
            print(f'{k = }')
            print(f'{v = }')


#dashly call back table 
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

def dashify(dataframe):
    ''' makes interactive dash table for df thats passed in '''
    app.layout = dbc.Container([
         dbc.Label('Lee County GIS Data:\n'),
         dash_table.DataTable(
             dataframe.to_dict('records'),
             [{"name":i, "id":i} for i in dataframe.columns],
             id='tbl'),
         dbc.Alert(id='tbl_out')])

def main():
    lee_df = load_dataframe('Lee_County')
    subdf = lee_df.iloc[:100,:]
    print(f'{subdf.head(10)}')
    print(f'\nSampled Dataframe Size:\n{subdf.describe = }')
    print("")
    dashify(subdf)
    #dashify(lee_df)


if __name__=="__main__":
    main()
    app.run_server(host='45.56.72.250',port=5000)

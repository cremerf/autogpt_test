import psycopg2
import numpy as np
import datetime
import pandas as pd


class ExtractPostgreSQL:

    def __init__(self, username:str, password:str, host:str, port:str, database:str, dialect:str, driver:str, table:str) -> None:

        self.username = username  
        self.password = password  
        self.host = host  
        self.port = port  
        self.database = database
        self.dialect = dialect
        self.driver = driver
        self.table = table
        self.uri_string = f'{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}'
        self.df_dtypes = self.get_table_dtypes(table=table)

        #self.connection = self.connect_to_postgres()


    def connect_to_postgres(self):
        try:
            connection = psycopg2.connect(
                database = self.database,
                user = self.username,
                password = self.password,
                host = self.host,
                port = self.port
            )
            print(f'Connection to {self.host, self.database} OK')
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")

        return connection

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
            print(f"Connection to {self.host, self.database} CLOSED.") 


    def get_table_dtypes(self, table:str) -> pd.DataFrame:

        sql_query = f"SELECT column_name, is_nullable, data_type FROM information_schema.columns asd WHERE table_name = '{table}'"

        try:
            df_dtypes = pd.read_sql_query(sql= sql_query, con= self.connect_to_postgres())
            if len(df_dtypes.values) == 0:
                raise ValueError(f'The specified table: "{table}", does not exist. Please, check your input.')
        except pd.errors.DatabaseError as e:
            #print(f'Check the query: \n - {sql_query}: \n - {e}')
            raise ValueError(f'Check the query: \n - {sql_query}: \n - {e}')
        except psycopg2.DatabaseError as e:
            raise ValueError({e})

        return df_dtypes
    
    def get_meta_rawdf(self) -> pd.DataFrame:

        headers = self.df_dtypes.T.iloc[0]
        df_to_cast  = pd.DataFrame(columns=headers)

        return df_to_cast

    def cast_columns_to_floats(self, df_raw: pd.DataFrame) -> None:

        list_of_numeric_cols = list(self.df_dtypes.loc[self.df_dtypes['data_type'].isin(['integer','bigint', 'uuid']), 'column_name'].values)

        for col in list_of_numeric_cols:
            df_raw[col] = df_raw[col].astype(float)

    def cast_columns_to_strings(self, df_raw: pd.DataFrame) -> None:

        list_of_string_columns = list(self.df_dtypes.loc[self.df_dtypes['data_type'].isin(['text','character varying', 'jsonb']), 'column_name'].values)

        for col in list_of_string_columns:
            df_raw[col] = df_raw[col].astype('object')
    
    def cast_columns_to_bool(self, df_raw: pd.DataFrame) -> None:

        list_of_bool_cols = list(self.df_dtypes.loc[self.df_dtypes['data_type'].isin(['boolean']), 'column_name'].values)

        for col in list_of_bool_cols:
            df_raw[col] = df_raw[col].astype(bool)
    
    def cast_columns_to_datetime(self, df_raw: pd.DataFrame) -> None:

        list_of_date_cols = list(self.df_dtypes.loc[self.df_dtypes['data_type'].isin(['timestamp without time zone']), 'column_name'].values)

        for col in list_of_date_cols:
            df_raw[col] = df_raw[col].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d %H:%M:%S'), meta=pd.Series([], dtype='datetime64[ns]'))



        




from urllib.parse import urlparse
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import psycopg2
import ast


class Interface_db():
    """Class for connection with mysql, postgresql and mongodb

    Attributes:
        scheme [string]: Connection scheme (mysql, postgres or mongodb)
        hostname [string]: Hostname (url)
        username [string]: Username
        password [string]: Username password
        database [string]: Database name
    
    Returns:
        Dataframe: Data in pandas dataframe - get_all() method
        Boolean: Confirmation of data frame conversion to mongodb collection - to_collection() method
    """

    scheme = ""
    hostname = ""
    username = ""
    password = ""
    database = ""
    
    
    def __init__(self, url):
                
        url = urlparse(url)
        
        self.scheme = url.scheme
        self.hostname = url.hostname
        self.username = url.username
        self.password = url.password
        self.database = url.path.lstrip('/')

        
    def connect(self):
        """Database connection method
        """
        if(self.scheme == "mysql"):
            try:
                con = mysql.connector.connect(user=self.username, password=self.password, host=self.hostname, database=self.database)
                cursor = con.cursor()
            except Exception as e:
                print("MySQL connect error: ",str(e))
            else:
                return con, cursor
        elif(self.scheme == "postgres"):
            try:
                con = psycopg2.connect(f"dbname='{self.database}' user='{self.username}' host='{self.hostname}' password='{self.password}'")
                cursor = con.cursor()
            except Exception as e:
                print("Postgres connect error: ",str(e))
            else:
                return con, cursor
        elif(self.scheme == "mongodb"):
            try:
                self.client = MongoClient(self.hostname)
                self.database=self.client[self.database]
            except Exception as e:
                print("Mongodb connect error: ",str(e)) 
        
        
    def disconnect(self, con, cursor):
        """Method for disconnecting from a relational database (Mysql or Postgresql)
        """
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print("Disconnect error: ",str(e)) 


    def get_all(self, rawdataset):
        """Method to return a table or collection in pandas dataframe format
        """
        if(self.scheme == "mysql"):
            try:              
                con, cursor = self.connect()
                cursor.execute(f"select * from {rawdataset};")
            except Exception as e:
                print("Mysql get all error: ",str(e)) 
            else:
                return pd.DataFrame(cursor.fetchall())
            finally:
                self.disconnect(con, cursor)
        elif(self.scheme == "postgres"):
            try:              
                con, cursor = self.connect()
                cursor.execute(f"select * from {rawdataset};")
            except Exception as e:
                print("Postgres get all error: ",str(e)) 
            else:
                return pd.DataFrame(cursor.fetchall())
            finally:
                self.disconnect(con, cursor)
        elif(self.scheme == "mongodb"):
            try:
                self.connect()
                self.collection = self.database[rawdataset]        
                list = []
                collection_data = self.collection.find()
                for d in collection_data:
                    list.append(d)
            except Exception as e:
                print("Mongodb get all error: ",str(e)) 
            else:
                return pd.DataFrame(list)

            
    def to_collection(self, new_dataframe, collection_name):
        """Method to convert a pandas dataframe to nosql collection
        """
        try:
            client = MongoClient(self.hostname)
            db = client[self.database]
            collection = db[collection_name]
            data_dict = new_dataframe.to_json(orient="records")
            data_dict = ast.literal_eval(data_dict)
            collection.insert_many(data_dict)
        except Exception as e:
            print("Insert mongo error: " + str(e))
        else:
            return True
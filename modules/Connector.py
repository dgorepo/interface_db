from pymongo import MongoClient
import mysql.connector
import pandas as pd
import psycopg2
import ast


class Interface_db():
    """Class for connection with mysql, postgresql and mongodb

    Attributes:
        host [string]: Connection host
        database [string]: Database name
    
    Returns:
        Dataframe: Data in pandas dataframe - get_all() method
        Boolean: Confirmation of data frame conversion to mongodb collection - to_collection() method
    """

    host = ""
    database = ""
    
    def __init__(self, host, database):
        
        self.host = host
        self.database = database
        
        # Check host substring to define which data base management system  will be used
        if(host.find("q") == 3):
            self.name = "mysql"
        elif(host.find("g") == 3):
            self.name = "mongodb"
        elif(host.find("t") == 3):
            self.name = "postgres"
    
        
    def connect(self):
        """Database connection method
        """
        if(self.name == "mysql"):
            try:
                con = mysql.connector.connect(user=self.host[self.host.find('?u=')+3:self.host.find('&p=')], password=self.host[self.host.find("&p=")+3:], host=self.host[self.host.find('://')+3:self.host.find('/?u')], database=self.database)
                cursor = con.cursor()
            except Exception as e:
                print("MySQL connect error: ",str(e))
            else:
                return con, cursor
        elif(self.name == "postgres"):
            try:
                con = psycopg2.connect(f"dbname='{self.database}' user='{self.host[self.host.find('?u=')+3:self.host.find('&p=')]}' host='{self.host[self.host.find('://')+3:self.host.find('/?u')]}' password='{self.host[self.host.find('&p=')+3:]}'")
                cursor = con.cursor()
            except Exception as e:
                print("Postgres connect error: ",str(e))
            else:
                return con, cursor
        elif(self.name == "mongodb"):
            try:
                self.client = MongoClient(self.host)
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
        if(self.name=="mysql"):
            try:              
                con, cursor = self.connect()
                cursor.execute(f"select * from {rawdataset};")
            except Exception as e:
                print("Mysql get all error: ",str(e)) 
            else:
                return pd.DataFrame(cursor.fetchall())
            finally:
                self.disconnect(con, cursor)
        elif(self.name=="postgres"):
            try:              
                con, cursor = self.connect()
                cursor.execute(f"select * from {rawdataset};")
            except Exception as e:
                print("Postgres get all error: ",str(e)) 
            else:
                return pd.DataFrame(cursor.fetchall())
            finally:
                self.disconnect(con, cursor)
        elif(self.name=="mongodb"):
            try:
                self.connect()
                self.collection = self.database[rawdataset]        
                lista = []
                dados = self.collection.find()
                for d in dados:
                    lista.append(d)
            except Exception as e:
                print("Mongodb get all error: ",str(e)) 
            else:
                return pd.DataFrame(lista)

            
    def to_collection(self, new_dataframe, collection_name):
        """Method to convert a pandas dataframe to nosql collection
        """
        try:
            client = MongoClient(self.host)
            db = client[self.database]
            collection = db[collection_name]
            data_dict = new_dataframe.to_json(orient="records")
            data_dict = ast.literal_eval(data_dict)
            collection.insert_many(data_dict)
        except Exception as e:
            print("Insert mongo error: " + str(e))
        else:
            return True
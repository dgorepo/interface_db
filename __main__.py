#TODO: Cassandra driver

# _author_ = "Diego Alves"
# _license_ = "Beerware"
# _version_ = "0.0.1"

from modules.Connector import Interface_db
import pandas as pd
import os


if __name__ == "__main__":
    """ Connection with mysql, postgresql and mongodb & 
        return a table or collection in pandas dataframe format &  
        dataframe conversion to mongodb collection
    """

    try:
        
        os.system("clear")        

        # Get all mongodb collections
        # interface_db1 = Interface_db("mongodb://127.0.0.1:27017/soulcode")
        # df_mongo = interface_db1.get_all("professores")
        # print(df_mongo)

        # Get all mysql rows
        # interface_db2 = Interface_db("mysql://root:novasenha@127.0.0.1:3306/atividade13")
        # df_mysql = interface_db2.get_all("dados")
        # print(df_mysql)

        # Get all postgres rows
        # interface_db3 = Interface_db("postgres://postgres:novasenha@localhost/postgres")
        # df_postgres = interface_db3.get_all("funcionarios")
        # print(df_postgres)

        # Convert a dataframe to mongodb collection
        # interface_db4 = Interface_db("mongodb://127.0.0.1:27017/soulcode")
        # print(interface_db4.to_collection(df_mysql, "dados"))
        
    except Exception as e:
        
        print("Main error: ", str(e))
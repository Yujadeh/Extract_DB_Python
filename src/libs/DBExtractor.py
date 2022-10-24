import json
import pyodbc
import pandas as pd

class DBExtractor():
    def __init__(self, configFile: str):
        f = open(configFile)
        data = json.load(f)
        
        self._HOST = data["HOST"]
        self._PORT = data["PORT"]
        self._DATABASE = data["DATABASE"]
        self._USER = data["USER"]
        self._PASSWORD = data["PASSWORD"]
        
        f.close()
 

    def extract(self, targetFile: str):
        conn = None
        
        try:
            conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server}" +
                                ";SERVER=" + self._HOST + 
                                ";DATABASE=" + self._DATABASE + 
                                ";UID=" + self._USER + 
                                ";PWD=" + self._PASSWORD + 
                                ";TrustServerCertificate=Yes")
            
            # Insert your exercise code here
            
            q = """SELECT [ItemId]
      ,[ItemDocumentNbr]
      ,CustomerName
      ,[CreateDate]
      ,[UpdateDate]
      ,ItemSource
FROM (
SELECT  [ItemId]
      ,[ItemDocumentNbr]
      ,C.CustomerName
      ,[CreateDate]
      ,[UpdateDate]
      ,VersionNbr
      ,[DeletedFlag]
      ,ROW_NUMBER()OVER(PARTITION BY ItemId ORDER BY VersionNbr desc) AS RN
      ,CASE WHEN RIGHT(CAST(I.CustomerId AS varchar),2) = '99' OR RIGHT(CAST(I.CustomerId AS varchar),2) = '69' THEN 'Local'
            ELSE 'External' END AS ItemSource
  FROM [ATLAX360_HI_DB].[dbo].[Item] I
  LEFT JOIN [ATLAX360_HI_DB].[dbo].[Customer] C on I.CustomerId = C.CustomerId
) T
WHERE RN = 1 AND [DeletedFlag] = 0"""
            

            
            df = pd.read_sql(q, conn)
            df.to_csv(targetFile, sep=';', encoding='utf-8', compression='gzip')
            
            # End of exercise
        except:
            print("error extracting data from sqlserver")
        finally:        
            if conn: conn.close()

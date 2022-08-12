! pip install  pymysql 
import os  
class Config:   
      MYSQL_HOST = '34.136.184.58'   
      MYSQL_PORT = 3306    
      MYSQL_USER = 'r2de2'   
      MYSQL_PASSWORD = 'I_Love_Data_Engineer'   
      MYSQL_DB = 'r2de2'   
      MYSQL_CHARSET = 'utf8mb4' 
import  pymysql 
connection = pymysql.connect(host = Config.MYSQL_HOST,                             
                             port = Config.MYSQL_PORT ,                               
                             user = Config.MYSQL_USER ,                               
                             password = Config.MYSQL_PASSWORD ,                               
                             db = Config.MYSQL_DB ,                               
                             charset = Config.MYSQL_CHARSET,                               
                             cursorclass = pymysql.cursors.DictCursor) 
connection cursor = connection.cursor() 
cursor.execute("show tables;") 
tables = cursor.fetchall() 
cursor.close() 
print(tables)  

import pandas as pd 
sql = "SELECT * FROM audible_transaction" 
audible_transaction = pd.read_sql(sql,connection) 
sql = "SELECT * FROM audible_data" 
audible_data = pd.read_sql(sql,connection) 

audible_data = audible_data.set_index("Book_ID") 
transaction = audible_transaction.merge(audible_data, how = "left" ,left_on = "book_id" ,right_on = "Book_ID") 
import requests    
url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate" 
r = requests.get(url) 
result_conversion_rate = r.json() 
conversion_rate = pd.DataFrame(result_conversion_rate) 
conversion_rate = conversion_rate.reset_index() 
conversion_rate = conversion_rate.rename(columns = {"index" : "date"}) 
transaction['timestamp'] = pd.to_datetime(transaction['timestamp']).dt.date 
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date  
final_df = transaction.merge(conversion_rate ,how = "left" , left_on = "timestamp" ,right_on = "date")
final_df["Price"] = final_df.apply(lambda x : x["Price"].replace("$",""), axis = 1) 
final_df["Price"] = final_df["Price"]*final_df["conversion_rate"]
final_df = final_df.drop("date" ,axis =1 )
final_df.to_csv("output.csv",index = False)


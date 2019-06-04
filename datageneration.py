
import random 
import pandas as pd
import numpy as np
import json 
from time import time, sleep
from datetime import datetime,timedelta

"""
 Install Azure Service Bus Client in Python
		pip install azure-servicebus
"""
"""
For Python 3 use the below command to import the Azure service bus
	from azure.servicebus.control_client import ServiceBusService
"""
from azure.servicebus import ServiceBusService #For Python 2

# Establishing Connection with Event Hub
sbs = ServiceBusService(service_namespace='Your Created Namespace', shared_access_key_name='RootManageSharedAccessKey', shared_access_key_value='Extracted Key from Connection String'')

shop_items=['Noodles','Flour','Rice','Pancake Mix','Toilet Soap','Ball Pen','Shampoo','Olive Oil','Banana','Pomegranate','Marshmallows','Tropicana','Hair Oil','Room Freshener','Deodorant','Incense Sticks','Coffee Beans','Chamonile Tea','Marker','Napkins']
payment = ['Cash','Debit Card','Credit Card','PayTm']


tr_data={}
df_final=pd.DataFrame()
count = 0
for j in range(1,20000):
    count = count +1
    print(count)
    df=pd.DataFrame()
    for i in range(random.randint(1,20)):
        tr_data_i={}
        tr_data_i['cust_id']=j
        tr_data_i['item']=random.choice(shop_items)
        tr_data_i['units']=random.randint(1,10)
        tr_data_i['unit_price']=np.random.normal(75,30,1)
        tr_data_i['hour_of_day']=np.random.normal(15,2.5,1)
        tr_data_i['payment_method'] = random.choice(payment)
        tr_data_i['cost_price'] = np.random.normal(50,10,1)
        for k in  str(random.randint(1,30)):
            k = int(k)
            tr_data_i['Date'] = (datetime.today() - timedelta(days=k)).strftime('%Y-%m-%d')
            break
        
        data3 = tr_data_i

        data3['unit_price'] = float(data3['unit_price'])
        data3['cost_price'] = float(data3['cost_price'])
        data3['hour_of_day'] = float(data3['hour_of_day'])
        
		# Sends the data to Event Hub
        sbs.send_event('Youe created Event Hub Name', data3)

        


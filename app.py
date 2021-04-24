from flask import Flask, render_template,request,redirect,url_for # For flask implementation
from pymongo import MongoClient # Database connector
from bson.objectid import ObjectId # For ObjectId to work
from bson.errors import InvalidId # For catching InvalidId exception for ObjectId
import os
import csv
import pymongo
import calendar
import datetime
import sys
from flask import Flask, render_template, request, jsonify
import werkzeug
import pandas as pd
import json
from pandas.io.json import json_normalize
from bson import ObjectId
from fbprophet import Prophet
#from flask_table import Table,Col
from bson.json_util import dumps
import json
from datetime import datetime, timedelta
import pickle
from urllib.parse import quote_plus
from sshtunnel import SSHTunnelForwarder
from flask_pymongo import PyMongo
import pmdarima as pm
from pmdarima.model_selection import train_test_split
import numpy as np
from sklearn.metrics import mean_squared_error
from statsmodels.tools.eval_measures import rmse
from statsmodels.tsa.statespace.sarimax import SARIMAX
from pmdarima import auto_arima
import warnings
from bson import json_util
import pytz
from pandas import Series
from sklearn.cluster import KMeans
from flask_cors import CORS
from flask import Response
import socket

#total_Api = 10

app = Flask(__name__)
CORS(app)

MONGO_HOST = 'hostname'
SERVER_USER = 'username'
PRIVATE_KEY = 'your private key'

MONGO_USER = 'ubuntu'
MONGO_PASS = 'ubuntu'
MONGO_DB = "name of database"
    # define ssh tunnel
server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username=SERVER_USER,
    ssh_pkey=PRIVATE_KEY,
    remote_bind_address=('127.0.0.1', 27017)
)
    # start ssh tunnel
server.start()
client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
db = client[MONGO_DB]

@app.route('/')
def get_Host_name_IP():
    try:
        host_name = socket.gethostname()
        host_ip = socket.gethostbyname(host_name)

        #port = socket.getsockname()[1]
        print("port:",port)
        #socket.sethostname("analytics.appemporio.net")
        print("Hostname :",host_name)
        print("IP : ",host_ip)
    except:
        print("Unable to get Hostname and IP")
    return "hi"



@app.route('/tables', methods=['POST'])
def show_tables():
    mycol = db["carts"]
    mycol1 = db["stores"]
    mycol2 = db["substores"]
    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))
    x2 = mycol2.find()
    x2 = pd.DataFrame(list(x2))


    for i in x2.columns:
        print(i)

    print(x1[['_id','server_token']])

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    type = request.headers.get('type')
    print(type)
    #store_id = _json['store_id']
    #store_id = ObjectId(store_id)
    server_token = request.headers.get('servertoken')
    print(server_token)
    start = _json['start_date']
    end = _json['end_date']
    #limit = _json['limit']

    if(type=="1" and _id in x2['_id'].values and server_token in x2['server_token'].values):



        x2 = x2[(x2['_id'] == _id)  & (x2['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x2['main_store_id']
        #print(merge['store_id'])
        x = pd.merge(x,merge,how='inner',on=['store_id'])

        start = pd.to_datetime(start)

        ##end='2020-12-21'
        end = pd.to_datetime(end)

        x['updated_at'] = pd.to_datetime(x['updated_at'])
        #x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]

        mask = (x['updated_at'] >= start) & (x['updated_at'] <= end)


        if(mask.any()==True):
            x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]
            #print(x)
        else:
            x = x
            #print(x)

        y = []
        for i in x['order_details']:
            for j in i:
                for k in j['items']:
                    y.append(k)

        y= pd.DataFrame(list(y))
        for i in y.columns:
            print(i)
        data = y
        limit = 5
        data['profit'] = data['quantity']*data['item_price']
        df = pd.DataFrame({'item': data['item_name'], 'profit': data['profit']})
        df1 = pd.DataFrame({'item': data['item_name'], 'selling': data['quantity']})
        grouped_multiple = df.groupby(['item']).sum().reset_index()
        grouped_multiple = grouped_multiple.sort_values('profit',ascending=False)
        grouped_multiple=grouped_multiple.head(limit)
        gp = df1.groupby(['item']).sum().reset_index()
        gp = gp.sort_values('selling',ascending=False)
        g = gp.head(limit)
        p = gp.tail(limit)

        new_list = []
        new_dict = {}
        new_list1 = []
        new_list2 = []


        for i in range(len(grouped_multiple['item'])):
            temp_dict ={}

            temp_dict["item"] = grouped_multiple["item"].iloc[i]
            temp_dict["value"] = grouped_multiple["profit"].iloc[i]
            #print(temp_dict["item"])
            temp_dict["value"] = temp_dict["value"].item()
            new_list.append(temp_dict)

        for i in range(len(g['item'])):
            temp_dict ={}

            temp_dict["item"] = g["item"].iloc[i]
            temp_dict["value"] = g["selling"].iloc[i]
            #print(temp_dict["item"])
            temp_dict["value"] = temp_dict["value"].item()
            new_list1.append(temp_dict)

        for i in range(len(p['item'])):
            temp_dict ={}

            temp_dict["item"] = p["item"].iloc[i]
            temp_dict["value"] = p["selling"].iloc[i]
            #print(temp_dict["item"])
            temp_dict["value"] = temp_dict["value"].item()
            new_list2.append(temp_dict)

        new_dict["most_profitable_items"] = new_list
        new_dict["most_selling_items"] = new_list1
        new_dict["least_selling_items"] = new_list2
        return json.dumps(new_dict)

    elif(type=="0" and _id in x1['_id'].values and server_token in x1['server_token'].values):

        #x2 = x2[(x2['_id'] == _id)  & (x2['server_token'] == server_token) ]
        x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x1['_id']
        #print(merge['store_id'])
        x = pd.merge(x,merge,how='inner',on=['store_id'])

        start = pd.to_datetime(start)

        ##end='2020-12-21'
        end = pd.to_datetime(end)

        x['updated_at'] = pd.to_datetime(x['updated_at'])
        #x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]

        mask = (x['updated_at'] >= start) & (x['updated_at'] <= end)


        if(mask.any()==True):
            x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]
            #print(x)
        else:
            x = x
            #print(x)

        y = []
        for i in x['order_details']:
            for j in i:
                for k in j['items']:
                    y.append(k)

        y= pd.DataFrame(list(y))
        for i in y.columns:
            print(i)
        data = y
        limit = 5
        data['profit'] = data['quantity']*data['item_price']
        df = pd.DataFrame({'item': data['item_name'], 'profit': data['profit']})
        df1 = pd.DataFrame({'item': data['item_name'], 'selling': data['quantity']})
        grouped_multiple = df.groupby(['item']).sum().reset_index()
        grouped_multiple = grouped_multiple.sort_values('profit',ascending=False)
        grouped_multiple=grouped_multiple.head(limit)
        gp = df1.groupby(['item']).sum().reset_index()
        gp = gp.sort_values('selling',ascending=False)
        g = gp.head(limit)
        p = gp.tail(limit)

        new_list = []
        new_dict = {}
        new_list1 = []
        new_list2 = []


        for i in range(len(grouped_multiple['item'])):
            temp_dict ={}

            temp_dict["item"] = grouped_multiple["item"].iloc[i]
            temp_dict["value"] = grouped_multiple["profit"].iloc[i]
            #print(temp_dict["item"])
            temp_dict["value"] = temp_dict["value"].item()
            new_list.append(temp_dict)

        for i in range(len(g['item'])):
            temp_dict ={}

            temp_dict["item"] = g["item"].iloc[i]
            temp_dict["value"] = g["selling"].iloc[i]
            #print(temp_dict["item"])
            temp_dict["value"] = temp_dict["value"].item()
            new_list1.append(temp_dict)

        for i in range(len(p['item'])):
            temp_dict ={}

            temp_dict["item"] = p["item"].iloc[i]
            temp_dict["value"] = p["selling"].iloc[i]
            #print(temp_dict["item"])
            temp_dict["value"] = temp_dict["value"].item()
            new_list2.append(temp_dict)

        new_dict["most_profitable_items"] = new_list
        new_dict["most_selling_items"] = new_list1
        new_dict["least_selling_items"] = new_list2
        return json.dumps(new_dict)
    else:
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')


@app.route('/CustomerRev+Rec', methods=['POST'])
def show_list1():
    mycol = db["orders"]
    mycol1 = db["order_payments"]
    mycol2 = db["stores"]
    mycol3 = db["substores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))

    y = mycol1.find()
    y =  pd.DataFrame(list(y))

    x1 = mycol2.find()
    x1 =  pd.DataFrame(list(x1))

    x2 = mycol3.find()
    x2=pd.DataFrame(list(x2))


    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    type = request.headers.get('type')
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
 

    if(type=="1" and _id in x2['_id'].values and server_token in x2['server_token'].values):
        x2 = x2[(x2['_id'] == _id )& (x2['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x2['main_store_id']
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        print(x['store_id'])
        #start='2020-11-19'
        start = pd.to_datetime(start)

        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')
        else:
            x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
            y['completed_date_in_city_timezone'] = pd.to_datetime(y['completed_date_in_city_timezone'])

            mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)
            mask1 = (y['completed_date_in_city_timezone'] >= start) & (y['completed_date_in_city_timezone'] <= end)

            print(mask)
            print(mask1)

            if((mask.any()==True)&(mask1.any()==True)):
                print("Inside mask loop ")
                x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]
                y = y[(y['completed_date_in_city_timezone'] >= start) & (y['completed_date_in_city_timezone'] <= end)]
                print(x)
                print(y)
            else:
                print("outside mask loop")
                x = x
                y = y

            name = []
            for i in x['user_detail']:
                name.append(i['name'])
            name= pd.DataFrame(list(name))
            print(name[0])

            data1 = pd.DataFrame({'ouid':x['user_id'],'opuid':y['user_id'],'user_name':name[0],'total':y['total']})
            print(data1)

            data1['total'] = pd.to_numeric(data1['total'],downcast='integer')
            data = data1.groupby('user_name').sum().reset_index()
            data = data.head()

            #data = data.to_json(default_handler=str)

            data2 = data1.groupby('user_name').count().reset_index()
            data2 = data2.head()
            #data2 = data2.to_json(default_handler=str)


            data = data.values.tolist()
            data2 = data2.values.tolist()
            print(data)
            print(data2)

            d1 = []
            count = 0
            while (count < len(data )):
                o1 = {"name":data[count][0] , "total":data [count][1]}
                count = count + 1
                d1.append(o1);

            d2 = []
            count = 0
            while (count < len(data2)):
                o2 = {"item_name": data2[count][0] , "value":data2[count][1]}
                count = count + 1
                d2.append(o2);

            send = {
            "customer_revenue": d1,
            "recursive_customer": d2,
            }

            y = json.dumps(send)
            return y

    elif(type=="0" and _id in x1['_id'].values and server_token in x1['server_token'].values):
        x1 = x1[(x1['_id'] == _id )& (x1['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x1['_id']
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        print(x['store_id'])
        #start='2020-11-19'
        start = pd.to_datetime(start)

        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')
        else:
            x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
            y['completed_date_in_city_timezone'] = pd.to_datetime(y['completed_date_in_city_timezone'])

            mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)
            mask1 = (y['completed_date_in_city_timezone'] >= start) & (y['completed_date_in_city_timezone'] <= end)

            print(mask)
            print(mask1)

            if((mask.any()==True)&(mask1.any()==True)):
                print("Inside mask loop ")
                x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]
                y = y[(y['completed_date_in_city_timezone'] >= start) & (y['completed_date_in_city_timezone'] <= end)]
                print(x)
                print(y)
            else:
                print("outside mask loop")
                x = x
                y = y

            name = []
            for i in x['user_detail']:
                name.append(i['name'])
            name= pd.DataFrame(list(name))
            print(name[0])

            data1 = pd.DataFrame({'ouid':x['user_id'],'opuid':y['user_id'],'user_name':name[0],'total':y['total']})
            print(data1)

            data1['total'] = pd.to_numeric(data1['total'],downcast='integer')
            data = data1.groupby('user_name').sum().reset_index()
            data = data.head()

            #data = data.to_json(default_handler=str)

            data2 = data1.groupby('user_name').count().reset_index()
            data2 = data2.head()
            #data2 = data2.to_json(default_handler=str)


            data = data.values.tolist()
            data2 = data2.values.tolist()
            print(data)
            print(data2)

            d1 = []
            count = 0
            while (count < len(data )):
                o1 = {"name":data[count][0] , "total":data [count][1]}
                count = count + 1
                d1.append(o1);

            d2 = []
            count = 0
            while (count < len(data2)):
                o2 = {"item_name": data2[count][0] , "value":data2[count][1]}
                count = count + 1
                d2.append(o2);

            send = {
            "customer_revenue": d1,
            "recursive_customer": d2,
            }

            y = json.dumps(send)
            return y
    else:
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')

@app.route('/BusyHour', methods=['POST'])
def show_list2():
    mycol =db["carts"]
    mycol1 = db["stores"]
    mycol2 = db["substores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))
    x2 = mycol2.find()
    x2 = pd.DataFrame(list(x2))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    type = request.headers.get('type')
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']

    if(type=="1" and _id in x2['_id'].values and server_token in x2['server_token'].values):

        x2 = x2[(x2['_id'] == _id)  & (x2['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x2['main_store_id']
        #print(merge['store_id'])
        x = pd.merge(x,merge,how='inner',on=['store_id'])

        start = pd.to_datetime(start)
        start = start.replace(hour=0)
        print(start)
        ##end='2020-12-21'
        end = pd.to_datetime(end)
        end = end.replace(hour=1)
        print(end)

        x['updated_at'] = pd.to_datetime(x['updated_at'])
        #x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]

        mask = (x['updated_at'] >= start) & (x['updated_at'] <= end)


        if(mask.any()==True):
            x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]
            print(x)
        else:
            x = x
            print(x)

        data = pd.DataFrame({'date': x['updated_at'], 'order_id': x['order_id'],'timezone':x1['timezone']})
        data = data.groupby(['date'])['order_id'].count()

        data = data.resample("1H").sum().reset_index()
        print(data)
        end = end + timedelta(days=7)
        idx = pd.period_range(start, end)
        idx = idx.to_timestamp()
        idx = pd.DataFrame(idx)
        idx.columns = ["date"]

        idx["date"] = pd.to_datetime(idx['date'])
        print(idx["date"])
        data = data.merge(idx,how='outer',on=['date'])
        data= data.fillna(0)
        data = data.sort_values(by="date").reset_index()

        print(data)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date').resample("1H").sum().reset_index()
        data['hour'] = data['date'].dt.hour

        #data = data.set_index('date').resample("1D").sum().reset_index()
        data['day'] = data['date'].dt.day_name()
        data = data.drop(data.tail(1).index)
        print(data.groupby(["date"]).order_id.sum().reset_index())
        pd.set_option('display.max_rows', data.shape[0]+1)
        print(data)
        #data = data.drop(['date'], axis=1)
        data = data.groupby(["hour","day"]).order_id.sum().reset_index()
        print(len(data))
        print(data.head(10))
        final_list = []
        for i in range(24):
            temp_dict = {}
            for j in range(len(data)):
                print(data['hour'][j])
                if(i == data["hour"][j]):
                    temp_dict[data["day"][j]] = data["order_id"][j]
                    temp_dict[data["day"][j]] = temp_dict[data["day"][j]].item()
            final_list.append(temp_dict)

        final_list = json.dumps(final_list)
        return Response(final_list, mimetype='application/json')

    elif(type=="0" and _id in x1['_id'].values and server_token in x1['server_token'].values):
        x1 = x1[(x1['_id'] == _id )& (x1['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x1['_id']
        x = pd.merge(x,merge,how='inner',on=['store_id'])

        start = pd.to_datetime(start)
        start = start.replace(hour=0)
        print(start)
        ##end='2020-12-21'
        end = pd.to_datetime(end)
        end = end.replace(hour=1)
        print(end)

        x['updated_at'] = pd.to_datetime(x['updated_at'])
        #x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]

        mask = (x['updated_at'] >= start) & (x['updated_at'] <= end)


        if(mask.any()==True):
            x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]
            print(x)
        else:
            x = x
            print(x)

        data = pd.DataFrame({'date': x['updated_at'], 'order_id': x['order_id'],'timezone':x1['timezone']})
        data = data.groupby(['date'])['order_id'].count()

        data = data.resample("1H").sum().reset_index()
        print(data)
        end = end + timedelta(days=7)
        idx = pd.period_range(start, end)
        idx = idx.to_timestamp()
        idx = pd.DataFrame(idx)
        idx.columns = ["date"]

        idx["date"] = pd.to_datetime(idx['date'])
        print(idx["date"])
        data = data.merge(idx,how='outer',on=['date'])
        data= data.fillna(0)
        data = data.sort_values(by="date").reset_index()

        print(data)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date').resample("1H").sum().reset_index()
        data['hour'] = data['date'].dt.hour

        #data = data.set_index('date').resample("1D").sum().reset_index()
        data['day'] = data['date'].dt.day_name()
        data = data.drop(data.tail(1).index)
        print(data.groupby(["date"]).order_id.sum().reset_index())
        pd.set_option('display.max_rows', data.shape[0]+1)
        print(data)
        #data = data.drop(['date'], axis=1)
        data = data.groupby(["hour","day"]).order_id.sum().reset_index()
        print(len(data))
        print(data.head(10))
        final_list = []
        for i in range(24):
            temp_dict = {}
            for j in range(len(data)):
                print(data['hour'][j])
                if(i == data["hour"][j]):
                    temp_dict[data["day"][j]] = data["order_id"][j]
                    temp_dict[data["day"][j]] = temp_dict[data["day"][j]].item()
            final_list.append(temp_dict)

        final_list = json.dumps(final_list)
        return Response(final_list, mimetype='application/json')
    else:
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')

@app.route('/EarningReport', methods=['POST'])
def show():
    mycol = db["order_payments"]
    mycol1 = db["stores"]
    mycol2 = db["substores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))
    x2 = mycol2.find()
    x2 = pd.DataFrame(list(x2))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    type = request.headers.get('type')
    server_token = request.headers.get('servertoken')


    #month = _json['month']
    #month = int(month)
    year = _json['year']
    year = int(year)
    try:
        month = _json['month']
        month = int(month)
    except KeyError:
        month = 0
        pass

    if(type=="1" and _id in x2['_id'].values and server_token in x2['server_token'].values):

        x2 = x2[(x2['_id'] == _id)  & (x2['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x2['main_store_id']
        #print(merge['store_id'])
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        if(month==0):
            data = pd.DataFrame({'refund_amount': x['refund_amount'], 'total': x['total'], 'updated_at':x['updated_at']})
            data['month'] = pd.to_datetime(data['updated_at'])
            #data['date'] = pd.to_datetime(data['date']) - pd.to_timedelta(7, unit='d')
            data = data.groupby([pd.Grouper(key='month')])['total'].sum().reset_index().sort_values('month')

            mask = (data['month'].dt.year == year)
            data = data.loc[mask]
            data['month'] = data['month'].dt.month
            #idx = pd.period_range(start_date, end_date)
            idx = [1,2,3,4,5,6,7,8,9,10,11,12]
            #idx = idx.to_timestamp()
            #print(idx)
            idx = pd.DataFrame(idx)
            idx.columns = ["month"]
            data = data.merge(idx,how='outer',on=['month'])
            data= data.fillna(0)
            print(data)
            data = data.groupby(data['month']).total.sum().reset_index()

            print(data)
            data = data.to_json(orient="records")
            return Response(data, mimetype='application/json')
            #some countOrd
        else:

            if((month==1)or(month==3)or(month==5)or(month==7)or(month==8)or(month==10)or(month==12)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,31)
                end_date = end_date.strftime('%m/%d/%Y')
            elif((month==4)or(month==6)or(month==9)or(month==11)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,30)
                end_date = end_date.strftime('%m/%d/%Y')
            elif((month==2) & (calendar.isleap(year) == False)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,28)
                end_date = end_date.strftime('%m/%d/%Y')
            elif((month==2) & (calendar.isleap(year) == True)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,29)
                end_date = end_date.strftime('%m/%d/%Y')

            data = pd.DataFrame({'refund_amount': x['refund_amount'], 'total': x['total'], 'updated_at':x['updated_at']})
            data['date'] = pd.to_datetime(data['updated_at'])
            data['date'] = data['date'].dt.normalize()
            #data['date'] = pd.to_datetime(data['date']) - pd.to_timedelta(7, unit='d')
            data = data.groupby([pd.Grouper(key='date')])['total'].sum().reset_index().sort_values('date')
            print(data['date'].dt.month)

            mask = (data['date'].dt.month == month) & (data['date'].dt.year == year)
            data = data.loc[mask]
            idx = pd.period_range(start_date, end_date)
            idx = idx.to_timestamp()
            print(idx)
            idx = pd.DataFrame(idx)
            idx.columns = ["date"]
            a = data.merge(idx,how='outer',on=['date'])
            a= a.fillna(0)
            a = a.sort_values(by="date").reset_index(drop=True)
            a['date'] = a['date'].dt.day
            print(a)
            a = a.to_json(orient="records")
            return Response(a, mimetype='application/json')

    elif(type=="0" and _id in x1['_id'].values and server_token in x1['server_token'].values):
        x1 = x1[(x1['_id'] == _id )& (x1['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x1['_id']
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        if(month==0):
            data = pd.DataFrame({'refund_amount': x['refund_amount'], 'total': x['total'], 'updated_at':x['updated_at']})
            data['month'] = pd.to_datetime(data['updated_at'])
            #data['date'] = pd.to_datetime(data['date']) - pd.to_timedelta(7, unit='d')
            data = data.groupby([pd.Grouper(key='month')])['total'].sum().reset_index().sort_values('month')

            mask = (data['month'].dt.year == year)
            data = data.loc[mask]
            data['month'] = data['month'].dt.month
            #idx = pd.period_range(start_date, end_date)
            idx = [1,2,3,4,5,6,7,8,9,10,11,12]
            #idx = idx.to_timestamp()
            #print(idx)
            idx = pd.DataFrame(idx)
            idx.columns = ["month"]
            data = data.merge(idx,how='outer',on=['month'])
            data= data.fillna(0)
            print(data)
            data = data.groupby(data['month']).total.sum().reset_index()

            print(data)
            data = data.to_json(orient="records")
            return Response(data, mimetype='application/json')
            #some countOrd
        else:

            if((month==1)or(month==3)or(month==5)or(month==7)or(month==8)or(month==10)or(month==12)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,31)
                end_date = end_date.strftime('%m/%d/%Y')
            elif((month==4)or(month==6)or(month==9)or(month==11)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,30)
                end_date = end_date.strftime('%m/%d/%Y')
            elif((month==2) & (calendar.isleap(year) == False)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,28)
                end_date = end_date.strftime('%m/%d/%Y')
            elif((month==2) & (calendar.isleap(year) == True)):
                start_date = pd.datetime(year,month,1)
                start_date = start_date.strftime('%m/%d/%Y')
                end_date = pd.datetime(year,month,29)
                end_date = end_date.strftime('%m/%d/%Y')

            data = pd.DataFrame({'refund_amount': x['refund_amount'], 'total': x['total'], 'updated_at':x['updated_at']})
            data['date'] = pd.to_datetime(data['updated_at'])
            data['date'] = data['date'].dt.normalize()
            #data['date'] = pd.to_datetime(data['date']) - pd.to_timedelta(7, unit='d')
            data = data.groupby([pd.Grouper(key='date')])['total'].sum().reset_index().sort_values('date')
            print(data['date'].dt.month)

            mask = (data['date'].dt.month == month) & (data['date'].dt.year == year)
            data = data.loc[mask]
            idx = pd.period_range(start_date, end_date)
            idx = idx.to_timestamp()
            print(idx)
            idx = pd.DataFrame(idx)
            idx.columns = ["date"]
            a = data.merge(idx,how='outer',on=['date'])
            a= a.fillna(0)
            a = a.sort_values(by="date").reset_index(drop=True)
            a['date'] = a['date'].dt.day
            print(a)
            a = a.to_json(orient="records")
            return Response(a, mimetype='application/json')
    else:
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')



@app.route('/Predict7', methods=['POST'])
def predict7():
    #a = myclient.server_info()
    #return a

    mycol = db["orders"]
    mycol1= db["order_payments"]
    mycol2 = db["substores"]
    mycol3 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))

    x1 = mycol3.find()
    x1 = pd.DataFrame(list(x1))

    x2 = mycol2.find()
    x2 = pd.DataFrame(list(x2))

    y = mycol1.find()
    y =  pd.DataFrame(list(y))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    type = request.headers.get('type')
    server_token = request.headers.get('servertoken')
    period = _json['period']

    #_id = ObjectId("5fb385b289fdca7513d17d0a")
    #server_token = "LhJTzSiKpgteY5dkLziIWECyaK6jG98q"
    if(type=="1" and _id in x2['_id'].values and server_token in x2['server_token'].values):


        x2 = x2[(x2['_id'] == _id)  & (x2['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x2['main_store_id']
        #print(merge['store_id'])
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        da = x

        da = da.query("order_status_id==10").reset_index(drop=True)
        da['date'] = pd.to_datetime(da['completed_at'])
            #print(da)


        data = pd.DataFrame({'store_id':da['store_id'],'order_id':da['_id'],'date': da['date']})
        data1 = pd.DataFrame({'order_id':y['order_id'],'total':y['total']})
            #print(data1)
        df = pd.merge(data,data1,on='order_id',how='left')

        df['date'] = df['date'].dt.strftime("%Y-%m-%d")
        df = df.groupby('date').sum().reset_index()
        df1 = df.groupby('date').sum()
        print(df)

        ######### if data is not change use this pikle model other wise add model (m = Prophet(),m.fit(df)) ########

        df.columns = ['ds', 'y']
        with open('forecast_model.pckl', 'rb') as fin:
            m2 = pickle.load(fin)
        #periods = 9
        future = m2.make_future_dataframe(periods=period)
        forecast = m2.predict(future)
        forecast1 = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(period)
        forecast1 = forecast1.to_json(orient='records',default_handler=str)
        return Response(forecast1, mimetype='application/json')

    elif(type=="0" and _id in x1['_id'].values and server_token in x1['server_token'].values):
        x1 = x1[(x1['_id'] == _id )& (x1['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x1['_id']
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        da = x

        da = da.query("order_status_id==10").reset_index(drop=True)
        da['date'] = pd.to_datetime(da['completed_at'])
            #print(da)


        data = pd.DataFrame({'store_id':da['store_id'],'order_id':da['_id'],'date': da['date']})
        data1 = pd.DataFrame({'order_id':y['order_id'],'total':y['total']})
            #print(data1)
        df = pd.merge(data,data1,on='order_id',how='left')

        df['date'] = df['date'].dt.strftime("%Y-%m-%d")
        df = df.groupby('date').sum().reset_index()
        df1 = df.groupby('date').sum()
        print(df)

        ######### if data is not change use this pikle model other wise add model (m = Prophet(),m.fit(df)) ########

        df.columns = ['ds', 'y']
        with open('forecast_model.pckl', 'rb') as fin:
            m2 = pickle.load(fin)
        #periods = 9
        future = m2.make_future_dataframe(periods=period)
        forecast = m2.predict(future)
        forecast1 = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(period)
        forecast1 = forecast1.to_json(orient='records',default_handler=str)
        return Response(forecast1, mimetype='application/json')
    else:
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')

@app.route('/Predict7_Order', methods=['POST'])
def predict7_Order():
    mycol = db["orders"]
    mycol1 = db["stores"]
    mycol2 = db["substores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))

    x1 = mycol1.find()
    x1 = pd.DataFrame(list(x1))

    x2 = mycol2.find()
    x2 = pd.DataFrame(list(x2))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    type = request.headers.get('type')
    period = _json['period']

    print("outside loop")
    if(type=="1" and _id in x2['_id'].values and server_token in x2['server_token'].values):

        print("inside loop")
        x2 = x2[(x2['_id'] == _id)  & (x2['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x2['main_store_id']
        #print(merge['store_id'])
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        print(x)
        df = {'store_id':x['store_id'],'order_id':x['_id'],'order_status_id':x['order_status_id'],'date':x['completed_date_in_city_timezone']}
        df['date'] = pd.to_datetime(df['date'])
        #df['countOrder'] = df['date'].dt.date.count()
        df = pd.DataFrame(df)
        print(df)
        df['date'] = df['date'].dt.strftime("%Y-%m-%d")
        df = df.groupby('date')['order_id'].count().reset_index()
        df.columns = ['ds', 'y']
        m = Prophet()
        m2 = m.fit(df)
        future = m2.make_future_dataframe(periods=period)
        future = future.tail(period).reset_index(drop=True)
        print(future)
        ##################################
        df.columns = ['date','order']
        df['date'] = pd.to_datetime(df['date'])
        model = model = SARIMAX(df['order'],order = (0, 1, 1),seasonal_order =(2, 1, 1, 12))
        result = model.fit()
        forecast = result.predict(start = len(df),end = (len(df)-1) + period ,typ = 'levels').rename('Forecast')
        forecast = forecast.reset_index(drop=True)
        print(forecast)
        df['future'] = future
        df['forecast'] = forecast
        df['forecast'] = df['forecast'].apply(np.ceil)
        df = df[['future','forecast']].dropna()
        df = df.to_json(orient='records', date_format='iso', default_handler=str)
        return Response(df, mimetype='application/json')
    elif(type=="0" and _id in x1['_id'].values and server_token in x1['server_token'].values):
        x1 = x1[(x1['_id'] == _id )& (x1['server_token'] == server_token) ]
        merge = pd.DataFrame()
        merge['store_id'] = x1['_id']
        x = pd.merge(x,merge,how='inner',on=['store_id'])
        df = {'store_id':x['store_id'],'order_id':x['_id'],'order_status_id':x['order_status_id'],'date':x['completed_date_in_city_timezone']}
        df['date'] = pd.to_datetime(df['date'])
        #df['countOrder'] = df['date'].dt.date.count()
        df = pd.DataFrame(df)
        print(df)
        df['date'] = df['date'].dt.strftime("%Y-%m-%d")
        df = df.groupby('date')['order_id'].count().reset_index()
        df.columns = ['ds', 'y']
        m = Prophet()
        m2 = m.fit(df)
        future = m2.make_future_dataframe(periods=period)
        future = future.tail(period).reset_index(drop=True)
        print(future)
        ##################################
        df.columns = ['date','order']
        df['date'] = pd.to_datetime(df['date'])
        model = model = SARIMAX(df['order'],order = (0, 1, 1),seasonal_order =(2, 1, 1, 12))
        result = model.fit()
        forecast = result.predict(start = len(df),end = (len(df)-1) + period ,typ = 'levels').rename('Forecast')
        forecast = forecast.reset_index(drop=True)
        print(forecast)
        df['future'] = future
        df['forecast'] = forecast
        df['forecast'] = df['forecast'].apply(np.ceil)
        df = df[['future','forecast']].dropna()
        df = df.to_json(orient='records', date_format='iso', default_handler=str)
        return Response(df, mimetype='application/json')
    else:
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')

@app.route('/Busiest_Day_Week', methods=['POST'])
def Busiest_Day_Week():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
    #_id = ObjectId("5fb385b289fdca7513d17d0a")
    #server_token = "LhJTzSiKpgteY5dkLziIWECyaK6jG98q"
    x1 = x1[(x1['_id'] == _id)]

    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        #start='2020-11-19'
        start = pd.to_datetime(start)

        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')


        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
        #x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]

        mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)


        if(mask.any()==True):
            x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]

        else:
            x = x

        df = {'order_id':x['_id'],'date':x['completed_date_in_city_timezone']}
        df = pd.DataFrame(df)
        df = df.dropna()
        df['date'] = pd.to_datetime(df['date'])
        df['date'] =df['date'].dt.day_name()
        dff = df.groupby([pd.Grouper(key='date')])['order_id'].count().reset_index().sort_values('date')
        dff = dff.to_json()
        return dff

@app.route('/Busiest_Hour', methods=['POST'])
def Busiest_Hour():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
  
    x1 = x1[(x1['_id'] == _id)]

    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        #start='2020-11-19'
        start = pd.to_datetime(start)
        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')

        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
        mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)
        if(mask.any()==True):
            x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]
        else:
            x = x
        df = {'order_id':x['_id'],'date':x['completed_date_in_city_timezone']}
        df = pd.DataFrame(df)
        df = df.dropna()
        df['date'] = pd.to_datetime(df['date']) - pd.to_timedelta(7, unit='d')
        df = df.groupby([pd.Grouper(key='date', freq='60Min')])['order_id'].count().reset_index().sort_values('date')
        df['date'] = [datetime.time(d) for d in df['date']]
        dff = df.groupby([pd.Grouper(key='date')])['order_id'].count().reset_index().sort_values('date')
        dff = dff.to_json()
        print(dff)
        return dff


@app.route('/Time_Analysis', methods=['POST'])
def Time_Analysis():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
    #_id = ObjectId("5fb385b289fdca7513d17d0a")
    #server_token = "LhJTzSiKpgteY5dkLziIWECyaK6jG98q"
    x1 = x1[(x1['_id'] == _id)]

    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        #start='2020-11-19'
        start = pd.to_datetime(start)
        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')

        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
        mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)
        if(mask.any()==True):
            x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]
        else:
            x = x


        whole = []
        for i in x['date_time']:
            for j in i:
                whole.append(j)

        df = pd.DataFrame(whole)
        #print(df)

        s1 = df.query('status == 1').reset_index(drop=True)
        s1 = Series(s1['date'])
        #print(s1)
        s3 = df.query('status == 3').reset_index(drop=True)
        s3 = Series(s3['date'])
        #print(s3)
        s5 = df.query('status == 5').reset_index(drop=True)
        s5 = Series(s5['date'])
        #print(s5)
        s7 = df.query('status == 7').reset_index(drop=True)
        s7 = Series(s7['date'])
        #print(s7)
        s25 = df.query('status == 25').reset_index(drop=True)
        s25 = Series(s25['date'])
        #print(s25)


        def mi(a,b):
            ans = abs(a-b)
            ans = ans.dropna()
            avg = (ans.sum()/len(ans))
            return avg

        diff1_3 = mi(s1,s3)
        diff3_5 = mi(s3,s5)
        diff5_7 = mi(s5,s7)
        diff7_25 = mi(s7,s25)


        df = {'diff1_3':diff1_3,'diff3_5':diff3_5,'diff5_7':diff5_7,'diff7_25':diff7_25}

        import datetime
        def myconverter(o):
            if isinstance(o, datetime.timedelta):
                return o.__str__()

        js_df = json.dumps(df,default=myconverter)
        return js_df

@app.route('/Time_Analysis_Deliveries', methods=['POST'])
def Time_Analysis_Deliveries():
    mycol = db["requests"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))



    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']

    x1 = x1[(x1['_id'] == _id)]

    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]

    store_id =[]
    for i in x['store_detail']:
            store_id.append(i['_id'])

    x['store_id'] = store_id
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')

    #print(df.head(20))
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        #start='2020-11-19'
        start = pd.to_datetime(start)
        #end='2020-12-21'
        end = pd.to_datetime(end)
        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')
        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
        mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)
        if(mask.any()==True):
            x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]
        else:
            x = x
        '''
        9_11_13_15_17_19_21_25
        '''
        whole = []
        for i in x['date_time']:
            for j in i:
                whole.append(j)

        df = pd.DataFrame(whole)
        s9 = df.query('status == 9').reset_index(drop=True)
        s9 = Series(s9['date'])
        #print(s9)

        s11 = df.query('status == 11').reset_index(drop=True)
        s11 = Series(s11['date'])
        #print(s11)
        s13 = df.query('status == 13').reset_index(drop=True)
        s13 = Series(s13['date'])
        #print(s13)
        s15 = df.query('status == 15').reset_index(drop=True)
        s15 = Series(s15['date'])
        #print(s15)
        s17 = df.query('status == 17').reset_index(drop=True)
        s17 = Series(s17['date'])
        #print(s17)
        s19 = df.query('status == 19').reset_index(drop=True)
        s19 = Series(s19['date'])
        #print(s19)
        s21 = df.query('status == 21').reset_index(drop=True)
        s21 = Series(s21['date'])
        #print(s21)
        s25 = df.query('status == 25').reset_index(drop=True)
        s25 = Series(s25['date'])
        #print(s25)


        def mi(a,b):
            ans = abs(a-b)
            ans = ans.dropna()
            avg = (ans.sum()/len(ans))
            return avg

        diff9_11 = mi(s9,s11)
        diff11_13 = mi(s11,s13)
        diff13_15 = mi(s13,s15)
        diff15_17 = mi(s15,s17)
        diff17_19 = mi(s17,s19)
        diff19_21 = mi(s19,s21)
        diff21_25 = mi(s21,s25)


        df = {'diff9_11':diff9_11,'diff11_13':diff11_13,'diff13_15':diff13_15,'diff15_17':diff15_17,'diff17_19':diff17_19,'diff19_21':diff19_21,'diff21_25':diff21_25}

        import datetime
        def myconverter(o):
            if isinstance(o, datetime.timedelta):
                return o.__str__()

        js_df = json.dumps(df,default=myconverter)
        return js_df

@app.route('/Monthly_Active_User', methods=['POST'])
def Monthly_Active_User():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
  
    x1 = x1[(x1['_id'] == _id)]
    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
            #start='2020-11-19'
            start = pd.to_datetime(start)

            #end='2020-12-21'
            end = pd.to_datetime(end)

            if end <= start:
                data = {'Success':False}
                data = json.dumps(data)
                return Response(data, mimetype='application/json')


            x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
            #x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]

            mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)


            if(mask.any()==True):
                x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]

            else:
                x = x
            df = {'user_id':x['user_id'],'YearMonth':x['completed_date_in_city_timezone']}
            df = pd.DataFrame(df)
            df['YearMonth'] = df['YearMonth'].map(lambda date: 100*date.year + date.month)
            Monthly_Active = df.groupby('YearMonth')['user_id'].nunique().reset_index()
            Monthly_Active = Monthly_Active.to_json()
            return Monthly_Active


@app.route('/New_User_Ratio', methods=['POST'])
def New_User_Ratio():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']

    x1 = x1[(x1['_id'] == _id)]
    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        x['YearMonth'] = x['completed_date_in_city_timezone'].map(lambda date: 100*date.year + date.month)
        tx_min_purchase = x.groupby('user_id').completed_date_in_city_timezone.min().reset_index()
        tx_min_purchase.columns = ['user_id','MinPurchaseDate']
        tx_min_purchase['MinPurchaseYearMonth'] = tx_min_purchase['MinPurchaseDate'].map(lambda date: 100*date.year + date.month)


        x = pd.merge(x, tx_min_purchase, on='user_id')


        x['UserType'] = 'New'
        x.loc[x['YearMonth']>x['MinPurchaseYearMonth'],'UserType'] = 'Existing'


        tx_user_type_revenue = x.groupby(['YearMonth','UserType'])['total'].sum().reset_index()
        tx_user_type_revenue = tx_user_type_revenue.to_json()
        return tx_user_type_revenue

@app.route('/User_Monthly_Retention', methods=['POST'])
def User_Monthly_Retention():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
  
    x1 = x1[(x1['_id'] == _id)]
    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        #start='2020-11-19'
        start = pd.to_datetime(start)

        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')


        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
        #x = x[(x['updated_at'] >= start) & (x['updated_at'] <= end)]

        mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)


        if(mask.any()==True):
            x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]

        else:
            x = x
        x['YearMonth'] = x['completed_date_in_city_timezone'].map(lambda date: 100*date.year + date.month)
        tx_user_purchase = x.groupby(['user_id','YearMonth'])['total'].sum().reset_index()
        tx_retention = pd.crosstab(tx_user_purchase['user_id'], tx_user_purchase['YearMonth']).reset_index()
        tx_retention = tx_retention.to_json(default_handler=str)
        return tx_retention

@app.route('/User_Weekly_Retention', methods=['POST'])
def User_Weekly_Retention():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
    
    x1 = x1[(x1['_id'] == _id)]
    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        #start='2020-11-19'
        start = pd.to_datetime(start)
        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')

        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
        mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)
        if(mask.any()==True):
            x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]
        else:
            x = x

        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone']).dt.week

        tx_user_purchase = x.groupby(['user_id','completed_date_in_city_timezone'])['total'].sum().reset_index()
        tx_retention = pd.crosstab(tx_user_purchase['user_id'], tx_user_purchase['completed_date_in_city_timezone'])
        tx_retention = tx_retention.to_json(default_handler=str)
        return tx_retention

@app.route('/Customer_Segment', methods=['POST'])
def Customer_Segment():
    mycol = db["orders"]
    mycol1 = db["stores"]

    x = mycol.find()
    x =  pd.DataFrame(list(x))
    x1 = mycol1.find()
    x1 =  pd.DataFrame(list(x1))

    _json = request.json
    _id = _json['_id']
    _id = ObjectId(_id)
    server_token = request.headers.get('servertoken')
    start = _json['start_date']
    end = _json['end_date']
   
    x1 = x1[(x1['_id'] == _id)]
    x1 = x1[(x1['_id'] == _id)  & (x1['server_token'] == server_token)  ]
    x = x[x['store_id'] == _id]
    if(x['store_id'].any() != _id):
       data = {'Success':False}
       data = json.dumps(data)
       return Response(data, mimetype='application/json')
    if(len(x1) == 0):
        data = {'Success':False}
        data = json.dumps(data)
        return Response(data, mimetype='application/json')
    else:
        #start='2020-11-19'
        start = pd.to_datetime(start)
        #end='2020-12-21'
        end = pd.to_datetime(end)

        if end <= start:
            data = {'Success':False}
            data = json.dumps(data)
            return Response(data, mimetype='application/json')

        x['completed_date_in_city_timezone'] = pd.to_datetime(x['completed_date_in_city_timezone'])
        mask = (x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)
        if(mask.any()==True):
            x = x[(x['completed_date_in_city_timezone'] >= start) & (x['completed_date_in_city_timezone'] <= end)]
        else:
            x = x

        tx_user = pd.DataFrame(x['user_id'].unique())
        tx_user.columns = ['user_id']

        tx_max_purchase = x.groupby('user_id').completed_date_in_city_timezone.max().reset_index()
        tx_max_purchase.columns = ['user_id','MaxPurchaseDate']

        #we take our observation point as the max invoice date in our dataset
        tx_max_purchase['Recency'] = (tx_max_purchase['MaxPurchaseDate'].max() - tx_max_purchase['MaxPurchaseDate']).dt.days


        tx_user = pd.merge(tx_user, tx_max_purchase[['user_id','Recency']], on='user_id')
        tx_user = tx_user.dropna()


        #build 4 clusters for recency and add it to dataframe
        kmeans = KMeans(n_clusters=4)
        kmeans.fit(tx_user[['Recency']])
        tx_user['RecencyCluster'] = kmeans.predict(tx_user[['Recency']])

        #function for ordering cluster numbers
        def order_cluster(cluster_field_name, target_field_name,df,ascending):
            new_cluster_field_name = 'new_' + cluster_field_name
            df_new = df.groupby(cluster_field_name)[target_field_name].mean().reset_index()
            df_new = df_new.sort_values(by=target_field_name,ascending=ascending).reset_index(drop=True)
            df_new['index'] = df_new.index
            df_final = pd.merge(df,df_new[[cluster_field_name,'index']], on=cluster_field_name)
            df_final = df_final.drop([cluster_field_name],axis=1)
            df_final = df_final.rename(columns={"index":cluster_field_name})
            return df_final

        tx_user = order_cluster('RecencyCluster', 'Recency',tx_user,False)
        ############FREQUENCY####################
        tx_frequency = x.groupby('user_id').completed_date_in_city_timezone.count().reset_index()
        tx_frequency.columns = ['user_id','Frequency']

        tx_user = pd.merge(tx_user, tx_frequency, on='user_id')

        kmeans = KMeans(n_clusters=4)
        kmeans.fit(tx_user[['Frequency']])
        tx_user['FrequencyCluster'] = kmeans.predict(tx_user[['Frequency']])

        tx_user = order_cluster('FrequencyCluster', 'Frequency',tx_user,True)


        ################MONETARY_VALUE/REVENURE###########
        tx_revenue = x.groupby('user_id').total.sum().reset_index()
        tx_revenue.columns = ['user_id','Revenue']

        tx_user = pd.merge(tx_user, tx_revenue, on='user_id')

        kmeans = KMeans(n_clusters=4)
        kmeans.fit(tx_user[['Revenue']])
        tx_user['RevenueCluster'] = kmeans.predict(tx_user[['Revenue']])

        tx_user = order_cluster('RevenueCluster', 'Revenue',tx_user,True)

        print(tx_user)
        ################OVERALL######################

        tx_user['OverallScore'] = tx_user['RecencyCluster'] + tx_user['FrequencyCluster'] + tx_user['RevenueCluster']
        tx_user.groupby('OverallScore')['Recency','Frequency','Revenue'].mean()


        print(tx_user.groupby('OverallScore')['Recency'].count())

        #########SEGMENT######################

        tx_user['Segment'] = 'Low-Value'
        tx_user.loc[tx_user['OverallScore']>2,'Segment'] = 'Mid-Value'
        tx_user.loc[tx_user['OverallScore']>4,'Segment'] = 'High-Value'
        tx_user = tx_user.to_json(default_handler=str)
        return tx_user



if __name__ == "__main__":
    app.run(port=5000,debug=True)

import http.client
import json
import pandas as pd
import pickle as pk
from urllib.parse import urlencode
from datetime import datetime, timedelta
import time
from collections import defaultdict
import math
import pandas as pd
import heapq as hq
import heapq
import numpy as np
import openpyxl         
from openpyxl import Workbook, load_workbook
import sys
import numpy
_url="/maps/api/distancematrix/json?"
_sec="KdrWGBeHLpdBotRnqUca7Y3pWxm0N"
conn = http.client.HTTPSConnection(host="api.distancematrix.ai")


df=pd.read_excel("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\3monthdata.xlsx", sheet_name=None)
df1=pd.read_csv("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\location_data.csv")
df2=pd.read_csv("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\one_day_data.csv")
df3=pd.read_excel("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\Top_20.xlsx")
df4=pd.read_csv("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\Top_20_branches.csv")
df5=pd.read_csv("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\Top_20_Location.csv")
df6=pd.read_csv("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\Top_60_Packages_Weight.csv")
all_df = pd.concat(df, ignore_index=True)
pd.set_option('display.max_rows', None)
all_df['packages_weight']=all_df['number_of_packages']*all_df['actual_weight']
df6['lat'].apply(lambda x: float(x))
df6['long'].apply(lambda x: float(x))
def distance(origin, destination):
    lat1, lon1=origin
    lat2, lon2=destination
    radius=6371
    dlat=math.radians(lat2-lat1)
    dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)*math.sin(dlat/2)+math.cos(math.radians(lat1)) \
        *math.cos(math.radians(lat2))*math.sin(dlon/2)*math.sin(dlon/2)
    c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))
    d=radius*c
    return d

def helper(x):
    return x

my_set=set([])
for i in df6.index:
    my_set.add(df6['Branches'][i]) 
src="COKB"
dest="BDQB"
src1="COKB"
dest1="JAIB"
src2="AMDB"
dest2="IXJB"
src3="HYDB"
dest3="GAUB"
origin_lat0=(float)(df6[df6['Branches']==src].lat)
origin_long0=(float)(df6[df6['Branches']==src].long)
origin0=(origin_lat0,origin_long0)


dest_lat0=(float)(df6[df6['Branches']==dest].lat)
dest_long0=(float)(df6[df6['Branches']==dest].long)
destination0=(dest_lat0,dest_long0)
origin_lat1=(float)(df6[df6['Branches']==src1].lat)
origin_long1=(float)(df6[df6['Branches']==src1].long)
origin1=(origin_lat1,origin_long1)

dest_lat1=(float)(df6[df6['Branches']==dest1].lat)
dest_long1=(float)(df6[df6['Branches']==dest1].long)
destination1=(dest_lat1,dest_long1)

origin_lat2=(float)(df1[df1['code']==src2].lat)
origin_long2=(float)(df1[df1['code']==src2].long)
origin2=(origin_lat2,origin_long2)


dest_lat2=(float)(df1[df1['code']==dest2].lat)
dest_long2=(float)(df1[df1['code']==dest2].long)
destination2=(dest_lat2,dest_long2)
print(src)
print(dest)
print(distance(origin0,destination0))
print(src1)
print(dest1)
print(distance(origin1,destination1))
print(src2)
print(dest2)
print(distance(origin2,destination2))

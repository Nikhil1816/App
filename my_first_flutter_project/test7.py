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
import numpy as np
import networkx as nx


df=pd.read_excel("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\3monthdata.xlsx", sheet_name=None)
df1=pd.read_csv("C:\\Users\\Nikhil\\Desktop\\dpWorld\\data\\location_data.csv")
all_df = pd.concat(df, ignore_index=True)
pd.set_option('display.max_rows', None)
print(all_df.groupby(['pickup_branch_code'])['number_of_packages'].sum().sort_values(ascending=False))
print(all_df.groupby(['delivery_branch_code'],sort=True)['number_of_packages'].sum().sort_values(ascending=False))
all_loc={}
count=0
for i in df1.index:
    all_loc[count]=df1['code'][i]
    count=count+1
print(all_loc)
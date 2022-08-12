# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Open Ambulance Data
# MAGIC 22-07-2022 jonathanpearson@nhs.net
# MAGIC 
# MAGIC **Notes**
# MAGIC 
# MAGIC https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/ambulance-systems-indicators-ambsys https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2022/07/20220714-AQI-Stats-
# MAGIC Note.pdf https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2019/09/20190912-AmbSYS-specification.pdf https://www.england.nhs.uk/wp-
# MAGIC content/uploads/2019/09/Operational_productivity_and_performance_NHS_Ambulance_Trusts_final.pdf

# COMMAND ----------

# MAGIC %pip install fsspec

# COMMAND ----------

# Import libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import io
import fsspec

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Import and Sturcutre Data

# COMMAND ----------

##Define location of lakes, folders & files
lakeName="udalstdataanalysisprod.dfs.core.windows.net"
containerName="ambulance-hackathon"

# COMMAND ----------

# Import AmbSys-to-June22-nodots
fileName="/SourceData/Test/JPearson/AmbSYS-to-Jun22-nodots.csv"
fullPath="abfss://"+containerName+"@"+lakeName+fileName

df_AmbSys = spark.read.format('csv').options(header='true', inferSchema='true').load(fullPath).toPandas()

# COMMAND ----------

# Import AmbSYSindicatorlist
fileName="/SourceData/Test/JPearson/AmbSYSindicatorlist.csv"
fullPath="abfss://"+containerName+"@"+lakeName+fileName

df_AmbSysIndicatorList = spark.read.format('csv').options(header='true', inferSchema='true').load(fullPath).toPandas()

# COMMAND ----------

ambSerName = {'RX9': 'EMAS',
               'RYC': 'EEAST',
               'RRU': 'LAS',
               'RX6': 'NEAS',
               'RX7': 'NWAS',
               'RYE': 'SCAS',
               'RYD': 'SECAmb',
               'RYF': 'SWAS',
               'RYA': 'WMAS',
               'RX8': 'YAS',
               'R1F': 'IOW'
}

# COMMAND ----------

# Import ccgtoAmbMapping
fileName="/SourceData/Test/JPearson/ccgtoAmbMapping.csv"
fullPath="abfss://"+containerName+"@"+lakeName+fileName

df_ccgmapping = spark.read.format('csv').options(header='true', inferSchema='true').load(fullPath).toPandas()

# COMMAND ----------

df_AmbSys.dtypes

# COMMAND ----------

# Intial vis of data
df_AmbSys=df_AmbSys.replace(['.'], np.nan)
df_AmbSys.describe(include = 'all')

# COMMAND ----------

df_AmbSys.count()

# COMMAND ----------

df_AmbSys.columns = [c.replace(' ', '_') for c in df_AmbSys.columns]
df_AmbSys.Org_Code.unique()

# COMMAND ----------

df_AmbSys["datetime"] = pd.to_datetime(df_AmbSys[['Year', 'Month']].assign(day=15))
df_AmbSys.head()


# COMMAND ----------

#pd.set_option('max_rows', 30)
df_AmbSys.groupby([df_AmbSys["datetime"], "Org_Code"]).A3.mean()

# COMMAND ----------

df_AmbSys_reduced = df_AmbSys[(df_AmbSys['datetime'] >= '2019-11-15') & (df_AmbSys['datetime'] <= '2022-08-15')]
df_AmbSys_reduced = df_AmbSys_reduced[df_AmbSys_reduced['Org_Code'].str.contains("R")]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visual Investigation of Data
# MAGIC 
# MAGIC Graph time series for each ambulance service some of the metrics of interest.

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A25')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A25: Mean C1 Response Time' per ambulance service")
axs.set_ylabel("A25: Mean Time")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A28')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A28: Mean C1T Response Time' per ambulance service")
axs.set_ylabel("A28: Mean Time")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A31')


# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A31: Mean C2 Response Time' per ambulance service")
axs.set_ylabel("A31: Mean Time")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A34')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A34: Mean C3 Response Time' per ambulance service")
axs.set_ylabel("A34: Mean Time")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A37')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A37: Mean C4 Response Time' per ambulance service")
axs.set_ylabel("A37: Mean Time")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A1')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A1: # Calls Answered' per ambulance service")
axs.set_ylabel("A1: # Calls Answered")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A3')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A3: Mean Call Time' per ambulance service")
axs.set_ylabel("A3: Mean Call Time")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC RRU is London Ambulance trust. Clearly the mean time had an issue at the start of Covid but has then done better than most other services more recently.

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A7')

# COMMAND ----------


fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A7: All Incidents' per ambulance service")
axs.set_ylabel("A7: # Incidents")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------


data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A115')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A115: C1 Incidents Excluding HCP & IFT' per ambulance service")
axs.set_ylabel("A115: # Incidents")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A119')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A119: C2 Incidents Excluding HCP & IFT' per ambulance service")
axs.set_ylabel("A119: # Incidents")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A53')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A53: To ED' per ambulance service")
axs.set_ylabel("A53: #")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A56')


# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A56: Non ED' per ambulance service")
axs.set_ylabel("A56: #")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A111')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A111: from 111' per ambulance service")
axs.set_ylabel("A111: #")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

data = df_AmbSys_reduced.pivot(index='datetime',columns='Org_Code',values='A15')

# COMMAND ----------

fig, axs = plt.subplots(figsize=(12,4))

#axs.plot('datetime', 'A3', data=data)
for col in data.columns:
    axs.plot(data[col], label='Line '+col)
axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1,7)))
axs.grid(True)
axs.set_title("Quick view of 'A15: time to identify NoC' per ambulance service")
axs.set_ylabel("A15: Mean Time")
axs.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
for label in axs.get_xticklabels(which='major'):
    label.set(rotation=30, horizontalalignment='right')

plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Discussion
# MAGIC ## Thoughts from available stats releases
# MAGIC We can see from https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2022/07/20220714-AQI-Stats-Note.pdf that:
# MAGIC 
# MAGIC - the response times across all categories are worsening
# MAGIC - the deteriation appears to start at the onset of Covid restrictions in March 2020
# MAGIC - C1 & C1T appear to have similar trends and so will be treated together for speed
# MAGIC - C2,C3 & C4 appear to have similar trends and so will be treated together for speed
# MAGIC - C1/C1T have a different rate of deteriation from C2/C3/C4
# MAGIC - The mean and 90th show similar trends albeit the 90th elongating as would be expected for an increasing mean
# MAGIC - There was a spike in calls times around March 2020 which has become the current norm from Jun-21
# MAGIC 
# MAGIC ## Our charts above show
# MAGIC 
# MAGIC - isle of wight is an outlier for C1 and East Midlands (RX9) are an outlier for C1T.
# MAGIC - Across all response times we see a large variation across ambulance trusts but all still seem to have an underlying driver causing increases in the mean times.
# MAGIC - C4 response time increase are recently driven by North West (RX7)
# MAGIC - the mean call time spike in March 2020 was driven by London (RRU)
# MAGIC - C1 incidents appear to have increase but C2 haven't
# MAGIC - Number of "To ED" has remianed reasonable stable
# MAGIC - Incidents are stable! Calls are increasing.
# MAGIC - a mixed picture from 111 call transfer with diffeerent services seeing increases and decreases.
# MAGIC 
# MAGIC ## Questions
# MAGIC 
# MAGIC - Is the call time increasing because the operator stays on the call until ambulance arrives and the ambulances are taking longer to get there
# MAGIC - Why is NoC identification increasing (does this also depend on ambulance availability)
# MAGIC - Overall is there an issue with the calls or is it the ambulance availability which is driving the mean times and number of calls
# MAGIC - specific ambulance trust outlier questions
# MAGIC - Why are calls increasing (slightly) but incidents not
# MAGIC - Do we need to focus on HCP and ITF
# MAGIC 
# MAGIC ## What to look at next
# MAGIC 
# MAGIC - Correlation:
# MAGIC   - Change in # calls answered Vs change in C1 and C2 response times
# MAGIC   - could we say that
# MAGIC     - Response Time ~ (number ambulances * mean call time) / call frequency
# MAGIC     - can we see a consistent number of abmulances per service from above
# MAGIC    - Do we need to look at IFTs more
# MAGIC 
# MAGIC ## Correlation

# COMMAND ----------

dataRX9 = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RX9")]
dataRYC = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RYC")]
dataR1F = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "R1F")]
dataRRU = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RRU")]
dataRX6 = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RX6")]
dataRX7 = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RX7")]
dataRYE = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RYE")]
dataRYD = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RYD")]
dataRYF = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RYF")]
dataRYA = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RYA")]
dataRX8 = df_AmbSys_reduced[(df_AmbSys_reduced['Org_Code'] == "RX8")]

# COMMAND ----------

corrA3A25 = []
corrA3A25.append(dataRX9["A3"].corr(dataRX9["A25"]))
corrA3A25.append(dataRYC["A3"].corr(dataRYC["A25"]))
corrA3A25.append(dataR1F["A3"].corr(dataR1F["A25"]))
corrA3A25.append(dataRRU["A3"].corr(dataRRU["A25"]))
corrA3A25.append(dataRX6["A3"].corr(dataRX6["A25"]))
corrA3A25.append(dataRX7["A3"].corr(dataRX7["A25"]))
corrA3A25.append(dataRYE["A3"].corr(dataRYE["A25"]))
corrA3A25.append(dataRYD["A3"].corr(dataRYD["A25"]))
corrA3A25.append(dataRYF["A3"].corr(dataRYF["A25"]))
corrA3A25.append(dataRYA["A3"].corr(dataRYA["A25"]))
corrA3A25.append(dataRX8["A3"].corr(dataRX8["A25"]))
plt.plot(corrA3A25)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Excluding Isle of Wight (R1F - index 2) as an outlier (the correlation between mean call time and C1 response times is between 0.6 and 0.9)

# COMMAND ----------

corrA3A31 = []
corrA3A31.append(dataRX9["A3"].corr(dataRX9["A31"]))
corrA3A31.append(dataRYC["A3"].corr(dataRYC["A31"]))
corrA3A31.append(dataR1F["A3"].corr(dataR1F["A31"]))
corrA3A31.append(dataRRU["A3"].corr(dataRRU["A31"]))
corrA3A31.append(dataRX6["A3"].corr(dataRX6["A31"]))
corrA3A31.append(dataRX7["A3"].corr(dataRX7["A31"]))
corrA3A31.append(dataRYE["A3"].corr(dataRYE["A31"]))
corrA3A31.append(dataRYD["A3"].corr(dataRYD["A31"]))
corrA3A31.append(dataRYF["A3"].corr(dataRYF["A31"]))
corrA3A31.append(dataRYA["A3"].corr(dataRYA["A31"]))
corrA3A31.append(dataRX8["A3"].corr(dataRX8["A31"]))
plt.plot(corrA3A31)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Similar correlation levels and pattern across services for C2

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Fancy Visual (just because)
# MAGIC 
# MAGIC Can we show a map of ambulance services coloured by change from the same month in 2019 possibly with a scrll bar to move through time.
# MAGIC 
# MAGIC https://amaral.northwestern.edu/blog/step-step-how-plot-map-slider-represent-time-evolu

# COMMAND ----------

# Further Imports

import plotly
import plotly.graph_objs as go
import plotly.offline as offline

from plotly.graph_objs import *
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

from urllib.request import urlopen
import json

# COMMAND ----------

data = df_AmbSys_reduced
data.head()

# COMMAND ----------

# Set comparison month and year which will also be start date
startDatetime = df_AmbSys_reduced["datetime"].min()
data_slider = []

# COMMAND ----------

with urlopen('https://nihr.opendatasoft.com/api/records/1.0/search/?dataset=ccg-boundaries&q=') as response: 
  ccgboundaries = json.load(response)

# COMMAND ----------

for record in ccgboundaries["records"]:
    for k, v in record['fields'].items():
        if k == 'ccg21nm':
            print(v)

# COMMAND ----------

data_ccgleftjoin = pd.merge(left=df_ccgmapping[['NHS CCG code','CCG name','Ambulance Code','Ambulance Service ']],right=data, how='left',left_on='Ambulance Code',right_on='Org_Code')

# COMMAND ----------


data_ccgleftjoin.rename(columns = {'CCG name':'ccg21nm'}, inplace=True)

# COMMAND ----------

# new str column for mouse hover
data_str = data_ccgleftjoin.copy()

for col in data_ccgleftjoin.columns:
    data_str[col] = data_ccgleftjoin[col].astype(str)

data_ccgleftjoin['text'] = data_str['Org_Code'] + ' A25 - C1 response times: ' + data_str['A25'] + 'A3 - Mean Call Time: ' + data_str['A3']

# COMMAND ----------

data_ccgleftjoin = data_ccgleftjoin[['NHS CCG code','ccg21nm','Ambulance Code','Ambulance Service ','datetime','text','Org_Code','Org_Name','A3','A25']]
data_ccgleftjoin.head(n=100)

# COMMAND ----------

import plotly.express as px

fig = px.choropleth_mapbox(data_ccgleftjoin, geojson=ccgboundaries, locations='ccg21nm', color='A25',
                           #range_color=(0, 12),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": -3.056805902750275, "lon": 53.77653973249381},
                           opacity=0.5,
                           labels={'A25':'C1 Response Time'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

# COMMAND ----------

datam = [ dict(
            type='choropleth', 
            autocolorscale = False,
            locations = data['Org_Code'], 
            z = data['A25'].astype(float), 
            locationmode = 'geojson-id', 
            text = data['text'], 
            marker = dict(     # for the boudary lines 
                        line = dict (
                                  color = 'rgb(255,255,255)', 
                                  width = 1) ),               
            colorbar = dict(
                        title = "C1 Response Time")
            ) 
       ]

# COMMAND ----------

import requests
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

#with urlopen('https://nihr.opendatasoft.com/api/records/1.0/search/?dataset=ccg-boundaries&q=') as response: 
#  ccgboundaries = json.load(response)


df = data_ccgleftjoin
fig = px.scatter_geo(
    df,
    locations="ccg21nm",
    size="A3",  # size of markers, "pop" is one of the columns of gapminder
)


fig = fig.add_trace(
    go.Scattergeo(
        lat=[
            v
            for sub in [
                np.array(f["geometry"]["coordinates"])[:, 1].tolist() + [None]
                for record in ccgboundaries["records"]:
                  for k, v in record['fields'].items():
                    if k == 'ccg21nm':
                      v
            ]
            for v in sub
        ],
        lon=[
            v
            for sub in [
                np.array(f["geometry"]["coordinates"])[:, 0].tolist() + [None]
                for f in ccgboundaries["records"][0]["fields"]["ccg21nm"]
            ]
            for v in sub
        ],
        line_color="brown",
        line_width=1,
        mode="lines",
        showlegend=False,
    )
)

fig.update_geos(
    visible=True, resolution=50, scope="world", showcountries=True, countrycolor="Black"
)

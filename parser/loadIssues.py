import json
import pandas as pd
import re
import numpy as np
from datetime import datetime


def bugsDate(date_end, bugsFinal):
    dataFrame=bugsFinal
    dataFrame['created_at'] = pd.to_datetime(dataFrame['created_at'])
    dataFrame['closed_at'] = pd.to_datetime(dataFrame['closed_at'])
    dataFrame = newColum(bugsFinal)

    mask = (dataFrame['created_at'] <= date_end)
    return dataFrame.loc[mask]

def newColum(dataFrame):
    dataFrame['interval'] = dataFrame.apply(lambda row: (row['closed_at']-row['created_at']), axis=1)
    return dataFrame


def getAverage(date,dataFrame):
    total=bugsDate(date, dataFrame)
    total=total[pd.notnull(total['interval'])]
    total['intervalHour']=total.apply(lambda row: (row['interval']) / np.timedelta64(1, 'h'), axis=1)
    #print(total)
    return total['intervalHour'].mean(axis = 0)

i=1
data=[]
while i<237:
    file='lxd_'+str(i)+'.json'
    with open('/home/nellybett/Desktop/issues/'+file) as json_file:
        data = data+json.load(json_file)

    i=i+1

bugs=[]
for i in data:
    for l in i['labels']:
        if l['name']=='Bug':
            bugs.append(i)

sortedArray = sorted(
    bugs,
    key=lambda x: datetime.strptime(x['created_at'], '%Y-%m-%dT%H:%M:%SZ'), reverse=True
)

bugsFinal=[]
for i in bugs:
    d = {}
    d["node_id"] =i["node_id"]
    d["title"] =i["title"]
    d["created_at"] =datetime.strptime(i["created_at"],'%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%y')
    d["closed_at"] =i["closed_at"]
    bugsFinal.append(d)

bugsFinal=pd.DataFrame(bugsFinal)

#NUMBER OF BUGS

#print(bugsDate('2020-03-19',bugsFinal).count())
#print(bugsDate('2020-03-31',bugsFinal).count())
#print(bugsDate('2020-01-17',bugsFinal).count())
#print(bugsDate('2019-10-02',bugsFinal).count())
#print(bugsDate('2019-09-06',bugsFinal).count())
#print(bugsDate('2019-08-09',bugsFinal).count())
#print(bugsDate('2019-07-15',bugsFinal).count())
#print(bugsDate('2019-06-26',bugsFinal).count())
#print(bugsDate('2019-05-09',bugsFinal).count())
#print(bugsDate('2019-04-05',bugsFinal).count())
#print(bugsDate('2019-03-06',bugsFinal).count())
#print(bugsDate('2019-02-07',bugsFinal).count())
#print(bugsDate('2019-01-08',bugsFinal).count())
#print(bugsDate('2018-10-19',bugsFinal).count())
#print(bugsDate('2018-09-12',bugsFinal).count())
#print(bugsDate('2018-08-14',bugsFinal).count())
#print(bugsDate('2018-07-27',bugsFinal).count())
#print(bugsDate('2018-06-21',bugsFinal).count())
#print(bugsDate('2018-05-15',bugsFinal).count())
#print(bugsDate('2018-04-02',bugsFinal).count())
#print(bugsDate('2017-12-19',bugsFinal).count())
#print(bugsDate('2017-11-15',bugsFinal).count())
#print(bugsDate('2017-10-17',bugsFinal).count())
#print(bugsDate('2017-08-23',bugsFinal).count())
#print(bugsDate('2017-05-30',bugsFinal).count())
#print(bugsDate('2017-03-20',bugsFinal).count())
#print(bugsDate('2017-02-28',bugsFinal).count())
#print(bugsDate('2017-02-16',bugsFinal).count())
#print(bugsDate('2017-02-14',bugsFinal).count())
#print(bugsDate('2016-01-19',bugsFinal).count())
#print(bugsDate('2015-09-01',bugsFinal).count())
#print(bugsDate('2015-10-14',bugsFinal).count())
#print(bugsDate('2015-11-10',bugsFinal).count())
#print(bugsDate('2015-12-08',bugsFinal).count())




#AVERAGE TIME

#print(getAverage('2020-03-19',bugsFinal))
#print(getAverage('2020-03-31',bugsFinal))
#print(getAverage('2020-01-17',bugsFinal))
#print(getAverage('2019-10-02',bugsFinal))
#print(getAverage('2019-09-06',bugsFinal))
#print(getAverage('2019-08-09',bugsFinal))
#print(getAverage('2019-07-15',bugsFinal))
#print(getAverage('2019-06-26',bugsFinal))
#print(getAverage('2019-05-09',bugsFinal))
#print(getAverage('2019-04-05',bugsFinal))
#print(getAverage('2019-03-06',bugsFinal))
#print(getAverage('2019-02-07',bugsFinal))
#print(getAverage('2019-01-08',bugsFinal))
#print(getAverage('2018-10-19',bugsFinal))
#print(getAverage('2018-09-12',bugsFinal))
#print(getAverage('2018-08-14',bugsFinal))
#print(getAverage('2018-07-27',bugsFinal))
#print(getAverage('2018-06-21',bugsFinal))
#print(getAverage('2018-05-15',bugsFinal))
#print(getAverage('2018-04-02',bugsFinal))
#print(getAverage('2017-12-19',bugsFinal))
#print(getAverage('2017-11-15',bugsFinal))
#print(getAverage('2017-10-17',bugsFinal))
#print(getAverage('2017-08-23',bugsFinal))
#print(getAverage('2017-05-30',bugsFinal))
#print(getAverage('2017-03-20',bugsFinal))
#print(getAverage('2017-02-28',bugsFinal))
#print(getAverage('2017-02-16',bugsFinal))
#print(getAverage('2017-02-14',bugsFinal))
#print(getAverage('2016-01-19',bugsFinal))
#print(getAverage('2015-09-01',bugsFinal))
#print(getAverage('2015-10-14',bugsFinal))
#print(getAverage('2015-11-10',bugsFinal))
#print(getAverage('2015-12-08',bugsFinal))




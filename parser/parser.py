from pyspark.sql.types import FloatType,StringType,IntegerType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
import pandas as pd
import numpy as np
import re
import requests
from datetime import datetime



def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL crime prediction functions") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def printDictionary(elem):
    for x in elem:
        print(x)
        for y in elem[x]:
            print(y, ':', elem[x][y])


def parserFile():
    f = open("/home/nellybett/PycharmProjects/ReengineeringProject/data/result", "r")
    content=f.read().split("commit ")
    commits=[]

    for x in content:
        arrayLines=x.strip().splitlines()

        if  len(arrayLines)>2 and (re.search('^Author', arrayLines[1].strip())):
            d = {}
            d["id"]=arrayLines[0]

            date=arrayLines[2].split(" ")
            formatDate=str(date[4])+" "+str(date[5])+", "+str(date[-2])
            finalDate=datetime.strptime(formatDate, '%b %d, %Y').strftime('%m/%d/%y')

            d["date"]=finalDate
            lastLine=arrayLines[-1]
            partes=lastLine.split(',')

            if len(partes)>1 and re.search('^insertions', partes[1].strip().split(" ")[1].strip()):
                d["insertion"]=int(partes[1].strip().split(" ")[0].strip())
            else:
                d["insertion"] =0

            if len(partes) > 1 and re.search('^deletion', partes[-1].strip().split(" ")[1].strip()):
                d["deletion"]=int(partes[-1].strip().split(" ")[0].strip())
            else:
                d["deletion"] =0

            files=int(str(lastLine.strip())[0])
            d["numberFiles"]=files

            i=0
            filesNames=""
            while i<files:
                add=0
                dele=0
                for x in arrayLines[-2 - i].split('|')[1].strip().split(" ")[-1]:
                    if x=="+" :
                       add=add+1
                    else :
                        dele=dele+1
                filesNames=filesNames+' '+str(arrayLines[-2-i].split('|')[0].strip())+' '+str(add)+" "+str(dele)+"$"
                i=i+1

            d["filesNames"]=filesNames.strip()
            #print(d)
            commits.append(d)
    #print(commits)
    #print(len(commits))
    return commits

def createDataForPeriod(commits,date_start,date_end):
    dataFrame=pd.DataFrame(commits)
    dataFrame['date'] = pd.to_datetime(dataFrame['date'])
    mask = (dataFrame['date'] > date_start) & (dataFrame['date'] <= date_end)

    return dataFrame.loc[mask]

def average(dataF):
    return dataF['numberFiles'].mean(axis = 0)

def insertions(dataF):
    return dataF['insertion'].sum()

def deletions(dataF):
    return dataF['deletion'].sum()

def prob(dataF):

    if not dataF.empty:
        dataFrameFiles=pd.DataFrame({'count': dataF.groupby(['numberFiles']).size()}).reset_index()

        mask = (dataFrameFiles['numberFiles'] == 1)
        valueOfOnes=dataFrameFiles.loc[mask]['count']

        if len(valueOfOnes)==0:
            valueOfOne=dataF['numberFiles'].count()
        else:
            print("entre")
            valueOfOne=valueOfOnes.iloc[0]
        totalFiles=dataF['numberFiles'].count()
        prob=valueOfOne/totalFiles
    else:
        prob=0
    return 1-prob

def listProb(dataF1,dataF2,dataF3,dataF4,dataF5,dataF6):
    listProb=[]
    listProb.append(prob(dataF1))
    listProb.append(prob(dataF2))
    listProb.append(prob(dataF3))
    listProb.append(prob(dataF4))
    listProb.append(prob(dataF5))
    listProb.append(prob(dataF6))

    return listProb

def numberChangesModule(module,dataF):
    counterChanges=0
    dataFrame=dataF['filesNames']
    files = dataFrame.tolist()
    j=0
    for file in files:
        elements=file.split('$')
        i=0
        while i< len(elements):
            if(elements[i]!=""):
                oneFile=elements[i].strip().split(" ")
                #print(oneFile[0])
            i=i+1;
        j=j+1
    print(j)
    return 0





# Versions to study

dataFrameVersion1=createDataForPeriod(parserFile(),'2014-02-01','2020-03-19')
dataFrameVersion2=createDataForPeriod(parserFile(),'2014-02-01','2020-03-31')
dataFrameVersion3=createDataForPeriod(parserFile(),'2014-02-01','2020-01-17')
dataFrameVersion4=createDataForPeriod(parserFile(),'2014-02-01','2019-10-02')
dataFrameVersion5=createDataForPeriod(parserFile(),'2014-02-01','2019-09-06')
dataFrameVersion6=createDataForPeriod(parserFile(),'2014-02-01','2019-08-09')
dataFrameVersion7=createDataForPeriod(parserFile(),'2014-02-01','2019-07-15')
dataFrameVersion8=createDataForPeriod(parserFile(),'2014-02-01','2019-06-26')
dataFrameVersion9=createDataForPeriod(parserFile(),'2014-02-01','2019-05-09')
dataFrameVersion10=createDataForPeriod(parserFile(),'2014-02-01','2019-04-05')
dataFrameVersion11=createDataForPeriod(parserFile(),'2014-02-01','2019-03-06')
dataFrameVersion12=createDataForPeriod(parserFile(),'2014-02-01','2019-02-07')
dataFrameVersion13=createDataForPeriod(parserFile(),'2014-02-01','2019-01-08')
dataFrameVersion14=createDataForPeriod(parserFile(),'2014-02-01','2018-10-19')
dataFrameVersion15=createDataForPeriod(parserFile(),'2014-02-01','2018-09-12')
dataFrameVersion16=createDataForPeriod(parserFile(),'2014-02-01','2018-08-14')
dataFrameVersion17=createDataForPeriod(parserFile(),'2014-02-01','2018-07-27')
dataFrameVersion18=createDataForPeriod(parserFile(),'2014-02-01','2018-06-21')
dataFrameVersion19=createDataForPeriod(parserFile(),'2014-02-01','2018-05-15')
dataFrameVersion20=createDataForPeriod(parserFile(),'2014-02-01','2018-04-02')
dataFrameVersion21=createDataForPeriod(parserFile(),'2014-02-01','2017-12-19')
dataFrameVersion22=createDataForPeriod(parserFile(),'2014-02-01','2017-11-15')
dataFrameVersion23=createDataForPeriod(parserFile(),'2014-02-01','2017-10-17')
dataFrameVersion24=createDataForPeriod(parserFile(),'2014-02-01','2017-08-23')
dataFrameVersion25=createDataForPeriod(parserFile(),'2014-02-01','2017-05-30')
dataFrameVersion26=createDataForPeriod(parserFile(),'2014-02-01','2017-03-20')
dataFrameVersion27=createDataForPeriod(parserFile(),'2014-02-01','2017-02-28')
dataFrameVersion28=createDataForPeriod(parserFile(),'2014-02-01','2017-02-16')
dataFrameVersion29=createDataForPeriod(parserFile(),'2014-02-01','2017-02-14')
dataFrameVersion30=createDataForPeriod(parserFile(),'2014-02-01','2016-01-19')
dataFrameVersion31=createDataForPeriod(parserFile(),'2014-02-01','2015-09-01')
dataFrameVersion32=createDataForPeriod(parserFile(),'2014-02-01','2015-10-14')
dataFrameVersion33=createDataForPeriod(parserFile(),'2014-02-01','2015-11-10')
dataFrameVersion34=createDataForPeriod(parserFile(),'2014-02-01','2015-12-08')


#NUMBER OF CHANGES

#print(dataFrameVersion1.count())
#print(dataFrameVersion2.count())
#print(dataFrameVersion3.count())
#print(dataFrameVersion4.count())
#print(dataFrameVersion5.count())
#print(dataFrameVersion6.count())
#print(dataFrameVersion7.count())
#print(dataFrameVersion8.count())
#print(dataFrameVersion9.count())
#print(dataFrameVersion10.count())
#print(dataFrameVersion11.count())
#print(dataFrameVersion12.count())
#print(dataFrameVersion13.count())
#print(dataFrameVersion14.count())
#print(dataFrameVersion15.count())
#print(dataFrameVersion16.count())
#print(dataFrameVersion17.count())
#print(dataFrameVersion18.count())
#print(dataFrameVersion19.count())
#print(dataFrameVersion20.count())
#print(dataFrameVersion21.count())
#print(dataFrameVersion22.count())
#print(dataFrameVersion23.count())
#print(dataFrameVersion24.count())
#print(dataFrameVersion25.count())
#print(dataFrameVersion26.count())
#print(dataFrameVersion27.count())
#print(dataFrameVersion28.count())
#print(dataFrameVersion29.count())
#print(dataFrameVersion30.count())
#print(dataFrameVersion31.count())
#print(dataFrameVersion32.count())
#print(dataFrameVersion33.count())
#print(dataFrameVersion34.count())

#Average of changed files

#print(average(dataFrameVersion1))
#print(average(dataFrameVersion2))
#print(average(dataFrameVersion3))
#print(average(dataFrameVersion4))
#print(average(dataFrameVersion5))
#print(average(dataFrameVersion6))
#print(average(dataFrameVersion7))
#print(average(dataFrameVersion8))
#print(average(dataFrameVersion9))
#print(average(dataFrameVersion10))
#print(average(dataFrameVersion11))
#print(average(dataFrameVersion12))
#print(average(dataFrameVersion13))
#print(average(dataFrameVersion14))
#print(average(dataFrameVersion15))
#print(average(dataFrameVersion16))
#print(average(dataFrameVersion17))
#print(average(dataFrameVersion18))
#print(average(dataFrameVersion19))
#print(average(dataFrameVersion20))
#print(average(dataFrameVersion21))
#print(average(dataFrameVersion22))
#print(average(dataFrameVersion23))
#print(average(dataFrameVersion24))
#print(average(dataFrameVersion25))
#print(average(dataFrameVersion26))
#print(average(dataFrameVersion27))
#print(average(dataFrameVersion28))
#print(average(dataFrameVersion29))
#print(average(dataFrameVersion30))
#print(average(dataFrameVersion31))
#print(average(dataFrameVersion32))
#print(average(dataFrameVersion33))
#print(average(dataFrameVersion34))


#INSERTIONS

#print(insertions(dataFrameVersion1))
#print(insertions(dataFrameVersion2))
#print(insertions(dataFrameVersion3))
#print(insertions(dataFrameVersion4))
#print(insertions(dataFrameVersion5))
#print(insertions(dataFrameVersion6))
#print(insertions(dataFrameVersion7))
#print(insertions(dataFrameVersion8))
#print(insertions(dataFrameVersion9))
#print(insertions(dataFrameVersion10))
#print(insertions(dataFrameVersion11))
#print(insertions(dataFrameVersion12))
#print(insertions(dataFrameVersion13))
#print(insertions(dataFrameVersion14))
#print(insertions(dataFrameVersion15))
#print(insertions(dataFrameVersion16))
#print(insertions(dataFrameVersion17))
#print(insertions(dataFrameVersion18))
#print(insertions(dataFrameVersion19))
#print(insertions(dataFrameVersion20))
#print(insertions(dataFrameVersion21))
#print(insertions(dataFrameVersion22))
#print(insertions(dataFrameVersion23))
#print(insertions(dataFrameVersion24))
#print(insertions(dataFrameVersion25))
#print(insertions(dataFrameVersion26))
#print(insertions(dataFrameVersion27))
#print(insertions(dataFrameVersion28))
#print(insertions(dataFrameVersion29))
#print(insertions(dataFrameVersion30))
#print(insertions(dataFrameVersion31))
#print(insertions(dataFrameVersion32))
#print(insertions(dataFrameVersion33))
#print(insertions(dataFrameVersion34))

#DELETIONS

#print(deletions(dataFrameVersion1))
#print(deletions(dataFrameVersion2))
#print(deletions(dataFrameVersion3))
#print(deletions(dataFrameVersion4))
#print(deletions(dataFrameVersion5))
#print(deletions(dataFrameVersion6))
#print(deletions(dataFrameVersion7))
#print(deletions(dataFrameVersion8))
#print(deletions(dataFrameVersion9))
#print(deletions(dataFrameVersion10))
#print(deletions(dataFrameVersion11))
#print(deletions(dataFrameVersion12))
#print(deletions(dataFrameVersion13))
#print(deletions(dataFrameVersion14))
#print(deletions(dataFrameVersion15))
#print(deletions(dataFrameVersion16))
#print(deletions(dataFrameVersion17))
#print(deletions(dataFrameVersion18))
#print(deletions(dataFrameVersion19))
#print(deletions(dataFrameVersion20))
#print(deletions(dataFrameVersion21))
#print(deletions(dataFrameVersion22))
#print(deletions(dataFrameVersion23))
#print(deletions(dataFrameVersion24))
#print(deletions(dataFrameVersion25))
#print(deletions(dataFrameVersion26))
#print(deletions(dataFrameVersion27))
#print(deletions(dataFrameVersion28))
#print(deletions(dataFrameVersion29))
#print(deletions(dataFrameVersion30))
#print(deletions(dataFrameVersion31))
#print(deletions(dataFrameVersion32))
#print(deletions(dataFrameVersion33))
#print(deletions(dataFrameVersion34))


#prob(dataFrameVersion1)
#print(listProb(dataFrameVersion1,dataFrameVersion2,dataFrameVersion3,dataFrameVersion4,dataFrameVersion5,dataFrameVersion6))
#print(average(dataFrameVersion1))
#print(average(dataFrameVersion2))
#print(average(dataFrameVersion3))
#print(average(dataFrameVersion4))
#print(average(dataFrameVersion5))
#print(average(dataFrameVersion6))

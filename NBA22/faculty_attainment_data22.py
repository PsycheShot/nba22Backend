import pandas as pd
from pymongo import MongoClient
import pprint as pp
import numpy as np
import itertools
import json
import DbAccess
import itertools
from flask import jsonify
from bson import json_util
from NBA21 import  faculty_attainment_data
dbdetails = DbAccess.details()
myclient = MongoClient(dbdetails["host"],dbdetails["port"])
db = myclient[dbdetails["dbName"]]
db.authenticate(dbdetails["user"], dbdetails["password"])


#maps the blooms level to all the CO's in the received coursecode. This is a helper function
def get_bloomslevel_with_co(x):
    pipeline=    [
    {
        '$match': {
            'courseCode': x['courseCode'], 
            'academicYear': x['year'], 
            'faculties.facultyGivenId': x['facultyId'], 
            'departments.termNumber': x['termNumber']
        }
    }, {
        '$unwind': {
            'path': '$plan'
        }
    }, {
        '$unwind': {
            'path': '$plan.couseOutcomes'
        }
    }, {
        '$unwind': {
            'path': '$departments'
        }
    }, {
        '$unwind': {
            'path': '$faculties'
        }
    }, {
        '$project': {
            '_id': 0, 
            'bloomsLevel': '$plan.bloomsLevel', 
            'coNumber': '$plan.couseOutcomes', 
            'courseName': 1, 
            'courseCode': 1, 
            'termNumber': '$departments.termNumber', 
            'section': '$departments.section', 
            'facultyId': '$faculties.facultyId',
            'academicYear': 1
        }
    }
    ] 
    
    mapping=db.dhi_lesson_plan.aggregate(pipeline);
    docs=[doc for doc in mapping]
    details=json.dumps(docs,default=json_util.default)

    df=pd.DataFrame(docs)
    if df.empty:
        print("No data")
        return []
    lst=[]
    if "bloomsLevel" in df.columns:
        df["bloomsLevel"]=df["bloomsLevel"].fillna("EMPTY")
        bloomap={"EMPTY":0,"UNDERSTAND":2,"REMEMBER":1,"APPLY":3,"ANALYZE":4,"EVALUATE":5,"CREATE":6}
        bloominverse={1:"REMEMBER",2:"UNDERSTAND",3:"APPLY",4:"ANALYZE",5:"EVALUATE",6:"CREATE",0:"EMPTY"}
        res_table={}
        for i in range(df["coNumber"].min(),df["coNumber"].max()+1):
            df1=df.loc[df["coNumber"]==i]
            if(~df1.empty):
                df1=df1.reset_index(drop=True)
                for j in range(len(df1)):
                    if(df1.loc[j]["bloomsLevel"]!="EMPTY"):
                        lst.append(bloomap[df1.loc[j]["bloomsLevel"]])
                if len(lst)==0 :
                    average=0
                else:
                    average=int(round(sum(lst)/len(lst)))
                res_table[df1["coNumber"][0]]=bloominverse[average]
    
        resd= pd.DataFrame(list(res_table.items()),columns = ['coNumber','BloomsLevel'])
        resd["courseCode"]=df["courseCode"][0]
        resd["section"]=x["section"]
        res_j=json.loads(resd.reset_index().to_json(orient="records"))
        return res_j
    else:
        print("data not avail")
        return []
    
#sends back the CO's bloomsLevel mapped for the facultyId,year and term list
def get_map_blooms_to_co(facultyId,termList,year,cCode):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyGivenId':facultyId,
            'academicYear':year,
            'departments.termNumber':{'$in':termList},
            'courseCode':cCode
            }
        },
        {
        '$project':
            {
                '_id':0,
                'courseCode':1,
                'courseName':1,
                'termNumber':'$departments.termNumber',
                'section':'$departments.section',
                'facultyId':'$faculties.facultyId',
                'year':'$academicYear',
            } 
        }
        ]
    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    details=json.dumps(docs,default=json_util.default)
    df=pd.DataFrame(docs)
    print(df)
    blooms_level_mapping=[]
    if df.empty:
        return blooms_level_mapping
    blooms_level_mapping=[get_bloomslevel_with_co(df.loc[i]) for i in range(len(df))]
    return blooms_level_mapping

#sends back the indivudual CO's indirect attainment,direct attainment and tota attainment for the center graph
# uses the get_required_attainment_detail helper function from NBA21.faculty_attaimnet_data.py
def rwd_individual_attainment_data(facultyId,termList,year):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyId':facultyId,
            'academicYear':year,
            'departments.termNumber':{'$in':termList}
            
            }
        },
        {
        '$project':
            {
                '_id':0,
                'courseCode':1,
                'courseName':1,
                'termNumber':'$departments.termNumber',
                'section':'$departments.section',
                'facultyId':'$faculties.facultyId',
                'year':'$academicYear',
                'deptId':'$departments.deptId'
            } 
        }
        ]

    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    df=pd.DataFrame(docs)
    attainment_data = [faculty_attainment_data.get_required_attainment_detail(df.loc[i]) for i in range(len(df))]
    attainment = list(itertools.chain.from_iterable(attainment_data))
    df2 = pd.DataFrame(data=attainment, columns=['coNumber','coTitle','termNumber','section','courseCode',
                                           'year','facultyId','totalAttainment','directAttainment','indirectAttainment','deptId'])
    if df2.empty:
        return attainment_data
    df2['totalAttainment'] = df2['totalAttainment'].round(decimals=2)
    df2['directAttainment'] = df2['directAttainment'].round(decimals=2)
    df2['indirectAttainment'] = df2['indirectAttainment'].round(decimals=2)
    attainment_details = json.loads(df2.to_json(orient='records'))
    return attainment_details

def get_co_description(facultyId,termList,year,cCode):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyId':facultyId,
            'academicYear':year,
            'departments.termNumber':{'$in':termList},
            'courseCode':cCode,
            }
        },
        {
        '$project':
            {
                '_id':0,
                'courseCode':1,
                'courseName':1,
                'termNumber':'$departments.termNumber',
                'section':'$departments.section',
                'facultyId':'$faculties.facultyId',
                'year':'$academicYear',
            } 
        }
        ]

    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    details=json.dumps(docs,default=json_util.default)
    df=pd.DataFrame(docs)
    attainment_data = [faculty_attainment_data.get_attainment_details(df.loc[i]) for i in range(len(df))]
    attainment = list(itertools.chain.from_iterable(attainment_data))
    df2 = pd.DataFrame(data=attainment, columns=['coNumber','coTitle','termNumber','section','courseCode',
                                           'year','facultyId','dir_methodName','dir_methodDescription','dir_attainment',
                                            'dir_attainmentPercentage','indir_attainment','indir_attainmentPercentage',
                                           'indir_methodName','indir_methodDescription','totalAttainment','directAttainment','indirectAttainment'])
    df2['totalAttainment'] = df2['totalAttainment'].round(decimals=2)
    df2['directAttainment'] = df2['directAttainment'].round(decimals=2)
    df2['indirectAttainment'] = df2['indirectAttainment'].round(decimals=2)
    attainment_details = json.loads(df2.to_json(orient='records'))
    return attainment_details
#page 1 graph data. Sends back the average co attainment values of the particular terms,year and FID

def get_individual_attainment_data(facultyId,termList,year,cCode):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyId':facultyId,
            'academicYear':year,
            'departments.termNumber':{'$in':termList},
            'courseCode':cCode
            }
        },
        {
        '$project':
            {
                '_id':0,
                'courseCode':1,
                'courseName':1,
                'termNumber':'$departments.termNumber',
                'section':'$departments.section',
                'facultyId':'$faculties.facultyId',
                'year':'$academicYear',
            } 
        }
        ]

    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    details=json.dumps(docs,default=json_util.default)
    df=pd.DataFrame(docs)
    attainment_data = [faculty_attainment_data.get_required_attainment_detail(df.loc[i]) for i in range(len(df))]
    attainment = list(itertools.chain.from_iterable(attainment_data))
    df2 = pd.DataFrame(data=attainment, columns=['coNumber','coTitle','termNumber','section','courseCode',
                                           'year','facultyId','totalAttainment','directAttainment','indirectAttainment'])
    if df2.empty:
        return attainment_data
    df2['totalAttainment'] = df2['totalAttainment'].round(decimals=2)
    df2['directAttainment'] = df2['directAttainment'].round(decimals=2)
    df2['indirectAttainment'] = df2['indirectAttainment'].round(decimals=2)
    attainment_details = json.loads(df2.to_json(orient='records'))
    return attainment_details


def get_average_co_attainment(facultyId,termList,year):
    pipeline=pipeline=[
    {
        '$unwind': {
            'path': '$faculties'
        }
    }, {
        '$unwind': {
            'path': '$courseDetails'
        }
    }, {
        '$match': {
            'termNumber': {'$in':termList}, 
            'faculties.facultyId': facultyId, 
            'academicYear': year
        }
    }, {
        '$unwind': {
            'path': '$courseOutcomeDetailsForAttainment'
        }
    }, {
        '$project': {
            '_id': 0, 
            'coNumber': '$courseOutcomeDetailsForAttainment.coNumber', 
            'coTitle': '$courseOutcomeDetailsForAttainment.coTitle', 
            'termNumber': '$termNumber', 
            'section': '$section', 
            'courseCode': '$courseDetails.courseCode',
            'courseName':'$courseDetails.courseName',
            'deptId':'$deptId', 
            'year': '$academicYear', 
            'facultyId': '$faculties.facultyId', 
            'totalAttainment': '$courseOutcomeDetailsForAttainment.totalAttainment'
        }
    }
    ]

    mapping=db.dhi_generic_attainment_data.aggregate(pipeline)
    docs=[doc for doc in mapping]
    details=json.dumps(docs,default=json_util.default)
    df=pd.DataFrame(docs)
    if df.empty:
        return []
    df2=df.groupby(["termNumber","section","courseCode","year","courseName","deptId"])
    average_attainment_data = list(df2['totalAttainment'].mean())
    total_average_attainment = []

    for k, v in df2:
        data_ = {}
        data_['termNumber'] = k[0]
        data_['courseCode'] = k[2]
        data_['section'] = k[1]
        data_['facultyId'] = k[3]
        data_['courseName'] = k[4]
        data_['deptId'] = k[5]
        data_['average_attainment'] = round(v['totalAttainment'].mean(),2)
        total_average_attainment.append(data_)

    docs=[doc for doc in total_average_attainment]
    # details=json.dumps(docs,default=json_util.default)
    return (docs)

# Functions for RWD version ################################################################################## 

def rwd_map_blooms_to_co(facultyId,termList,year):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyGivenId':facultyId,
            'academicYear':year,
            'departments.termNumber':{'$in':termList}
            }
        },
        {
        '$project':
            {
                '_id':0,
                'courseCode':1,
                'courseName':1,
                'termNumber':'$departments.termNumber',
                'section':'$departments.section',
                'facultyId':'$faculties.facultyId',
                'year':'$academicYear',
            } 
        }
        ]
    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    details=json.dumps(docs,default=json_util.default)
    df=pd.DataFrame(docs)
    blooms_level_mapping=[]
    if df.empty:
        return blooms_level_mapping
    blooms_level_mapping=[get_bloomslevel_with_co(df.loc[i]) for i in range(len(df))]
    return blooms_level_mapping



def combine_bloom(facultyId,termList,year):
    temp=rwd_map_blooms_to_co(facultyId,termList,year)
    df1=[ pd.DataFrame(temp[i]) for i in range(len(temp))]
    df2=pd.concat(df1)
    df2=df2.reset_index(drop=True)
    df2.pop("index")
    df=pd.DataFrame(rwd_individual_attainment_data(facultyId,termList,year))
    df["bloomsLevel"]="EMPTY"
    for i in range(len(df)):
        for j in range(len(df2)):
            if(df2.loc[j]['courseCode']==df.loc[i]['courseCode'] and df2.loc[j]['section']==df.loc[i]['section'] and df2.loc[j]['coNumber']==df.loc[i]['coNumber']):
                df.at[i,"bloomsLevel"]=df2.loc[j]["BloomsLevel"]
                continue
    df3=df[df["bloomsLevel"]!="EMPTY"]
    return json.loads(df3.to_json(orient='records'))
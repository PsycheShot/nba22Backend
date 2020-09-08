import pandas as pd
from pymongo import MongoClient
import pprint as pp
import numpy as np
import itertools
import json
import DbAccess
from flask import jsonify
from bson import json_util
from NBA22 import faculty_attainment_data22
from NBA21 import faculty_attainment_data
dbdetails = DbAccess.details()
myclient = MongoClient(dbdetails["host"],dbdetails["port"])
db = myclient[dbdetails["dbName"]]
db.authenticate(dbdetails["user"], dbdetails["password"])

def hodDetails(facultyId, academicYear, termNumber):
    hodDepartment = [x for x in db.dhi_user.aggregate([
        {"$match": {
            "employeeGivenId": facultyId,
            # "academicYear": academicYear, 
            #"degreeId": degreeId
            }
            },
        {"$unwind": "$handlingDegreeAndDepartments"},
        {"$unwind": "$handlingDegreeAndDepartments.handlingDepartments"},

        {"$project": {
            "_id": 0,
            "deptName": "$handlingDegreeAndDepartments.handlingDepartments.deptName",
            "deptId": "$handlingDegreeAndDepartments.handlingDepartments.deptId"}}
    ])]
    if hodDepartment !=[]:
        return (hodSubject(academicYear, termNumber,
                       hodDepartment[0]["deptId"]))


def hodSubject(academicYear, termNumber, department):
    # termNumber = list(termNumber.split(","))
    # pprint(termNumber)
    course = [subjects for subjects in db.dhi_lesson_plan.aggregate([
        {'$match': {
            'academicYear': academicYear,
            'departments.termNumber': {'$in': termNumber},
            # 'degreeId': degreeId,
            'departments.deptId': department
        }
        },
        {"$unwind": "$faculties"},
        {"$unwind": "$departments"},
        {'$project': {
            '_id': 0,

            'facultyId': '$faculties.facultyId',

        }}

    ])]
    facultylist = []
    for x in course:
        if "facultyId" in x:
            if x["facultyId"] not in facultylist:
                facultylist.append(x["facultyId"])
    finalanswer = []
    final = get_average_co_attainment_hod(facultylist,termNumber,academicYear,department)
    return final


def sorting_data(data):
    details = []
    [details.append(i['_id']) for i in data]
    df = pd.DataFrame(data=details)
    df1 = df.sort_values(by=['facultyName'])
    df2 = df1.drop_duplicates(subset = ['facultyId','termNumber'])
    return df2

def get_overall_attainment_data(facultyIdList,termList,year,department):
    overall_attainmnet_details = {}
    course = []
    courses = db.dhi_lesson_plan.aggregate([
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyId':{'$in':facultyIdList},
            'academicYear':year,
            'departments.termNumber':{'$in':termList},
            'departments.deptId': department

            }
        },
        {
        '$group': 
            {
                '_id': {
                'courseCode':"$courseCode",
                'courseName':"$courseName",
                'termNumber':'$departments.termNumber',
                'section':'$departments.section',
                'facultyId':'$faculties.facultyId',
                'facultyGivenId':'$faculties.facultyGivenId',
                'facultyName':'$faculties.facultyName',
                'year':'$academicYear',
                'deptId':'$departments.deptId',
            } 
        }
        },
        ])
    data = sorting_data(list(courses))
    data_ = json.loads(data.to_json(orient='records'))
    print(data_)
    return data_


def get_hod_dept(email):
    pipeline= [
   
    {
        '$unwind': {
            'path': '$roles'
        }
    }, {
        '$match': {
            'roles.roleName': 'HOD', 
            'email': email
        }
    }, {
        '$project': {
            'deptId': 1, 
            '_id': 0
        }
    }

]

    mapping=db.dhi_user.aggregate(pipeline)
    docs=[doc for doc in mapping]
    #details=json.dumps(docs,default=json_util.default)
    return(docs)

def get_term_hod_dept(deptId,year):
    pipeline=[
    {
        '$match': {
            'academicYear': year 
        }
    }, {
        '$unwind': {
            'path': '$faculties'
        }
    }, {
        '$match': {
            'departments.deptId': deptId
        }
    }, {
        '$unwind': {
            'path': '$departments'
        }
    }, {
        '$group': {
            '_id': '$departments.termNumber'
        }
    }
]

    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    termList = [term['_id'] for term in docs]
    termList = sorted(termList)
    
    #details=json.dumps(docs,default=json_util.default)
    return(termList)

def getHodIdUsingDept(deptId):
    pipeline = [
        {
            '$unwind': {
                'path': '$roles'
            }
        }, {
            '$match': {
                'roles.roleName': 'HOD', 
                'deptId': deptId
            }
        }, {
            '$project': {
                'employeeGivenId': 1, 
                '_id': 0
            }
        }
    ]
    mapping=db.dhi_user.aggregate(pipeline)
    docs=[doc for doc in mapping]
    return docs
    
def get_average_co_attainment_hod(facultyId,termList,year,department):
    pipeline=[
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
            'faculties.facultyId': {'$in':facultyId}, 
            'academicYear': year,
            'deptId':department
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
            'facultyName':'$faculties.facultyName',
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
    df2=df.groupby(["termNumber","section","courseCode","facultyId","courseName","deptId","facultyName"])
    average_attainment_data = list(df2['totalAttainment'].mean())
    total_average_attainment = []
    print(df2)

    for k, v in df2:
        data_ = {}
        data_['termNumber'] = k[0]
        data_['courseCode'] = k[2]
        data_['section'] = k[1]
        data_['facultyId'] = k[3]
        data_['courseName'] = k[4]
        data_['deptId'] = k[5]
        data_['facultyName'] = k[6]
        data_['average_attainment'] = round(v['totalAttainment'].mean(),2)
        total_average_attainment.append(data_)

    docs=[doc for doc in total_average_attainment]
    # details=json.dumps(docs,default=json_util.default)
    return (docs)

###################################################################################

def rwd_map_blooms_to_co(facultyId,termList,year):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyGivenId':{'$in':facultyId},
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
    pd.set_option('display.max_columns', None)
    #print(df)
    blooms_level_mapping=[]
    if df.empty:
        return blooms_level_mapping
    # blooms_level_mapping=[get_bloomslevel_with_co(df.loc[i]) for i in range(len(df))]
    for i in range(len(df)):
      temp = faculty_attainment_data22.get_bloomslevel_with_co(df.loc[i])
      #print(temp)
      if (temp == []):
        continue
      else:
        blooms_level_mapping.append(temp)
      
    return blooms_level_mapping

def rwd_individual_attainment_data(facultyId,termList,year):
    attainment_data = []
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyId':{'$in':facultyId},
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
    # attainment_data = [get_required_attainment_detail(df.loc[i]) for i in range(len(df))]
    for i in range(len(df)):
      temp = faculty_attainment_data.get_required_attainment_detail(df.loc[i])
      if (temp == []):
        continue
      else:
        attainment_data.append(temp)
    

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

def combine_bloom(facultyId,termList,year):
    df1=[ pd.DataFrame(rwd_map_blooms_to_co(facultyId,termList,year)[i]) for i in range(len(rwd_map_blooms_to_co(facultyId,termList,year)))]
    # print(df1)
    if(len(df1)==0):
        return []
    df2=pd.concat(df1)                                                                                     
    df2=df2.reset_index(drop=True)
    df2.pop("index")
    df2.pop("courseCode")
    # df2.pop("coNumber")
    # print(df2)
    df=pd.DataFrame(rwd_individual_attainment_data(facultyId,termList,year))
    #print(df)
    # print(df2["BloomsLevel"][0])
    df["BloomsLevel"]=''
    #print(df2)
    for i in range(len(df)):
      for j in range(len(df2)):
        if (df["coNumber"][i]==df2["coNumber"][j]):
          df["BloomsLevel"][i] = df2["BloomsLevel"][j]
          break
        else:
          df["BloomsLevel"][i] = "EMPTY"
    
    df = df[df.BloomsLevel!="EMPTY"]
    #print(df)

    return json.loads(df.to_json(orient='records'))

    ############################################################################
    
    
    ##### NEW PIPELINE ########

def rwd_bloomslevel_with_co(x):
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
        resd["section"]=x['section']
        res_j=json.loads(resd.reset_index().to_json(orient="records"))
        return res_j
    else:
        print("data not avail")
        return []

def rwd_bloom_chart_HOD(facultyList,year,termList,deptId):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyGivenId':{'$in':facultyList},
            'academicYear':year,
            'departments.termNumber':{'$in':termList},
             'departments.deptId':deptId
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
                'deptId':"$departments.deptId"
            } 
        }
        ]
    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    df=pd.DataFrame(docs)
    df1=df.sort_values(by=["courseCode","section","termNumber"])
    blooms_level_mapping=[rwd_bloomslevel_with_co(df1.loc[i]) for i in range(len(df1))]
    blooms_level_mapping
    return blooms_level_mapping



def hod_get_required_attainment_detail(x):
    attaiment_data = db.dhi_generic_attainment_data.aggregate([
    {'$unwind':'$courseDetails'},
    {'$unwind':'$faculties'},
    {'$match':{
        'termNumber':x['termNumber'],
        'section':x['section'],
        'courseDetails.courseCode':x['courseCode'],
        'faculties.facultyId':x['facultyId'],
        'year':x['year'],
        'deptId':x['deptId']
      }    
    },
        
    {'$unwind':'$courseOutcomeDetailsForAttainment'},

    {
      '$project':
        {
        '_id':0,
        'coNumber':'$courseOutcomeDetailsForAttainment.coNumber',
        'coTitle':'$courseOutcomeDetailsForAttainment.coTitle',
        'termNumber':'$termNumber',
        'section':'$section',
        'courseCode':'$courseDetails.courseCode',
        'courseName':'$courseDetails.courseName',
        'year':'$academicYear',
        'facultyId':'$faculties.facultyId',
        'deptId':'$deptId',
        'totalAttainment':'$courseOutcomeDetailsForAttainment.totalAttainment',
        'directAttainment':'$courseOutcomeDetailsForAttainment.directAttainment',
        'indirectAttainment':'$courseOutcomeDetailsForAttainment.indirectAttainment',
        'facultyName':'$faculties.facultyName'
        }
    }
    ])
    return list(attaiment_data)



def hod_rwd_individual_attainment_details(faclist,year,termList,deptId):
    pipeline1=[
            {'$unwind':'$faculties'},
            {'$unwind':'$departments'},
            {'$match':
                {
                'faculties.facultyId':{'$in':faclist},
                'academicYear':year,
                'departments.termNumber':{'$in':termList},
                'departments.deptId':deptId
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

    mapping=db.dhi_lesson_plan.aggregate(pipeline1)
    docs=[doc for doc in mapping]

    df=pd.DataFrame(docs)
    df1=df.sort_values(by=["courseCode","section","termNumber","facultyId"])
    attainment_data = [hod_get_required_attainment_detail(df1.loc[i]) for i in range(len(df1))]
    attainment = list(itertools.chain.from_iterable(attainment_data))
    df2 = pd.DataFrame(data=attainment, columns=['coNumber','coTitle','termNumber','section','courseCode',
                                            'year','facultyId','totalAttainment','directAttainment','indirectAttainment','facultyName'])
    df2['totalAttainment'] = df2['totalAttainment'].round(decimals=2)
    df2['directAttainment'] = df2['directAttainment'].round(decimals=2)
    df2['indirectAttainment'] = df2['indirectAttainment'].round(decimals=2)
    attainment_details = json.loads(df2.to_json(orient='records'))
    attainment_details
    return attainment_details


def hod_combine_bloom(facList,year,termList,deptId):
    temp=rwd_bloom_chart_HOD(facList,year,termList,deptId)
    df1=[ pd.DataFrame(temp[i]) for i in range(len(temp))]
    df2=pd.concat(df1)   
    df2=df2.reset_index(drop=True)
    df2.pop("index")
    df2
    df=pd.DataFrame(hod_rwd_individual_attainment_details(facList,year,termList,deptId))
    df["bloomsLevel"]="EMPTY"
    for i in range(len(df)):
        for j in range(len(df2)):
            if(df2.loc[j]['courseCode']==df.loc[i]['courseCode'] and df2.loc[j]['section']==df.loc[i]['section'] and df2.loc[j]['coNumber']==df.loc[i]['coNumber']):
                df.at[i,"bloomsLevel"]=df2.loc[j]["BloomsLevel"]
                continue
    df3=df[df["bloomsLevel"]!="EMPTY"]
    return json.loads(df3.to_json(orient='records'))





def hod_web_individual_attainment_details(facultyId,year,termList,deptId,cCode):
    pipeline=[
            {'$unwind':'$faculties'},
            {'$unwind':'$departments'},
            {'$match':
                {
                'faculties.facultyId':facultyId,
                'academicYear':year,
                'departments.termNumber':{'$in':termList},
                'courseCode':cCode,
                'departments.deptId':deptId
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
    details=json.dumps(docs,default=json_util.default)
    df=pd.DataFrame(docs)
    attainment_data = [hod_get_required_attainment_detail(df.loc[i]) for i in range(len(df))]
    attainment = list(itertools.chain.from_iterable(attainment_data))
    df2 = pd.DataFrame(data=attainment, columns=['coNumber','coTitle','termNumber','section','courseCode',
                                            'year','facultyId','totalAttainment','directAttainment','indirectAttainment','deptId'])
    df2['totalAttainment'] = df2['totalAttainment'].round(decimals=2)
    df2['directAttainment'] = df2['directAttainment'].round(decimals=2)
    df2['indirectAttainment'] = df2['indirectAttainment'].round(decimals=2)
    attainment_details = json.loads(df2.to_json(orient='records'))
    attainment_details
    return attainment_details

def web_bloom_chart_HOD(facultyId,year,termList,deptId,cCode):
    pipeline=[
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyGivenId':facultyId,
            'academicYear':year,
            'departments.termNumber':{'$in':termList},
             'departments.deptId':deptId,
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
                'deptId':"$departments.deptId"
            } 
        }
        ]
    mapping=db.dhi_lesson_plan.aggregate(pipeline)
    docs=[doc for doc in mapping]
    df=pd.DataFrame(docs)
    if df.empty:
        return []
    df1=df.sort_values(by=["courseCode","section","termNumber"])
    blooms_level_mapping=[rwd_bloomslevel_with_co(df1.loc[i]) for i in range(len(df1))]
    blooms_level_mapping
    return blooms_level_mapping

def web_combine_bloom_hod(facultyId,year,termList,deptId,cCode):
    temp=web_bloom_chart_HOD(facultyId,year,termList,deptId,cCode)
    df1=[ pd.DataFrame(temp[i]) for i in range(len(temp))]
    if len(df1)==0:
        return []
    df2=pd.concat(df1)
    if df2.empty:
        return []
    df2=df2.reset_index(drop=True)
    df2.pop("index")
    df=pd.DataFrame(hod_web_individual_attainment_details(facultyId,year,termList,deptId,cCode))
    df["bloomsLevel"]="EMPTY"
    for i in range(len(df)):
        for j in range(len(df2)):
            if(df2.loc[j]['courseCode']==df.loc[i]['courseCode'] and df2.loc[j]['section']==df.loc[i]['section'] and df2.loc[j]['coNumber']==df.loc[i]['coNumber']):
                df.at[i,"bloomsLevel"]=df2.loc[j]["BloomsLevel"]
                continue
    df3=df[df["bloomsLevel"]!="EMPTY"]
    return json.loads(df3.to_json(orient='records'))
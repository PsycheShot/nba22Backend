import pandas as pd
from pymongo import MongoClient
import pprint as pp
import numpy as np
import itertools
import json
import DbAccess
# myclient = MongoClient('88.99.143.82',47118)
# db = myclient['dhi_analytics']
# db.authenticate('analytics','pPM8FUJflenw')



dbdetails = DbAccess.details()
myclient = MongoClient(dbdetails["host"],dbdetails["port"])
db = myclient[dbdetails["dbName"]]
db.authenticate(dbdetails["user"], dbdetails["password"])



def get_attainment_details(x):
    attaiment_data = db.dhi_generic_attainment_data.aggregate([
    {'$unwind':'$courseDetails'},
    {'$unwind':'$faculties'},
    {'$match':{
        'termNumber':x['termNumber'],
        'section':x['section'],
        'courseDetails.courseCode':x['courseCode'],
        'faculties.facultyId':x['facultyId'],
        'academicYear':x['year']
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
        'year':'$academicYear',
        'facultyId':'$faculties.facultyId',
        'dir_methodName':'$courseOutcomeDetailsForAttainment.directMethods.methodName',
        'dir_methodDescription':'$courseOutcomeDetailsForAttainment.directMethods.methodDescription',
        'dir_attainment':'$courseOutcomeDetailsForAttainment.directMethods.attainment',
        'dir_attainmentPercentage':'$courseOutcomeDetailsForAttainment.directMethods.attainmentPercentage',
        'indir_attainment':'$courseOutcomeDetailsForAttainment.indirectMethods.attainment',
        'indir_attainmentPercentage':'$courseOutcomeDetailsForAttainment.indirectMethods.attainmentPercentage',
        'indir_methodName':'$courseOutcomeDetailsForAttainment.indirectMethods.methodName',
        'indir_methodDescription':'$courseOutcomeDetailsForAttainment.indirectMethods.methodDescription',
        'totalAttainment':'$courseOutcomeDetailsForAttainment.totalAttainment',
        'directAttainment':'$courseOutcomeDetailsForAttainment.directAttainment',
        'indirectAttainment':'$courseOutcomeDetailsForAttainment.indirectAttainment'
        
        }
    }
    ])
    return list(attaiment_data)


def get_required_attainment_detail(x):
    attaiment_data = db.dhi_generic_attainment_data.aggregate([
    {'$unwind':'$courseDetails'},
    {'$unwind':'$faculties'},
    {'$match':{
        'termNumber':x['termNumber'],
        'section':x['section'],
        'courseDetails.courseCode':x['courseCode'],
        'faculties.facultyId':x['facultyId'],
        'year':x['year']
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
        'indirectAttainment':'$courseOutcomeDetailsForAttainment.indirectAttainment'
        
        }
    }
    ])
    return list(attaiment_data)

def convert_(num):
    return [round(i,2) for i in num]

def get_overall_attainment_data(facultyId,termList,year):
    overall_attainmnet_details = {}
    courses = db.dhi_lesson_plan.aggregate([
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
            } 
        }
        ]
    
       )
    
    df = pd.DataFrame(courses)
    attainmnet_data = []
    df1 = df.copy()
    attainment_data = [get_attainment_details(df1.loc[i]) for i in range(len(df1))]
    attainment = list(itertools.chain.from_iterable(attainment_data))
    df2 = pd.DataFrame(data=attainment, columns=['coNumber','coTitle','termNumber','section','courseCode',
                                           'year','facultyId','dir_methodName','dir_methodDescription','dir_attainment',
                                            'dir_attainmentPercentage','indir_attainment','indir_attainmentPercentage',
                                           'indir_methodName','indir_methodDescription','totalAttainment','directAttainment','indirectAttainment'])
    
    if df2.empty:
        return  attainmnet_data
    df2['dir_attainment'] = df2['dir_attainment'].apply(convert_)
    df2['indir_attainment'] = df2['indir_attainment'].apply(convert_)
    df2['totalAttainment'] = df2['totalAttainment'].round(decimals=2)
    df2['directAttainment'] = df2['directAttainment'].round(decimals=2)
    df2['indirectAttainment'] = df2['indirectAttainment'].round(decimals=2)
    attainment_details = json.loads(df2.to_json(orient='records'))
    df3 = pd.pivot_table(df2, values=['totalAttainment','directAttainment','indirectAttainment'], index=['termNumber','section','courseCode','coNumber','facultyId'], aggfunc=np.mean)
    co_wise_attainment_val = json.loads(df3.reset_index().to_json(orient="records"))
    required_attainment_data_ = [get_required_attainment_detail(df1.loc[i]) for i in range(len(df1))]
    required_attainment_data = list(itertools.chain.from_iterable(required_attainment_data_))
    data = pd.DataFrame(data=required_attainment_data, columns=['coNumber','coTitle','termNumber','section','courseCode','deptId',
                                           'year','facultyId','totalAttainment','directAttainment','indirectAttainment','courseName'])
    required_data = data.groupby(['termNumber','section','courseCode','facultyId','courseName','deptId'])
    average_attainment_data = list(required_data['totalAttainment'].mean())
    total_average_attainment = []

    for k, v in required_data:
        data_ = {}
        data_['termNumber'] = k[0]
        data_['courseCode'] = k[2]
        data_['section'] = k[1]
        data_['facultyId'] = k[3]
        data_['courseName'] = k[4]
        data_['deptId'] = k[5]
        data_['average_attainment'] = round(v['totalAttainment'].mean(),2)
        total_average_attainment.append(data_)
    # print(total_average_attainment)
    overall_attainmnet_details = {"Total_Attainmnet_Data":total_average_attainment,
    "Co_Wise_Atainmnet_Val":co_wise_attainment_val,"Attainmnet_Details":attainment_details}
    return overall_attainmnet_details

    
# pp.pprint(get_overall_attainment_data('465',['1','2','3','4','5','6','7','8'],'2017-18'))
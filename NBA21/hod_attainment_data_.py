import pandas as pd
from pymongo import MongoClient
import pprint as pp
import numpy as np
import itertools
import json
# import DbAccess
myclient = MongoClient('88.99.143.82',47118)
db = myclient['dhi_analytics']
db.authenticate('analytics','pPM8FUJflenw')



# dbdetails = DbAccess.details()
# myclient = MongoClient(dbdetails["host"],dbdetails["port"])
# db = myclient[dbdetails["dbName"]]
# db.authenticate(dbdetails["user"], dbdetails["password"])


db.authenticate('analytics','pPM8FUJflenw')
def hodDetails(facultyId, academicYear, termNumber, degreeId):
    hodDepartment = [x for x in db.dhi_user.aggregate([
        {"$match": {
            "employeeGivenId": facultyId,
            # "academicYear": academicYear, 
            "degreeId": degreeId}},
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
    final = get_overall_attainment_data(facultylist,termNumber,academicYear)
    return final


def sorting_data(data):
    details = []
    [details.append(i['_id']) for i in data]
    df = pd.DataFrame(data=details)
    df1 = df.sort_values(by=['facultyName'])
    df2 = df1.drop_duplicates(subset = ['facultyId','termNumber'])
    return df2

def get_overall_attainment_data(facultyIdList,termList,year):
    overall_attainmnet_details = {}
    course = []
    courses = db.dhi_lesson_plan.aggregate([
        {'$unwind':'$faculties'},
        {'$unwind':'$departments'},
        {'$match':
            {
            'faculties.facultyId':{'$in':facultyIdList},
            'academicYear':year,
            'departments.termNumber':{'$in':termList}
            }
        },
        {
        '$group': 
            {
                '_id': {
                # 'courseCode':1,
                # 'courseName':1,
                'termNumber':'$departments.termNumber',
                # 'section':'$departments.section',
                'facultyId':'$faculties.facultyId',
                'facultyName':'$faculties.facultyName',
                'year':'$academicYear',
                # 'deptId':'$departments.deptId',
            } 
        }
        },
        ])
    data = sorting_data(list(courses))
    data_ = json.loads(data.to_json(orient='records'))
    print(data_)
    return data_


hodDetails('368', '2018-19', ['4'], 'BE')
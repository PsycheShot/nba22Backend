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

def faculty(email):

    facultylist = [x for x in db.dhi_user.aggregate([

        {
            "$match":
            {
                "email":email,
            }
        },
        {
            "$project":
            {
               "facultyGivenId": "$employeeGivenId",
               "degreeId": "$degreeId",
               "facultyName": "$name",
               "roles":"$roles.roleName",
               "deptId":"$deptId",
                "_id":0
            }
        }
    ])]
    return facultylist

def getFacultyAttainmentData(email,academic_year,term_list):
    result = []
    faculty_Id = faculty(email)[0]['facultyGivenId']
    department = faculty(email)[0]['deptId']
    roles = faculty(email)[0]['roles']
    # print(roles)
    if "HOD" in roles:
        courses = db['dhi_lesson_plan'].aggregate([
        {
            '$unwind': {
                'path': '$faculties'
            }
        }, {
            '$unwind': {
                'path': '$departments'
            }
        }, {
            '$match': {
                'academicYear': academic_year,
                'departments.deptId': department,
                'departments.termNumber': {
                    '$in': term_list
                }
            }
        }, {
            '$group': {
                '_id': {
                    'academicYear': '$acdemicYear',
                    'termNumber': '$departments.termNumber',
                    'section': '$departments.section',
                    'semester': '$departments.termName',
                    'courseName': '$courseShortName',
                    'courseCode' : '$courseCode'
                }
            }
        }
        ])
        for course in courses:
            # print(course["_id"])
            result.append(course["_id"])

    else:
        courses = db['dhi_lesson_plan'].aggregate([
            {
                '$unwind': {
                    'path': '$faculties'
                }
            }, {
                '$unwind': {
                    'path': '$departments'
                }
            }, {
                '$match': {
                    'faculties.facultyId': faculty_Id,
                    'academicYear': academic_year,
                    'departments.termNumber': {
                        '$in': term_list
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'academicYear': '$acdemicYear',
                        'termNumber': '$departments.termNumber',
                        'section': '$departments.section',
                        'semester': '$departments.termName',
                        'courseName': '$courseShortName',
                        'courseCode' : '$courseCode'
                    }
                }
            }
        ])
        for course in courses:
            # print(course["_id"])
            result.append(course["_id"])

    return result

def getAllAttainmentData(academic_year,term_list,department):
    result = []
    courses = db['dhi_lesson_plan'].aggregate([
        {
            '$unwind': {
                'path': '$faculties'
            }
        }, {
            '$unwind': {
                'path': '$departments'
            }
        }, {
            '$match': {
                'academicYear': academic_year,
                'departments.deptId': department,
                'departments.termNumber': {
                    '$in': term_list
                }
            }
        }, {
            '$group': {
                '_id': {
                    'academicYear': '$acdemicYear',
                    'termNumber': '$departments.termNumber',
                    'section': '$departments.section',
                    'semester': '$departments.termName',
                    'courseName': '$courseShortName',
                    'courseCode' : '$courseCode'
                }
            }
        }
    ])
    for course in courses:
        print(course["_id"])
        result.append(course["_id"])
    return result

dhi_generic_data=db['dhi_generic_attainment_data']

def getPo(year,semester,courseCode,section):
    po=[]
    count=0
    sumof=0.0
    average=0.0
    for x in db.dhi_generic_attainment_data.aggregate(
        [
        {
            '$match': {
                'courseDetails.courseCode': courseCode,
                'termNumber': semester, 
                'section': section, 
                'academicYear': year
        }
        }, {
        '$unwind': {
            'path': '$courseOutcomeDetailsForAttainment'
        }
        }, {
            '$unwind': {
                'path': '$courseOutcomeDetailsForAttainment.coursePoMapAndAttainment'
            }
        },{
        '$match': {
            'courseOutcomeDetailsForAttainment.coursePoMapAndAttainment.poTargetLevel': {
                '$nin': [
                    'NO_MAPPING'
                    ]
                }
            }
        },
         {
            '$group': {
                '_id': '$courseOutcomeDetailsForAttainment.coursePoMapAndAttainment.programOutcomeNumber', 
                'poAttainment': {
                    '$avg': '$courseOutcomeDetailsForAttainment.coursePoMapAndAttainment.poAttainment'
                }
            }
        }
        ]):
        po.append({"PO Number":x['_id'],"Average PO Attainment":x['poAttainment']})
        # po[x['_id']]=x['poAttainment']
        count+=1
        sumof+=x['poAttainment']
    if count>0:
        average=float(sumof)/count
    result={"PO Details":po,"Average PO Attainment Value":average}
    return result

def getCOForPO(year,term,courseCode,section,poNumber):
    co_for_po = db.dhi_generic_attainment_data.aggregate(
        [
            {
                '$unwind': {
                    'path': '$courseOutcomeDetailsForAttainment'
                }
            }, {
                '$match': {
                    'year': year,
                    'termNumber': term,
                    'courseDetails.courseCode': courseCode,
                    'section': section
                }
            }, {
                '$unwind': {
                    'path': '$courseOutcomeDetailsForAttainment.coursePoMapAndAttainment'
                }
            }, {
                '$match': {
                    'courseOutcomeDetailsForAttainment.coursePoMapAndAttainment.programOutcomeNumber': poNumber
                }
            }, {
                '$project': {
                    '_id': 0,
                    'CO Number': '$courseOutcomeDetailsForAttainment.coNumber', 
                    'Total Attained Value': '$courseOutcomeDetailsForAttainment.coursePoMapAndAttainment.poTargetValue', 
                    'Mapping Level': '$courseOutcomeDetailsForAttainment.coursePoMapAndAttainment.poTargetLevel', 
                    'Score': '$courseOutcomeDetailsForAttainment.coursePoMapAndAttainment.poAttainment'
                }
            }
        ]
    )
    return list(co_for_po)



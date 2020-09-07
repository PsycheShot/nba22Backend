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



def get_faculty_names(designation,deptid,year):
    faculty_list = list(db.dhi_user.aggregate([
    {
        '$match': {
            'designation': designation, 
            'deptId': deptid, 
            'academicYear': year
        }
    }, {
        '$project': {
            'name': 1, 
            'lastName': 1, 
            '_id': 0, 
            'employeeGivenId': 1
        }
    }
    ]))     
    # print(faculty_list)
    return faculty_list

blooms_level = {
    'REMEMBER':1,
    'UNDERSTAND':2,
    'APPLY':3,
    'ANALYZE':4,
    'EVALUATE':5,
    'CREATE':6
}

def get_chartdata(facultyid,deptid,year,term):
    data = {}
    data['x_axis']= get_x_axis_co_blooms_level_data(facultyid,deptid,year,term)
    data['y_axis']= getAttainmentData(year, deptid, term, facultyid)
    return data

   
def get_x_axis_co_blooms_level_data(facultyid,deptid,year,term):
    # return "hello"
    xaxis_data = list(db.dhi_lesson_plan.aggregate([
    {
        '$match': {
            'faculties.facultyId': facultyid, 
            'academicYear': year, 
            'courseDeptId': deptid, 
            'departments.termNumber': term
        }
    }, {
        '$project': {
            '_id': 0,
            'courseName': 1,
            'plan':1,
            'courseCode':1
        }
    }
    ]))
    complete = {}
    print(len(xaxis_data))
    for cor in xaxis_data:
        complete[cor.get('courseCode')] = {}
        plans = cor.get('plan')
        for lesson in plans:
            if 'couseOutcomes' in lesson and 'bloomsLevel' in lesson:
                for co in lesson['couseOutcomes']:
                    if str(co) in complete[cor.get('courseCode')]:
                        complete[cor.get('courseCode')][str(co)].append(blooms_level[lesson['bloomsLevel']])
                    else:
                        complete[cor.get('courseCode')][str(co)] = [blooms_level[lesson['bloomsLevel']]]


    # print(complete)
    for cor in complete.keys():
        for co in complete[cor].keys():
            complete[cor][co] = round(sum(complete[cor][co]) / len(complete[cor][co]))
    # print(complete)           
    return complete

# get_x_axis_co_blooms_level_data("359","CS","2019-20","7")
# get_x_axis_co_blooms_level_data("408","BT","2018-19","7")
# get_x_axis_co_blooms_level_data("315","ME","2018-19","5")

def getAttainmentData(year, dept, term, faculty):
    result = db.dhi_generic_attainment_data.aggregate([
        {
            '$match': {
                'year': year, 
                'deptId': dept, 
                'termNumber': term, 
                'faculties.facultyId': faculty
            }
        }, {
            '$group': {
                '_id': '$courseDetails.courseCode', 
                'courseOutcomeDetailsForAttainment': {
                    '$push': '$courseOutcomeDetailsForAttainment'
                },
                'co': {
                '$addToSet': "$courseOutcomeDetailsForAttainment.coNumber"
                }
            }
        }
    ])

    faculty_data=[]

    for i in result:
        temp = {}
        temp[i["_id"]] = {}
        for a in i['co'][0]:
            temp[i["_id"]][str(a)] = {}
            temp[i["_id"]][str(a)]["total"] = 0
            temp[i["_id"]][str(a)]["direct"] = 0
            temp[i["_id"]][str(a)]["indirect"] = 0
        count = 0
        for j in i["courseOutcomeDetailsForAttainment"]:
            count += 1
            for k in j:
                temp[i["_id"]][str(k["coNumber"])]["total"] += k["totalAttainment"]
                temp[i["_id"]][str(k["coNumber"])]["direct"] += k["directAttainment"]
                temp[i["_id"]][str(k["coNumber"])]["indirect"] += k["indirectAttainment"]

        for a in i['co'][0]:
            temp[i["_id"]][str(a)]["total"] = temp[i["_id"]][str(a)]["total"]/count
            temp[i["_id"]][str(a)]["direct"] = temp[i["_id"]][str(a)]["direct"]/count
            temp[i["_id"]][str(a)]["indirect"] = temp[i["_id"]][str(a)]["indirect"]/count
        faculty_data.append(temp)

    return faculty_data

def getCourseAttainment(year, dept, term, faculty, course):
    result = db.dhi_generic_attainment_data.aggregate([
        {
            '$match': {
                'faculties.facultyGivenId': faculty, 
                'year': year, 
                'termNumber': term, 
                'deptId': dept, 
                'courseDetails.courseCode': course
            }
        }, {
            '$group': {
                '_id': '$courseDetails.courseCode', 
                'courseOutcomeDetailsForAttainment': {
                    '$push': '$courseOutcomeDetailsForAttainment'
                }, 
                'co': {
                    '$addToSet': '$courseOutcomeDetailsForAttainment.coNumber'
                }
            }
        }
    ])

    course_data = {}

    for i in result:
        course_data[i["_id"]] = {}
        for a in i["co"][0]:
            course_data[i["_id"]][str(a)] = {}
            course_data[i["_id"]][str(a)]["total"] = 0
            course_data[i["_id"]][str(a)]["direct"] = 0
            course_data[i["_id"]][str(a)]["indirect"] = 0
        count = 0
        for j in i["courseOutcomeDetailsForAttainment"]:
            count += 1
            for k in j:
                course_data[i["_id"]][str(k["coNumber"])]["total"] += k["totalAttainment"]
                course_data[i["_id"]][str(k["coNumber"])]["direct"] += k["directAttainment"]
                course_data[i["_id"]][str(k["coNumber"])]["indirect"] += k["indirectAttainment"]
        for a in i["co"][0]:
            course_data[i["_id"]][str(a)]["total"] = course_data[i["_id"]][str(a)]["total"]/count
            course_data[i["_id"]][str(a)]["direct"] = course_data[i["_id"]][str(a)]["direct"]/count
            course_data[i["_id"]][str(a)]["indirect"] = course_data[i["_id"]][str(a)]["indirect"]/count

    return course_data





def getAcadYears():
    result_1 = db['dhi_term_detail'].aggregate([
        {
            '$group': {
                '_id': 0, 
                'acadYears': {
                    '$addToSet': '$academicYear'
                }
            }
        }
    ])

    for i in result_1:
        return sorted(list(i["acadYears"]))


#getting all departments
def getAllDept():
    result_2 = db['dhi_lesson_plan'].aggregate([
        {
            '$group': {
                '_id': 0, 
                'depts': {
                    '$addToSet': '$courseDeptId'
                }
            }
        }
    ])

    for i in result_2:
        return i["depts"]
  

def getTerms(year, dept):
    #gettign degree for selected dept
    degree_dept = db['dhi_lesson_plan'].aggregate([
        {
            '$project': {
                'degreeId': 1, 
                'depts': '$courseDeptId'
            }
        }, {
            '$group': {
                '_id': '$degreeId', 
                'fieldN': {
                    '$addToSet': '$depts'
                }
            }
        }
    ])

    for i in degree_dept:
        if dept in i["fieldN"]:
            degree = i["_id"]

    #getting term with respect selected year and dept

    result_3 = db['dhi_term_detail'].aggregate([
        {
            '$match': {
                'academicYear': year, 
                'degreeId': degree
            }
        }, {
            '$group': {
                '_id': 0, 
                'terms': {
                    '$push': '$academicCalendar.termNumber'
                }
            }
        }
    ])

    for i in result_3:
        return sorted(list(set(i["terms"][0])))
  
#getting all designations
def getDesignations():
    result_4 = db['dhi_user'].aggregate([
        {
            '$match': {
                'userType': 'TEACHING'
            }
        }, {
            '$group': {
                '_id': 0, 
                'designations': {
                    '$addToSet': '$designation'
                }
            }
        }
    ])

    for i in result_4:
        return i["designations"]


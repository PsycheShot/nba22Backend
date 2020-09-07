from pymongo import MongoClient
import pprint
import types

import DbAccess 

dbdetails = DbAccess.details()
myclient = MongoClient(dbdetails["host"],dbdetails["port"])
mydb = myclient[dbdetails["dbName"]]

mydb.authenticate(dbdetails["user"], dbdetails["password"])



dhi_internal = mydb["dhi_internal"]
dhi_lesson_plan = mydb['dhi_lesson_plan']


def getterm(facultyId, academicYear):
    termnumber = []
    for x in mydb.dhi_lesson_plan.aggregate([
        {"$match":
         {
            'faculties.facultyGivenId': facultyId,
            'academicYear': academicYear
             }
         },
        {"$unwind": "$departments"},

        {
            "$project":
            {
                "term": "$departments.termNumber",
                "_id": 0
            }
        }
    ]):
        termnumber.append(x["term"])
    termnumber = sorted(list(set(termnumber)))
    return termnumber


def dhi_internal_getterm(facultyId, academicYear):
    termnumber = []
    for x in mydb.dhi_internal.aggregate([
        {"$match":
         {
            'faculties.facultyGivenId': facultyId,
            'academicYear': academicYear
             }
         },
        {"$unwind": "$departments"},

        {
            "$project":
            {
                "term": "$departments.termNumber",
                "_id": 0
            }
        }
    ]):
        termnumber.append(x["term"])
    termnumber = sorted(list(set(termnumber)))
    return termnumber


def nba3getterm(facultyId, academicYear, role):
    termnumber = []
    if role == 'FACULTY':
        for x in mydb.dhi_lesson_plan.aggregate([
            {"$match":
             {
                'faculties.facultyId': facultyId,
                'academicYear': academicYear
                 }
             },
            {"$unwind": "$departments"},

            {
                "$project":
                {
                    "term": "$departments.termNumber",
                    "_id": 0
                }
            }
        ]):
            # print(x)
            termnumber.append(x["term"])
        termnumber = sorted(list(set(termnumber)))
        # termnumber.sort(reverse = True)
        return termnumber

    else:
        for x in mydb.dhi_lesson_plan.aggregate([
            {'$match': {
                'faculties.facultyId': facultyId,
                'academicYear': academicYear,
            }},
            {'$unwind': '$departments'},
            {'$project': {
                '_id': 0,
                'deptId': '$departments.deptId'}}
        ]):

            depart = x['deptId']

        for department in mydb.dhi_lesson_plan.aggregate([
            {'$match': {'academicYear': academicYear,
                        'departments.deptId': depart
                        }},
            {'$unwind': '$departments'},
            {'$project':
             {'termnumber': '$departments.termNumber', "_id": 0}}
        ]):
            termno = department['termnumber']

            termnumber.append(
                termno) if termno not in termnumber else termnumber
        termnumber = sorted(list(set(termnumber)))
        return termnumber


def termType(degreeId):
    term = []
    termtype = list(mydb.dhi_user.find(


        {
            "degreeId": degreeId,
        },

        {
            "termType": "$academicCalendar.termType",

            "_id": 0
        }

    ))

    for x in termtype:
        term.append(x) if x not in term and x != {
        } and isinstance(x, dict) else term
    # term.append('Academic Year')
    return (term[0]['termType'])


def nba7getterm(facultyId, academicYear, role):
    termnumber = []
    if role == 'FACULTY':
        for x in mydb.dhi_internal.aggregate([
            {"$match":
             {
                'faculties.facultyGivenId': facultyId,
                'academicYear': academicYear
                 }
             },
            {"$unwind": "$departments"},
            {
                "$project":
                {
                    "term": "$departments.termNumber",
                    "_id": 0
                }
            }
        ]):
            # print(x)
            termnumber.append(x["term"])
        termnumber = sorted(list(set(termnumber)))
        # termnumber.sort(reverse = True)
        return termnumber

    else:
        for x in mydb.dhi_internal.aggregate([
            {'$match': {
                'faculties.facultyGivenId': facultyId,
                'academicYear': academicYear,
            }},
            {'$unwind': '$departments'},
            {'$project': {
                '_id': 0,
                'department': '$departments.deptId'}}
        ]):
            # pprint(x)
            # return x['department']
            depart = x['department']

        for department in mydb.dhi_lesson_plan.aggregate([
            {'$match': {'academicYear': academicYear,
                        'departments.deptId': depart
                        }},
            {'$unwind': '$departments'},
            {'$project':
             {'termnumber': '$departments.termNumber', "_id": 0}}
        ]):
            termno = department['termnumber']
            termnumber.append(
                termno) if termno not in termnumber else termnumber
        termnumber = sorted(list(set(termnumber)))
        return termnumber

def nba13getterm(facultyId,academicYear, role):
    termnumber = []
    if role == 'FACULTY':
        for x in mydb.dhi_lesson_plan.aggregate([
            {"$match":
             {
                'faculties.facultyGivenId': facultyId,
                'academicYear': academicYear
                 }
             },
            {"$unwind": "$departments"},

            {
                "$project":
                {
                    "term": "$departments.termNumber",
                    "_id": 0
                }
            }
        ]):
            # print(x)
            termnumber.append(x["term"])
        termnumber = sorted(list(set(termnumber)))
        # termnumber.sort(reverse = True)
        return termnumber

    else:
        for x in mydb.dhi_internal.aggregate([
            {'$match': {
                'faculties.facultyGivenId': facultyId,
                'academicYear': academicYear,
            }},
            {'$unwind': '$departments'},
            {'$project': {
                '_id': 0,
                'department': '$departments.deptId'}}
        ]):
            # pprint(x)
            # return x['department']
            depart = x['department']

        for department in mydb.dhi_lesson_plan.aggregate([
            {'$match': {'academicYear': academicYear,
                        'departments.deptId': depart
                        }},
            {'$unwind': '$departments'},
            {'$project':
             {'termnumber': '$departments.termNumber', "_id": 0}}
        ]):
            termno = department['termnumber']
            termnumber.append(
                termno) if termno not in termnumber else termnumber
        termnumber = sorted(list(set(termnumber)))
        return termnumber

# print(nba7getterm("697", "2019-20", "HOD"))

from pprint import pprint
from pymongo import MongoClient
import DbAccess 

dbdetails = DbAccess.details()
myclient = MongoClient(dbdetails["host"],dbdetails["port"])

mydb = myclient[dbdetails["dbName"]]
mydb.authenticate(dbdetails["user"], dbdetails["password"])

dhi_lesson_plan = mydb['dhi_lesson_plan']
dhi_user = mydb['dhi_user']
dhi_internal = mydb["dhi_internal"]


subjectDetail = []

departments = []
years = []


def principalYear():
    for x in dhi_lesson_plan.find({}, {'_id': 0, 'academicYear': 1}):
        years.append(x['academicYear']
                     ) if x['academicYear'] not in years else years
    return years


def principalDepartments(academicYear):
    collegeDepartment = [x for x in dhi_lesson_plan.aggregate([
        {'$match': {'academicYear': academicYear}},

        {"$unwind": "$departments"},
        {"$unwind": "$faculties"},
        {
            '$group':
            {
                '_id': '$academicYear',

                'departments':
                {
                    '$push':
                    {
                        'deptId': '$departments.deptId',
                        # "deptName" : "$departments.deptName"

                    }
                },

            }
        }
    ])]
    # pprint(collegeDepartment)
    for i in range(len(collegeDepartment)):
        for j in range(len(collegeDepartment[i]['departments'])):
            if 'deptId' in collegeDepartment[i]['departments'][j]:
                depart = collegeDepartment[i]['departments'][j]['deptId']
                departments.append(
                    depart) if depart not in departments else departments

    # for department in departments:
    #     termsDetails(academicYear, department)
    return departments


def termsDetails(academicYear, department):
    terms = []
    departmentTerms = [x for x in dhi_lesson_plan.aggregate([
        {'$match': {'academicYear': academicYear}},
        {'$match': {'departments.deptId': department}},
        {"$unwind": "$departments"},
        {"$unwind": "$faculties"},
        {
            '$group':
            {
                '_id': '$departments.deptId',

                'departments':
                {
                    '$push':
                    {
                        'termNumber': '$departments.termNumber',
                        'degreeId': '$degreeId',
                        
                    }
                },
            }
        }

    ])]
    for i in range(len(departmentTerms)):
        for j in range(len(departmentTerms[i]['departments'])):
            degreeId = departmentTerms[i]['departments'][j]['degreeId']
            term = departmentTerms[i]['departments'][j]['termNumber']
            terms.append(term) if term not in terms else termsDetails
    return list(sorted(terms))





# nba7
def dhi_internal_principalYear():
    for x in dhi_internal.find({}, {'_id': 0, 'academicYear': 1}):
        if 'academicYear' in x:
            years.append(x['academicYear']) if x['academicYear'] not in years else years
    return sorted(years)


def dhi_internal_principalDepartments(academicYear):
    collegeDepartment = [x for x in dhi_internal.aggregate([
        {'$match': {'academicYear': academicYear}},

        {"$unwind": "$departments"},
        {"$unwind": "$faculties"},
        {
            '$group':
            {
                '_id': '$academicYear',

                'departments':
                {
                    '$push':
                    {
                        'deptId': '$departments.deptId',


                    }
                },

            }
        }
    ])]
    # pprint(collegeDepartment)
    for i in range(len(collegeDepartment)):
        for j in range(len(collegeDepartment[i]['departments'])):
            if 'deptId' in collegeDepartment[i]['departments'][j]:
                depart = collegeDepartment[i]['departments'][j]['deptId']
                departments.append(
                    depart) if depart not in departments else departments

    return departments


def dhi_internal_terms(academicYear, department):
    terms = []
    departmentTerms = [x for x in dhi_internal.aggregate([
        {'$match': {'academicYear': academicYear}},
        {'$match': {'departments.deptId': department}},
        {"$unwind": "$departments"},
        {"$unwind": "$faculties"},
        {
            '$group':
            {
                '_id': '$departments.deptId',

                'departments':
                {
                    '$push':
                    {
                        'termNumber': '$departments.termNumber',
                        'degreeId': '$degreeId',

                    }
                },
            }
        }

    ])]
    for i in range(len(departmentTerms)):
        for j in range(len(departmentTerms[i]['departments'])):
            degreeId = departmentTerms[i]['departments'][j]['degreeId']
            term = departmentTerms[i]['departments'][j]['termNumber']
            terms.append(term) if term not in terms else termsDetails
    return list(sorted(terms))



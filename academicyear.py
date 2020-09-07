from pymongo import MongoClient
import pprint
import DbAccess 

dbdetails = DbAccess.details()
myclient = MongoClient(dbdetails["host"],dbdetails["port"])
mydb = myclient[dbdetails["dbName"]]
mydb.authenticate(dbdetails["user"], dbdetails["password"])

dhi_lesson_plan = mydb["dhi_lesson_plan"]
dhi_user = mydb["dhi_user"]


def faculty(email):

    facultylist = [x for x in mydb.dhi_user.aggregate([
        {
            "$match":
            {
                "email": email,
            }
        },
        {
            "$project":
            {
                "facultyGivenId": "$employeeGivenId",
                "degreeId": "$degreeId",
                "facultyName": "name",
                "_id": 0
            }
        }
    ])]
    return facultylist


def getacadyear(email):
    faculty_info = faculty(email)
    facultyGivenId = faculty_info[0]["facultyGivenId"]
    academicYear = [x["academicYear"] for x in mydb.dhi_lesson_plan.aggregate([
        {"$unwind": "$faculties"},
        {"$match": {"faculties.facultyGivenId": facultyGivenId}},


        {
            "$group": {"_id": "$academicYear"}
        },
        {
            "$project": {
                "academicYear": "$_id",
                "_id": 0
            }

        }
    ])]
    return list(sorted(academicYear))


def getprincipalacademicyear():
    academicYear = [x["academicYear"] for x in mydb.dhi_lesson_plan.aggregate([


        {
            "$group": {"_id": "$academicYear"}
        },
        {
            "$project": {
                "academicYear": "$_id",
                "_id": 0
            }

        }
    ])]
    return list(sorted(academicYear))


# def nba3getacadyear(facultyId):
#     academicYear = [x["academicYear"] for x in mydb.dhi_lesson_plan.aggregate([
#         {"$unwind": "$faculties"},
#         {"$match": {"faculties.facultyId": facultyId}},
#         {
#             "$group": {"_id": "$academicYear"}
#         },
#         {
#             "$project": {
#                 "academicYear": "$_id",
#                 "_id": 0
#             }

#         }
#     ])]
#     return list(sorted(academicYear))


def nba3getacadyear(email):
    faculty_info = faculty(email)
    facultyId = faculty_info[0]["facultyGivenId"]
    academicYear = [x["academicYear"] for x in mydb.dhi_lesson_plan.aggregate([
        {"$unwind": "$faculties"},
        {"$match": {"faculties.facultyId": facultyId}},
        {
            "$group": {"_id": "$academicYear"}
        },
        {
            "$project": {
                "academicYear": "$_id",
                "_id": 0
            }

        }
    ])]
    return list(sorted(academicYear))


def dhi_internal_acadyear(email):
    faculty_info = faculty(email)
    facultyGivenId = faculty_info[0]["facultyGivenId"]
    academicYear = [x["academicYear"] for x in mydb.dhi_internal.aggregate([
        {"$unwind": "$faculties"},
        {"$match": {"faculties.facultyId": facultyGivenId}},


        {
            "$group": {"_id": "$academicYear"}
        },
        {
            "$project": {
                "academicYear": "$_id",
                "_id": 0
            }

        }
    ])]
    return list(sorted(academicYear))
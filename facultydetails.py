from pymongo import MongoClient
from pprint import pprint
import DbAccess 
dbdetails = DbAccess.details()
myclient = MongoClient(dbdetails["host"],dbdetails["port"])
mydb = myclient[dbdetails["dbName"]]
mydb.authenticate(dbdetails["user"], dbdetails["password"])

dhi_user = mydb["dhi_user"]

def faculty(email):

    facultylist = [x for x in mydb.dhi_user.aggregate([

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
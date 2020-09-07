from pprint import pprint
from pymongo import MongoClient
import multitenantconfig
tenantName = "dhi_analytics"



def details():
    # tenantName = serviceapi.tenant()
    return multitenantconfig.DBdetails(tenantName)
def tenant():
    return tenantName


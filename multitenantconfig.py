import yaml
import pymongo
import urllib.parse

tenant_dict = {}

def load_tenant_data(tenantName):
    with open('resource/application.yaml') as f:
        docs = yaml.load_all(f, Loader=yaml.Loader)
        for doc in docs:
            tenants = doc["app"]["tenants"]
            for tenant in tenants:
                t = tenants[tenant]
                if tenantName == t["dbName"]:
                    return t

def getTenantUri(tenantName):
    if len(tenant_dict) == 0:
        tenantDetail = load_tenant_data(tenantName)
        username = urllib.parse.quote_plus(tenantDetail["user"])
        password = urllib.parse.quote_plus(tenantDetail["password"])
        uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password,tenantDetail["host"],tenantDetail["port"],tenantDetail["dbName"])
        return uri

def DBdetails(tenantName):
    if len(tenant_dict) == 0:
        tenantDetail = load_tenant_data(tenantName)
        details = {"host":tenantDetail["host"],"port":tenantDetail["port"],"dbName":tenantDetail["dbName"],"user":tenantDetail["user"],"password":tenantDetail["password"]}
        return details
    



# print(getTenantUri('dhi_analytics'))
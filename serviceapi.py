from flask import Flask, jsonify,request
from flask_cors import CORS, cross_origin
from flask_pymongo import PyMongo
from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token,
    get_jwt_identity, get_jwt_claims

)
import multitenantconfig
import academicyear, facultydetails, principal, term
from NBA21 import faculty_attainment_data,hod_attainment_data_
import DbAccess
from NBA22 import faculty_attainment_data22,hod_attainment_data22
from NBA22A import faculty_average_attainment_data
from NBA26 import nba26

app = Flask(__name__)
CORS(app)
# app.config["MONGO_URI"] = "mongodb://88.99.143.82:47118/dhi_analytics"
tenantName = DbAccess.tenant()
app.config["MONGO_URI"] = multitenantconfig.getTenantUri(tenantName)

def details():
    return multitenantconfig.DBdetails(tenantName)


mongo = PyMongo(app)
# Setup the Flask-JWT-Extended extension
app.config['JWT_SECRET_KEY'] = 'super-secret'  # Change this!
jwt = JWTManager(app)


class UserObject:
    def __init__(self, username, roles):
        self.username = username
        self.roles = roles


@jwt.user_claims_loader
def add_claims_to_access_token(user):
    # print('roles', user.roles)
    return {'roles': user.roles}

@jwt.user_identity_loader
def user_identity_lookup(user):
    return user.username

# Provide a method to create access tokens. The create_access_token()
# function is used to actually generate the token, and you can return
# it to the caller however you choose.

@app.route('/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400

    username = request.json.get('username', None)
    password = request.json.get('password', None)
    if not username:
        return jsonify({"msg": "Missing username parameter"}), 400
    if not password:
        return jsonify({"msg": "Missing password parameter"}), 400
    user = mongo.db.dhi_user.find_one({'email': username})
   
    if not user:
        return jsonify({"msg": "Bad username or password"}), 401
    roles = [ x['roleName'] for x in user['roles']]
    user = UserObject(username=user["email"], roles=roles)
    # Identity can be any data that is json serializable
    access_token = create_access_token(identity=user,expires_delta=False)
    return jsonify(dhi_analytics_token=access_token), 200

@app.route('/message')
def message():
    return {"message":"Check you luck"}

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.
@app.route('/user', methods=['GET'])
@jwt_required
def protected():
    # Access the identity of the current user with get_jwt_identity
    ret = {
        'user': get_jwt_identity(),  
        'roles': get_jwt_claims()['roles'] ,
        }
    return jsonify(ret), 200






#nba
@app.route("/academicyears/<string:email>")
def academicYear(email):
    return jsonify({"AcademicYears":academicyear.getacadyear(email)})


@app.route("/termnumber/<string:fid>/<string:year>")
def termnumber(fid,year):
    return jsonify({"semesters": term.getterm(fid,year)})

@app.route("/termtype/<string:degreeId>")
def getTermType(degreeId):
     return jsonify({"termType":term.termType(degreeId)})

@app.route("/facultydetails/<string:email>")
def faculty_details(email):
    return jsonify({"facultydetails": facultydetails.faculty(email)})


@app.route("/Academicyear")
def principalYears():
    return jsonify({"Academicyears": principal.principalYear()})


@app.route("/principalDepartments/<string:year>")
def collegeDepartments(year):
    return jsonify({"Departments": principal.principalDepartments(year)})


@app.route("/principalTerms/<string:year>/<string:deptId>")
def DepartmentTerms(year, deptId):
    return jsonify({"PrincipalTerm": principal.termsDetails(year, deptId)})


@app.route("/termnumber/<string:fid>/<string:year>/<string:role>")
def nba3termnumber(fid, year, role):
    return jsonify({"semesters": term.nba3getterm(fid, year, role)})

#nba21
@app.route("/nba21facultyDetail/<string:year>/<termNumbers>/<string:facultyId>")
def nba21_faculty_data(year,termNumbers,facultyId):
    termNumbers = list(termNumbers.split(','))
    faculty_data =  faculty_attainment_data.get_overall_attainment_data(facultyId,termNumbers,year)
    # print(faculty_data)
    return jsonify({"faculty_data":faculty_data})


@app.route("/nba21hodDetail/<string:facultyId>/<string:year>/<termNumbers>/<string:degreeId>")
def nba21_hod_data(year,termNumbers,facultyId,degreeId):
    termNumbers = list(termNumbers.split(','))
    faculty_data =  hod_attainment_data_.hodDetails(facultyId,year,termNumbers,degreeId)
    return jsonify({"faculty_data":faculty_data})

@app.route("/nba22hodDetail/<string:facultyId>/<string:year>/<termNumbers>")
def nba22_hod_data(year,termNumbers,facultyId):
    termNumbers = list(termNumbers.split(','))
    faculty_data =  hod_attainment_data22.hodDetails(facultyId,year,termNumbers)
    return jsonify({"faculty_data":faculty_data})

@app.route("/nba21principalDetail/<string:academicYear>/<termNumber>/<string:department>")
def nba21_principal_details(academicYear, termNumber, department):
    termList=list(termNumber.split(','))
    principaldetails = hod_attainment_data_.hodSubject(
        academicYear, termList, department)
    return jsonify({"principaldetails": principaldetails})

# 15BT744/2018-19/408/7
# @app.route("/nba22facultyDetail/<string:courseCode>/<string:acadYear>/<string:facultyId>/<string:termNumber>")
# def nba22_faculty_details(courseCode,acadYear,facultyId,termNumber):
#     faculty_attainment_data22.get_bloomslevel_with_co()
#     return {"Mes":"Rec"};
@app.route("/nba22facultyDetail/bloommapping/<string:facultyId>/<string:year>/<termNumbers>/<string:cCode>") 
def nba22_faculty_data(year,termNumbers,facultyId,cCode):
    termList=list(termNumbers.split(','))
    return jsonify(faculty_attainment_data22.web_combine(facultyId,termList,year,cCode))
    
@app.route("/nba22hodDetail/bloommaping/<string:facultyId>/<string:year>/<termNumbers>/<string:deptId>/<string:cCode>")
def nba22_hod_bloom_chart(year,termNumbers,facultyId,deptId,cCode):
    termList=list(termNumbers.split(','))
    return jsonify(hod_attainment_data22.web_combine_bloom_hod(facultyId,year,termList,deptId,cCode))

@app.route("/nba22facultyDetail/codescription/<string:facultyId>/<string:year>/<termNumbers>/<string:cCode>")
def nba22_faculty_co_description(year,termNumbers,facultyId,cCode):
    termList=list(termNumbers.split(','))
    return jsonify(faculty_attainment_data22.get_co_description(facultyId,termList,year,cCode))

@app.route("/nba22facultyDetail/average_co/<string:facultyId>/<string:year>/<termNumbers>")  
def nba22_faculty_average_co(year,termNumbers,facultyId):
    termList=list(termNumbers.split(','))
    return jsonify({"averages":faculty_attainment_data22.get_average_co_attainment(facultyId,termList,year)})

@app.route("/nba22hodDetail/<string:email>")
def nba22_hod_dept(email):
    return jsonify(hod_attainment_data22.get_hod_dept(email))

@app.route("/nba22hodDetail/term/<string:deptId>/<string:year>")
def nba22_hod_dept_term(deptId,year):
    return jsonify(hod_attainment_data22.get_term_hod_dept(deptId,year))

@app.route("/nba22hodDetail/Id/<string:deptId>")
def nba22_hod_id(deptId):
    return jsonify(hod_attainment_data22.getHodIdUsingDept(deptId))

@app.route("/nba22hodDetail/bloom_rwd/<string:facultyId>/<string:year>/<termNumbers>")
def nba22_rwd_bloom(year,termNumbers,facultyId):
    termList=list(termNumbers.split(','))
    return jsonify(faculty_attainment_data22.combine_bloom(facultyId,termList,year))

@app.route("/nba22hodDetail/bloom_rwd/hod/<string:facultyId>/<string:year>/<termNumbers>")
def nba22_rwd_bloom_hod(year,termNumbers,facultyId):
    termList=list(termNumbers.split(','))
    facultyList = list(facultyId.split(','))
    return jsonify(hod_attainment_data22.combine_bloom(facultyList,termList,year))

#nba22a latest
@app.route("/nba22/fac/<string:designation>/<string:deptid>/<string:year>")
def nba22_get_faculty_list(designation,deptid,year):
    faculty_list =  faculty_average_attainment_data.get_faculty_names(designation,deptid,year)
    return jsonify({"faculty_data":faculty_list})

@app.route("/nba22/fac/<string:facultyid>/<string:deptid>/<string:year>/<string:term>")
def nba22_get_chartdata(facultyid,deptid,year,term):
    chart_data =  faculty_average_attainment_data.get_chartdata(facultyid,deptid,year,term)
    return jsonify({"chart_data":chart_data})

@app.route("/nba22/fac/acadyears")
def nba_22_get_acadyears():
    acad_years =  faculty_average_attainment_data.getAcadYears()
    return jsonify({"acad_years":acad_years})

@app.route("/nba22/fac/getalldept")
def nba_22_get_alldpet():
    all_dept =  faculty_average_attainment_data.getAllDept()
    return jsonify({"all_dept":all_dept})

@app.route("/nba22/fac/getterms/<string:year>/<string:dept>")
def nba_22_get_all_terms(year,dept):
    all_terms =  faculty_average_attainment_data.getTerms(year,dept)
    return jsonify({"all_terms":all_terms})

@app.route("/nba22/fac/getalldesignations")
def nba_22_get_alldesig():
    all_designations =  faculty_average_attainment_data.getDesignations()
    return jsonify({"all_designations":all_designations})

@app.route("/nba22/fac/<string:facultyid>/<string:deptid>/<string:year>/<string:term>/<string:courseid>")
def getCourseAttainmen(facultyid,deptid,year,term,courseid):
    coursedata =  faculty_average_attainment_data.getCourseAttainment(year,deptid,term,facultyid,courseid)
    return jsonify({"coursedata":coursedata})

@app.route("/nba22hodDetail/bloom_chart/<facultyId>/<string:year>/<termNumbers>/<string:deptID>")
def nba22_rwd_chart(facultyId,year,termNumbers,deptID):
    termList=list(termNumbers.split(','))
    facList=list(facultyId.split(','))
    return jsonify(hod_attainment_data22.hod_combine_bloom(facList,year,termList,deptID))




## nba26

@app.route("/nba26courseDetails/<string:email>/<string:year>/<terms>")
def getCourseList(email,year,terms):
    terms = list(terms.split(','))
    all_courses=nba26.getFacultyAttainmentData(email,year,terms)
    all_po=[] 
    all_co_for_po=[]
    for course in all_courses:
        po=nba26.getPo(year,course['termNumber'],course['courseCode'],course['section'])
        # all_po.append(po)
        for each in po['PO Details']:
            co_for_po=nba26.getCOForPO(year,course['termNumber'],course['courseCode'],course['section'],each['PO Number'])
            all_co_for_po.append({"PO Details":each,"CO Details":co_for_po})
        all_po.append({"Course Details":course,"PO List":all_co_for_po,"All_po_avg":po["Average PO Attainment Value"]})
    return jsonify({"course_po":all_po})

if __name__ == "__main__":
    app.run(port=8088,debug=True)

# FastAPI dependencies
from fastapi import FastAPI
from fastapi import File
from fastapi import UploadFile
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.param_functions import Depends
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

# Controller Classes
from resources.route import PingAPI
from resources.route import RouteSequenceAPI
from resources.route import DMSSequenceAPI

# Helpers
from helpers.errorfunctions import raw_errors_to_fields
from helpers.errorfunctions import CustomException
from helpers.querybodymodel import GetRoute
from helpers.querybodymodel import DMSRoute
from helpers.querybodymodel import ResponsePing
from helpers.querybodymodel import ResponseRoute
from helpers.querybodymodel import ResponseDMS

# Helper Libraries
import re
import datetime
import os

# Initiate App with FastAPI
app = FastAPI()

# Adding CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Error Handling
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    result = raw_errors_to_fields(exc.raw_errors)
    error = {}
    for res in result:
        if res['name'] == 'file':
            error[res['name']] = "File attribute (file) cannot be blank!"
        elif res['name'] == 'dcstart':
            error[res['name']] = "DC Start Time (dcstart) cannot be blank!"
        elif res['name'] == 'email':
            error[res['name']] = "Email (email) cannot be blank!"
    return JSONResponse(content=error, status_code=400)


@app.exception_handler(CustomException)
async def unicorn_exception_handler(request: Request, exc: CustomException):
    if exc.attr == 'file':
        content = {
                "message": "No file attached!"
            }
    elif exc.attr == 'fileext':
        content = {
                "message": "Not an Excel file!",
                "example": "<filename>.xlsx"
            }
    elif exc.attr == 'email':
        content = {
            "message": {
                "email": "Invalid Email format, format (example@mail.in)",
                "example": "devops@waycool.in"
            }
        }
    elif exc.attr == 'date':
        content = {
            "message": {
                "start": "Invalid Date format, format (%Y-%m-%d %H:%M:%S)",
                "example": "2020-09-10 04:00:00"
            }
        }
    return JSONResponse(
        status_code=400,
        content=content,
    )


# Ping Route
# http://127.0.0.1:8000/
@app.get('/', response_model=ResponsePing)
async def root():
    _cursor = PingAPI()
    result = await _cursor.response()
    return result


# Getroute Route
# http://127.0.0.1:8000/getroute
@app.post('/getroute', response_model=ResponseRoute)
async def getroute(
            file: UploadFile = File(...),
            dataparser: GetRoute = Depends(GetRoute.as_form)
        ):
    regex = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w+$'
    if not re.search(regex, dataparser.email):
        raise CustomException(attr='email')
    try:
        dataparser.dcstart = datetime.datetime.strptime(
            dataparser.dcstart,
            '%Y-%m-%d %H:%M:%S'
        )
    except Exception as e:
        print(e)
        raise CustomException(attr="date")
    if len(file.filename) == 0:
        raise CustomException(attr='file')
    _filename, _file_extension = os.path.splitext(file.filename)
    if _file_extension != '.xlsx':
        raise CustomException(attr='fileext')

    _cursor = RouteSequenceAPI(dataparser, file)
    result = await _cursor.response()
    return result


# Getroute Route
# http://127.0.0.1:8000/dmsroute
@app.post('/dmsroute', response_model=ResponseDMS)
async def dmsroute(dataparser: DMSRoute):
    try:
        dataparser.dcstart = datetime.datetime.strptime(
            dataparser.dcstart,
            '%Y-%m-%d %H:%M:%S'
        )
    except Exception as e:
        print(e)
        raise CustomException(attr="date")
    _cursor = DMSSequenceAPI(dataparser)
    result = await _cursor.response()
    return result
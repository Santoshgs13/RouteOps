# FastAPI Functions
from fastapi.responses import JSONResponse
from fastapi import status

# Helper Functions
from helpers.excelparser import create_data_model
from helpers.excelparser import excel_deparser
from helpers.adhocortools import pandas_route_optimisation
from helpers.googleortools import route_optimisation
from helpers.mailstream import sendmail
from helpers.jsonparser import data_preprocssing


class PingAPI:
    async def response(self):
        res = {
            "message": "Server up and running"
        }
        return JSONResponse(status_code=status.HTTP_200_OK, content=res)


class RouteSequenceAPI:
    def __init__(self, params, file):
        self.excelfile = file
        self.dcstart = params.dcstart
        self.email = params.email
        if bool(params.dock):
            self.dock = params.dock
        else:
            self.dock = 5

    async def response(self):
        res, status = await self.parseData()
        return JSONResponse(status_code=status, content=res)

    async def parseData(self):
        algodata, rejected_orders, rejected_vehicle = await create_data_model(
            self.excelfile,
            self.dcstart,
            self.dock
        )
        if isinstance(algodata, str):
            message = {
                'message': algodata
            }
            return message, status.HTTP_400_BAD_REQUEST
        if isinstance(algodata, list):
            message = {
                'message': "Column names are not matching according to fields",
                'fields': algodata
            }
            return message, status.HTTP_400_BAD_REQUEST
        _ = await pandas_route_optimisation(algodata, nullflag=False)
        if type(_) is not dict:
            message = {
                'message': _,
                'statuscode': status.HTTP_200_OK
            }
            return message, status.HTTP_200_OK
        drop, rsuccess, rstats, rsummary, runder = await excel_deparser(_)
        await sendmail(
            self.email,
            [
                rejected_orders,
                rejected_vehicle,
                drop,
                rsuccess,
                rstats,
                rsummary,
                runder
            ],
            [
                'rejected_orders.csv',
                'rejected_vehicles.csv',
                'dropped_orders.csv',
                'route_plan.csv',
                'route_stats.csv',
                'route_summary.csv',
                'underutilized_vehicles.csv'
            ]
        )
        message = {
                'message': "Route plan sent to {}".format(self.email),
                'statuscode': status.HTTP_200_OK
        }
        return message, status.HTTP_200_OK


class DMSSequenceAPI:
    def __init__(self, params):
        self.data = params.data
        self.dcstart = params.dcstart
        self.dock = params.dock
        self.loadingTime = params.loadingTime
        self.unloadingTime = params.unloadingTime

    async def response(self):
        res, status = await self.parseData()
        return JSONResponse(status_code=status, content=res)

    async def parseData(self):
        algodata, rejected_orders, rejected_vehicle = await data_preprocssing(
            self.data,
            self.dcstart,
            self.dock,
            self.loadingTime,
            self.unloadingTime
        )
        if isinstance(algodata, str):
            message = {
                'message': algodata
            }
            return message, status.HTTP_400_BAD_REQUEST
        if isinstance(algodata, list):
            message = {
                'message': "Attribute names are not matching according to fields",
                'fields': algodata
            }
            return message, status.HTTP_400_BAD_REQUEST

        _ = await route_optimisation(algodata)
        if type(_) is not dict:
            message = {
                'message': _,
                'statuscode': status.HTTP_200_OK
            }
            return message, status.HTTP_200_OK
        # print(_)
        _['rejectedOrders'] = rejected_orders
        _['rejectedVehicles'] = rejected_vehicle
        message = {
            'data': _,
            'statuscode': status.HTTP_200_OK
        }
        return message, status.HTTP_200_OK

# Import Query libraries
import inspect
from typing import Optional
from typing import Type
from typing import List
from fastapi import Form
from pydantic import BaseModel
from pydantic import create_model
from pydantic.fields import ModelField


def as_form(cls: Type[BaseModel]):
    new_parameters = []

    for field_name, model_field in cls.__fields__.items():
        model_field: ModelField  # type: ignore

        if not model_field.required:
            new_parameters.append(
                inspect.Parameter(
                    model_field.alias,
                    inspect.Parameter.POSITIONAL_ONLY,
                    default=Form(model_field.default),
                    annotation=model_field.outer_type_,
                )
            )
        else:
            new_parameters.append(
                inspect.Parameter(
                    model_field.alias,
                    inspect.Parameter.POSITIONAL_ONLY,
                    default=Form(...),
                    annotation=model_field.outer_type_,
                )
            )

    async def as_form_func(**data):
        return cls(**data)

    sig = inspect.signature(as_form_func)
    sig = sig.replace(parameters=new_parameters)
    as_form_func.__signature__ = sig  # type: ignore
    setattr(cls, 'as_form', as_form_func)
    return cls


# /getroute query body
@as_form
class GetRoute(BaseModel):
    dcstart: str
    email: str
    dock: Optional[int] = 5


class _Orders(BaseModel):  #need to change
    customerCode: int
    customerName: str
    latitude: float
    longitude: float
    deliveryTimeStart: str
    deliveryTimeEnd: str
    orderWeight: int
    requestId: int #added


class _Vehicles(BaseModel):
    registrationId: str
    model: str
    capacity: int
    fixedCost: int
    ratePerKm: int
    freeDistance: int


# /dmsroute query body
class DMSRoute(BaseModel):
    data: dict
    dcstart: str
    loadingTime: Optional[int] = 30
    unloadingTime: Optional[int] = 30
    dock: Optional[int] = 10



# / response doc
class ResponsePing(BaseModel):
    message: str = "Server up and running"


# /getroute response doc
class ResponseRoute(BaseModel):
    message: str = "Route plan sent to <email>"


class _Stats(BaseModel):
    registrationId: str
    totalCost: int
    totalDistance: int
    totalLoad: int
    totalTime: int
    costpload: float
    costpkm: float


class _UnderUtil(BaseModel):
    registrationId: str
    fixedCost: int
    capacity: int
    ratePerKm: int
    freeDistance: int


class _Plan(BaseModel):  #need to change
    registrationId: str
    stopNumber: int
    customerCode: int
    customerName: str
    deliveryTimeStart: str
    deliveryTimeEnd: str
    orderWeight: str
    latitude: float
    longitude: float
    requestId : str


# /dmsroute response doc
class ResponseDMS(BaseModel):
    data: create_model(
            "DData",
            dropNodes=(
                List[_Orders], ... 
            ),
            stats=(
                List[_Stats], ...
            ),
            plan=(
                List[_Plan], ...
            ),
            underutilized=(
                List[_UnderUtil], ...
            ),
            summary=(
                create_model(
                    "Summary",
                    totalCost=(int, ...),
                    totalDistance=(int, ...),
                    totalLoad=(int, ...),
                    totalTime=(int, ...),
                    totalVehicles=(int, ...),
                    avgCostpLoad=(float, ...),
                    avgCostpDistance=(float, ...)
                ), ...
            ),
            rejectedOrders=(
                List[_Orders], ...),
            rejectedVehicles=(
                List[_Vehicles], ...)
        )
    statuscode: int = 200

'''
https://fastapi-contrib.readthedocs.io/en/stable/_modules/fastapi_contrib/exception_handlers.html
'''
# Essential Libraries
from typing import List
from typing import Optional
from typing import Any
from pydantic import EnumError
from pydantic import StrRegexError


class CustomException(Exception):
    def __init__(self, attr: str):
        self.attr = attr


# Important Functions
def parse_error(
    err: Any, field_names: List, raw: bool = True
) -> Optional[dict]:
    """
    Parse single error object (such as pydantic-based or fastapi-based) to dict

    :param err: Error object
    :param field_names: List of names of the field that are already processed
    :param raw: Whether this is a raw error or wrapped pydantic error
    :return: dict with name of the field (or "__all__") and actual message
    """

    if isinstance(err.exc, EnumError):
        permitted_values = ", ".join(
            [f"'{val}'" for val in err.exc.enum_values]
        )
        message = f"Value is not a valid enumeration member; " \
                  f"permitted: {permitted_values}."
    elif isinstance(err.exc, StrRegexError):
        message = "Provided value doesn't match valid format."
    else:
        message = str(err.exc) or ""

    if hasattr(err.exc, "code") and err.exc.code.startswith("error_code"):
        error_code = int(err.exc.code.split(".")[-1])
    else:
        # default error code for non-custom errors is 400
        error_code = 400

    if not raw:
        if len(err.loc_tuple()) == 2:
            if str(err.loc_tuple()[0]) in ["body", "query"]:
                name = err.loc_tuple()[1]
            else:
                name = err.loc_tuple()[0]
        elif len(err.loc_tuple()) == 1:
            if str(err.loc_tuple()[0]) == "body":
                name = "__all__"
            else:
                name = str(err.loc_tuple()[0])
        else:
            name = "__all__"
    else:
        if len(err.loc_tuple()) == 2:
            name = str(err.loc_tuple()[0])
        elif len(err.loc_tuple()) == 1:
            name = str(err.loc_tuple()[0])
        else:
            name = "__all__"

    if name in field_names:
        return None

    if message and not any(
        [message.endswith("."), message.endswith("?"), message.endswith("!")]
    ):
        message = message + "."
    message = message.capitalize()

    return {"name": name, "message": message, "error_code": error_code}


def raw_errors_to_fields(raw_errors: List) -> List[dict]:
    """
    Translates list of raw errors (instances) into list of dicts with name/msg

    :param raw_errors: List with instances of raw error
    :return: List of dicts (1 dict for every raw error)
    """
    fields = []
    for top_err in raw_errors:
        if hasattr(top_err.exc, "raw_errors"):
            for err in top_err.exc.raw_errors:
                # This is a special case when errors happen both in request
                # handling & internal validation
                if isinstance(err, list):
                    err = err[0]
                field_err = parse_error(
                    err,
                    field_names=list(map(lambda x: x["name"], fields)),
                    raw=True,
                )
                if field_err is not None:
                    fields.append(field_err)
        else:
            field_err = parse_error(
                top_err,
                field_names=list(map(lambda x: x["name"], fields)),
                raw=False,
            )
            if field_err is not None:
                fields.append(field_err)
    return fields

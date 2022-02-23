import pandas as pd
import functools
import numpy as np
from io import BytesIO

from helpers.commonfunctions import pandas_difference_time



async def excel_deparser(data):
    drop = pd.DataFrame.from_dict(data['dropped'])
    rsuccess = pd.DataFrame.from_dict(data['success'])
    rstats = pd.DataFrame.from_dict(data['stats'])
    runder = pd.DataFrame.from_dict(data['underutilized'])
    rsummary = []
    rsummary.append(data['summary'])
    rsummary = pd.DataFrame.from_dict(rsummary)
    return drop, rsuccess, rstats, rsummary, runder


async def create_data_model(excelfile, date_obj, docklimit):
    file = BytesIO(excelfile.file.read())
    try:
        orders = pd.read_excel(
            file,
            sheet_name='Orders'
        )
    except Exception as e:
        print(e)
        return "Sheet name: 'Orders' not present", None, None
    try:
        vehicles = pd.read_excel(
            file,
            sheet_name='Vehicles'
        )
    except Exception as e:
        print(e)
        return "Sheet name: 'Vehicles' not present", None, None

    _ = list(orders.columns)
    _lst = ['Customer Code', 'Customer Name', 'Latitude',
            'Longitude', 'Delivery Time Start',
            'Delivery Time End', 'Order Weight'
            ]

    if len(_) != len(_lst) or not functools.reduce(
        lambda i, j: i and j,
        map(
            lambda m, k: m == k,
            _,
            _lst
        ),
        True
    ):
        return _lst, None, None

    _ = list(vehicles.columns)
    _lst = ['Vehicle Id', 'Name', 'Class', 'Capacity', 'Cost']

    if len(_) != len(_lst) or not functools.reduce(
        lambda i, j: i and j,
        map(
            lambda m, k: m == k,
            _,
            _lst
        ),
        True
    ):
        return _lst, None, None

    rejected_orders_frame = pd.DataFrame(columns=orders.columns)
    rejected_vehicle_frame = pd.DataFrame(columns=vehicles.columns)

    data = {}
    data['depot'] = 0
    data['routename'] = []
    data['locations'] = []
    data['customercode'] = []
    data['num_vehicles'] = 0
    data['num_locations'] = 0
    data['unloadtime'] = 30  # Adjust when doing dms task
    data['depot_capacity'] = docklimit
    data['absoluteweight'] = []
    data['vehicle_id'] = []
    data['vehicle_capacities'] = []
    data['vehicle_distances'] = []
    data['time_windows'] = []
    data['startwindow'] = []
    data['endwindow'] = []
    data['dateobj'] = date_obj
    # Adjust when doing dms task
    minimumdistance = 100
    costpkm = 14
    fixedcost = 1300
    extradistance = int((fixedcost/costpkm) - 1)
    totaldistance = minimumdistance + extradistance

    mask = orders['Customer Code'] == -100
    depo = orders[mask]
    orders = orders[~mask]

    mask = (
            orders['Latitude'] == 0
        ) | (
            orders['Longitude'] == 0
        ) | (
            orders['Order Weight'] <= 0
        )
    _ = orders[mask]
    orders = orders[~mask]
    rejected_orders_frame = rejected_orders_frame.append(_)

    mask = (
        orders['Latitude'].isnull()
        ) | (
            orders['Longitude'].isnull()
        ) | (
            orders['Order Weight'].isnull()
        )
    _ = orders[mask]
    orders = orders[~mask]
    rejected_orders_frame = rejected_orders_frame.append(_)

    mask = (
            orders['Customer Code'].isnull()
            ) | (
            orders['Delivery Time End'].isnull()
        )
    _ = orders[mask]
    orders = orders[~mask]
    rejected_orders_frame = rejected_orders_frame.append(_)

    orders = orders.reset_index(drop=True)
    orders['Start'] = orders['Delivery Time Start'].apply(
        pandas_difference_time, objdate=date_obj
    )
    orders['End'] = orders['Delivery Time End'].apply(
        pandas_difference_time, objdate=date_obj
    )

    orders['Customer Code'] = orders['Customer Code'].astype('int')
    orders['Order Weight'] = orders['Order Weight'].astype('int')
    orders['Longitude'] = orders['Longitude'].astype('float')
    orders['Latitude'] = orders['Latitude'].astype('float')
    orders['Start'] = orders['Start'].astype('int')
    orders['End'] = orders['End'].astype('int')
    orders['Delivery Time Start'] = orders['Delivery Time Start'].astype('str')
    orders['Delivery Time End'] = orders['Delivery Time End'].astype('str')
    orders = orders.replace({np.nan: "Null"})

    # Order pumping
    data['routename'] = list(orders['Customer Name'].values)
    data['routename'].insert(0, depo['Customer Name'].values[0])
    data['locations'] = list(zip(orders['Longitude'], orders['Latitude']))
    data['locations'].insert(0, (
            depo['Longitude'].values[0],
            depo['Latitude'].values[0]
        )
    )
    data['customercode'] = list(orders['Customer Code'].values)
    data['customercode'].insert(0, -100)
    data['absoluteweight'] = list(orders['Order Weight'].values)
    data['absoluteweight'].insert(0, 0)
    data['num_locations'] = len(data['locations'])
    data['time_windows'] = list(zip(orders['Start'], orders['End']))
    data['time_windows'].insert(0, (0, 1000))

    mask = (vehicles['Capacity'] <= 0)
    _ = vehicles[mask]
    vehicles = vehicles[~mask]
    rejected_vehicle_frame = rejected_vehicle_frame.append(_)

    mask = (vehicles['Capacity'].isnull())
    _ = vehicles[mask]
    vehicles = vehicles[~mask]
    rejected_vehicle_frame = rejected_vehicle_frame.append(_)

    mask = (vehicles['Cost'] <= 0)
    _ = vehicles[mask]
    vehicles = vehicles[~mask]
    rejected_vehicle_frame = rejected_vehicle_frame.append(_)

    mask = (vehicles['Cost'].isnull())
    _ = vehicles[mask]
    vehicles = vehicles[~mask]
    rejected_vehicle_frame = rejected_vehicle_frame.append(_)

    vehicles = vehicles.replace({None: "Null"})
    # Fix for dms.
    data['vehicle_id'] = list(vehicles['Vehicle Id'].values)
    data['vehicle_cost'] = list(vehicles['Cost'].values)
    data['vehicle_capacities'] = list(vehicles['Capacity'].values)
    data['num_vehicles'] = len(data['vehicle_capacities'])
    data['vehicle_distances'] = [totaldistance for x in range(
            data['num_vehicles']
        )
    ]
    data['startwindow'] = list(orders['Delivery Time Start'].values)
    data['startwindow'].insert(0, 0)
    data['endwindow'] = list(orders['Delivery Time End'].values)
    data['endwindow'].insert(0, 0)
    return data, rejected_orders_frame, rejected_vehicle_frame

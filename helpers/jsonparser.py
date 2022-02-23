from helpers.commonfunctions import calc_difference_time
from operator import itemgetter


async def data_preprocssing(
    rawdata,
    dcstart,
    dock,
    loadingTime,
    unloadingTime
):
    try:
        orders = rawdata['orders']
        orders = sorted(orders, key=itemgetter('orderWeight'), reverse=False) #added

    except Exception as e:
        print(e)
        return "Attribute: 'Orders' not present", None, None
    try:
        vehicles = rawdata['vehicles']
    except Exception as e:
        print(e)
        return "Attribute: 'Vehicles' not present", None, None

    _lst = [                                        #change
            'customerCode',
            'customerName',
            'latitude',
            'longitude',
            'deliveryTimeStart',
            'deliveryTimeEnd',
            'orderWeight',
            'requestId'  #added
        ]
    _lst.sort()
    for order in orders:
        _ = list(order.keys())
        _.sort()
        if _ != _lst:
            return _lst, None, None

    _lst = [
            'registrationId',
            'model',
            'capacity',
            'fixedCost',
            'ratePerKm',
            'freeDistance'
        ]
    _lst.sort()
    for vehicle in vehicles:
        _ = list(vehicle.keys())
        _.sort()

        if _ != _lst:
            return _lst, None, None

    data = {}
    data['depot'] = 0
    data['routename'] = []
    data['locations'] = []
    data['customercode'] = []
    data['num_vehicles'] = 0
    data['num_locations'] = 0
    data['unloadtime'] = unloadingTime
    data['loadingtime'] = loadingTime
    data['depot_capacity'] = dock
    data['absoluteweight'] = []
    data['vehicle_id'] = []
    data['vehicle_capacities'] = []
    data['vehicle_distances'] = []
    data['vehicle_rpkm'] = []
    data['vehicle_cost'] = []
    data['time_windows'] = []
    data['startwindow'] = []
    data['endwindow'] = []
    data['dateobj'] = dcstart
    data['requestId'] = [] #added
    # Adjust when doing dms task

    depo = list(filter(lambda d: d['customerCode'] in [-100], orders))
    orders = list(filter(lambda d: d['customerCode'] not in [-100], orders))

    rejected_orders = []
    for order in orders:
        if order['latitude'] is None or order['longitude'] is None:
            rejected_orders.append(order)
            continue
        if order['orderWeight'] <= 0 or order['orderWeight'] is None:
            rejected_orders.append(order)
            continue
        if order['customerCode'] is None:
            rejected_orders.append(order)
            continue
        if order['deliveryTimeStart'] is None:
            rejected_orders.append(order)
            continue
        if order['deliveryTimeEnd'] is None:
            rejected_orders.append(order)
            continue
        order['Start'] = calc_difference_time(
            order['deliveryTimeStart'], dcstart
        )
        order['End'] = calc_difference_time(
            order['deliveryTimeEnd'], dcstart
        )

        order['customerCode'] = int(order['customerCode'])
        order['orderWeight'] = int(order['orderWeight'])
        order['longitude'] = float(order['longitude'])
        order['latitude'] = float(order['latitude'])
        order['Start'] = int(order['Start'])
        order['End'] = int(order['End'])
        order['deliveryTimeStart'] = str(order['deliveryTimeStart'])
        order['deliveryTimeEnd'] = str(order['deliveryTimeEnd'])

        data['routename'].append(order['customerName'])
        data['locations'].append((order['longitude'], order['latitude']))
        data['customercode'].append(order['customerCode'])
        data['absoluteweight'].append(order['orderWeight'])
        data['time_windows'].append((order['Start'], order['End']))
        data['startwindow'].append(order['deliveryTimeStart'])
        data['endwindow'].append(order['deliveryTimeEnd'])
        data['requestId'].append(order['requestId']) #added

    data['routename'].insert(0, depo[0]['customerName'])
    data['locations'].insert(
        0,
        (depo[0]['longitude'], depo[0]['latitude'])
    )
    data['requestId'].insert(0, depo[0]['requestId']) #added
    data['customercode'].insert(0, -100)
    data['absoluteweight'].insert(0, 0)
    data['time_windows'].insert(0, (0, 1000))
    data['startwindow'].insert(0, 0)
    data['endwindow'].insert(0, 0)
    data['num_locations'] = len(data['locations'])
    rejected_vehicle = []
    for vehicle in vehicles:
        if vehicle['capacity'] <= 0 or vehicle['capacity'] is None:
            rejected_vehicle.append(vehicle)
            continue
        if vehicle['fixedCost'] <= 0 or vehicle['fixedCost'] is None:
            rejected_vehicle.append(vehicle)
            continue
        if vehicle['ratePerKm'] <= 0 or vehicle['ratePerKm'] is None:
            rejected_vehicle.append(vehicle)
            continue
        if vehicle['freeDistance'] <= 0 or vehicle['freeDistance'] is None:
            rejected_vehicle.append(vehicle)
            continue
        vehicle['registrationId'] = str(vehicle['registrationId'])
        vehicle['fixedCost'] = int(vehicle['fixedCost'])
        vehicle['capacity'] = int(vehicle['capacity'])
        vehicle['ratePerKm'] = int(vehicle['ratePerKm'])
        data['vehicle_id'].append(vehicle['registrationId'])
        data['vehicle_cost'].append(vehicle['fixedCost'])
        data['vehicle_capacities'].append(vehicle['capacity'])
        data['vehicle_rpkm'].append(vehicle['ratePerKm'])
        # extradistance = int((vehicle['fixedCost']/vehicle['ratePerKm']) - 1)
        totaldistance = vehicle['freeDistance']  # + extradistance #check
        data['vehicle_distances'].append(totaldistance)
    data['num_vehicles'] = len(data['vehicle_capacities'])
    # print(data)
    return data, rejected_orders, rejected_vehicle

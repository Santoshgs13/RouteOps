from math import radians, cos, sin, asin, sqrt
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from helpers.commonfunctions import google_distance_time
from functools import partial
import numpy as np
import datetime


async def haversine_distance_matrix(locations):
    """Creates callback to return distance between points."""
    distances = {}
    for from_counter, from_node in enumerate(locations):
        distances[from_counter] = {}
        for to_counter, to_node in enumerate(locations):
            if from_counter == to_counter:
                distances[from_counter][to_counter] = 0
            else:
                # Haversine distance
                lon1, lat1, lon2, lat2 = map(
                    radians,
                    [
                        from_node[0], from_node[1],
                        to_node[0], to_node[1]
                    ]
                )
                dlon = lon2 - lon1
                dlat = lat2 - lat1
                a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                c = 2 * asin(sqrt(a))
                r = 6371
                distances[from_counter][to_counter] = (c * r)
    return distances


def create_time_evaluator(data):

    def service_time(data, node):
        return data['unloadtime']

    def travel_time(data, from_node, to_node):
        if from_node == to_node:
            travel_time = 0
        else:
            travel_time = data['time_matrix'][from_node][to_node]
        return travel_time

    _total_time = {}
    for from_node in range(data['num_locations']):
        _total_time[from_node] = {}
        for to_node in range(data['num_locations']):
            if from_node == to_node:
                _total_time[from_node][to_node] = 0
            else:
                _total_time[from_node][to_node] = int(
                    service_time(data, from_node) + travel_time(
                        data, from_node, to_node))

    def time_evaluator(manager, from_node, to_node):
        return _total_time[manager.IndexToNode(from_node)][manager.IndexToNode(
            to_node)]

    return time_evaluator


def add_time_window_constraints(routing, manager, data, time_evaluator_index):
    time = 'Time'
    horizon = 0
    routing.AddDimension(
        time_evaluator_index,
        horizon,
        10000,
        False,
        time)
    time_dimension = routing.GetDimensionOrDie(time)
    for location_idx, time_window in enumerate(data['time_windows']):
        if location_idx == 0:
            continue
        index = manager.NodeToIndex(location_idx)
        time_dimension.CumulVar(index).SetRange(
            int(time_window[0]),
            int(time_window[1])
        )
        routing.AddToAssignment(time_dimension.SlackVar(index))
    # Add time window constraints for each vehicle start node
    # and 'copy' the slack var in the solution object
    # (aka Assignment) to print it

    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        time_dimension.CumulVar(index).SetRange(data['time_windows'][0][0],
                                                data['time_windows'][0][1])
        routing.AddToAssignment(time_dimension.SlackVar(index))
    for i in range(data['num_vehicles']):
        routing.AddVariableMinimizedByFinalizer(
            time_dimension.CumulVar(routing.Start(i)))
        routing.AddVariableMinimizedByFinalizer(
            time_dimension.CumulVar(routing.End(i)))
    solver = routing.solver()
    intervals = []
    for i in range(data['num_vehicles']):
        # Add time windows at start of routes
        intervals.append(
            solver.FixedDurationIntervalVar(
                time_dimension.CumulVar(routing.Start(i)),
                data['loadingtime'], 'depot_interval'))
    depot_usage = [1 for i in range(len(intervals))]
    solver.Add(
        solver.Cumulative(
            intervals,
            depot_usage,
            data['depot_capacity'],
            'depot'
        )
    )


async def return_solution(data, manager, routing, solution, nullflag=True):
    returndata = {}
    dropped_nodes = []
    for node in range(routing.Size()):
        if routing.IsStart(node) or routing.IsEnd(node):
            continue
        if solution.Value(routing.NextVar(node)) == node:
            dropped_nodes.append(manager.IndexToNode(node))
    returndata['dropNodes'] = []
    for inode in dropped_nodes:
        _ = {}
        _['customerCode'] = data['customercode'][inode]
        _['customerName'] = data['routename'][inode]
        _['deliveryTimeStart'] = data['startwindow'][inode]
        _['deliveryTimeEnd'] = data['endwindow'][inode]
        _['orderWeight'] = data['absoluteweight'][inode]
        _['latitude'] = data['locations'][inode][1]
        _['longitude'] = data['locations'][inode][0]
        _['requestId'] = data['requestId'][inode] #added
        returndata['dropNodes'].append(_)
    time_dimension = routing.GetDimensionOrDie('Time')
    total_time = 0
    total_distance = 0
    total_load = 0
    total_cost = 0
    total_vehicles = data['num_vehicles']
    returndata['stats'] = []
    returndata['plan'] = []
    returndata['underutilized'] = []

    for vehicle_id in range(data['num_vehicles']):
        returntemp = []
        index = routing.Start(vehicle_id)
        route_load = 0
        route_distance = 0
        stopitr = 0
        while not routing.IsEnd(index):
            _ = {}
            previous_index = index
            node_index = manager.IndexToNode(index)
            route_load += data['absoluteweight'][node_index]
            time_var = time_dimension.CumulVar(index)
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id)
            _['registrationId'] = data['vehicle_id'][vehicle_id]
            _['stopNumber'] = stopitr
            if stopitr == 0:
                _service = data['loadingtime']
                _loc = data['locations'][node_index]
                _originlocation = data['locations'][node_index]
                _origincode = data['customercode'][node_index]
                _originname = data['routename'][node_index]
                _dtime = solution.Min(time_var)
                _distance = 0
            else:
                _dist, _time = await google_distance_time(
                        _loc,
                        data['locations'][node_index]
                    )
                _dtime = _time + _dtime + _service
                _distance = _dist + _distance
                _loc = data['locations'][node_index]
                _service = data['unloadtime']
            _['customerCode'] = data['customercode'][node_index]
            _['customerName'] = data['routename'][node_index]
            _['deliveryTimeStart'] = datetime.datetime.strptime(
                str(data['dateobj']),
                '%Y-%m-%d %H:%M:%S'
            ) + datetime.timedelta(minutes=_dtime)
            _['deliveryTimeStart'] = _['deliveryTimeStart'].strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            _['deliveryTimeEnd'] = datetime.datetime.strptime(
                str(data['dateobj']),
                '%Y-%m-%d %H:%M:%S'
            ) + datetime.timedelta(
                minutes=_dtime+_service
            )
            _['deliveryTimeEnd'] = _['deliveryTimeEnd'].strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            _['orderWeight'] = data['absoluteweight'][node_index]
            _['latitude'] = data['locations'][node_index][1]
            _['longitude'] = data['locations'][node_index][0]
            _['requestId'] = data['requestId'][node_index] #added
            returntemp.append(_)
            stopitr += 1
        _dist, _time = await google_distance_time(
                        data['locations'][node_index],
                        _originlocation
                    )
        _dtime = _time + _dtime + _service
        _distance = _dist + _distance
        _ = {}
        _['registrationId'] = data['vehicle_id'][vehicle_id]
        _['stopNumber'] = stopitr
        _['customerCode'] = _origincode
        _['customerName'] = _originname
        _['deliveryTimeStart'] = datetime.datetime.strptime(
                str(data['dateobj']),
                '%Y-%m-%d %H:%M:%S'
            ) + datetime.timedelta(minutes=_dtime)
        _['deliveryTimeStart'] = _['deliveryTimeStart'].strftime(
                '%Y-%m-%d %H:%M:%S'
            )
        _['deliveryTimeEnd'] = datetime.datetime.strptime(
                str(data['dateobj']),
                '%Y-%m-%d %H:%M:%S'
            ) + datetime.timedelta(
                minutes=_dtime+_service
            )
        _['deliveryTimeEnd'] = _['deliveryTimeEnd'].strftime(
                '%Y-%m-%d %H:%M:%S'
            )
        _['orderWeight'] = 0
        _['latitude'] = _originlocation[1]
        _['longitude'] = _originlocation[0]
        # _['invoiceId'] = data['invoiceId'][inode] #added
        returntemp.append(_)
        time_var = time_dimension.CumulVar(index)
        _2 = {}
        if _distance == 0:
            total_vehicles = total_vehicles - 1
            monetorycost = 0
            _3 = {}
            _3['registrationId'] = data['vehicle_id'][vehicle_id]
            _3['fixedCost'] = data['vehicle_cost'][vehicle_id]
            _3['capacity'] = data['vehicle_capacities'][vehicle_id]
            _3['ratePerKm'] = data['vehicle_rpkm'][vehicle_id]
            _3['freeDistance'] = data['vehicle_distances'][vehicle_id]
            returndata['underutilized'].append(_3)
        elif _distance < data['vehicle_distances'][vehicle_id]:
            monetorycost = data['vehicle_cost'][vehicle_id]
            _2['registrationId'] = data['vehicle_id'][vehicle_id]
            _2['totalCost'] = monetorycost
            _2['totalDistance'] = _distance
            _2['totalLoad'] = route_load
            _2['totalTime'] = _dtime
            _2['costpload'] = _2['totalCost']/_2['totalLoad']
            _2['costpkm'] = _2['totalCost']/_2['totalDistance']
            returndata['stats'].append(_2)
            returndata['plan'] = returndata['plan'] + returntemp
        else:
            monetorycost = data['vehicle_cost'][vehicle_id] + (
                    route_distance - data['vehicle_distances'][vehicle_id]
                ) * data['vehicle_rpkm'][vehicle_id]
            _2['registrationId'] = data['vehicle_id'][vehicle_id]
            _2['totalCost'] = monetorycost
            _2['totalDistance'] = _distance
            _2['totalLoad'] = route_load
            _2['totalTime'] = _dtime
            _2['costpload'] = _2['totalCost']/_2['totalLoad']
            _2['costpkm'] = _2['totalCost']/_2['totalDistance']
            returndata['stats'].append(_2)
            returndata['plan'] = returndata['plan'] + returntemp
        total_time += _dtime
        total_distance += _distance
        total_load += route_load
        total_cost += monetorycost
        total_time += solution.Min(time_var)
    returndata['summary'] = {
        'totalCost': None,
        'totalDistance': None,
        'totalLoad': None,
        'totalTime': None,
        'totalVehicles': None,
        'avgCostpLoad': None,
        'avgCostpDistance': None
    }
    if total_load == 0 or total_distance == 0:
        return returndata
    returndata['summary']['totalCost'] = total_cost
    returndata['summary']['totalDistance'] = total_distance
    returndata['summary']['totalLoad'] = total_load
    returndata['summary']['totalTime'] = total_time
    returndata['summary']['totalVehicles'] = total_vehicles
    returndata['summary']['avgCostpLoad'] = total_cost / total_load
    returndata['summary']['avgCostpDistance'] = total_cost / total_distance
    return returndata


async def route_optimisation(data):
    try:
        manager = pywrapcp.RoutingIndexManager(
            data['num_locations'],
            data['num_vehicles'],
            data['depot']
        )
        routing = pywrapcp.RoutingModel(manager)

        distance_matrix = await haversine_distance_matrix(data['locations'])
        dmatrix = []
        for key, value in distance_matrix.items():
            _ = []
            for k, v in value.items():
                _.append(v)
            dmatrix.append(_)
        distance_matrix = np.array(dmatrix)
        data['time_matrix'] = distance_matrix * 6

        time_evaluator_index = routing.RegisterTransitCallback(
            partial(create_time_evaluator(data), manager))

        add_time_window_constraints(
            routing,
            manager,
            data,
            time_evaluator_index
        )

        def demand_callback(from_index):
            from_node = manager.IndexToNode(from_index)
            return data['absoluteweight'][from_node]

        demand_callback_index = routing.RegisterUnaryTransitCallback(
            demand_callback
        )
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,  # null capacity slack
            data['vehicle_capacities'],  # vehicle maximum capacities
            True,  # start cumul to zero
            'Capacity'
        )

        penalty = 1000
        for node in range(1, len(distance_matrix)):
            routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

        def distance_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return distance_matrix[from_node][to_node]

        transit_callback_index = routing.RegisterTransitCallback(
            distance_callback
        )
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        dimension_name = 'Distance'
        routing.AddDimensionWithVehicleCapacity(
            transit_callback_index,
            0,  # no slack
            data['vehicle_distances'],  # vehicle maximum travel distance
            True,  # start cumul to zero
            dimension_name)
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
        solution = routing.SolveWithParameters(search_parameters)

        if solution:
            return await return_solution(
                data,
                manager,
                routing,
                solution
            )
        else:
            return "No solution"
    # except Exception as e:
    #     print(e)
    #     return "Kindly contact Analytics Team with data and params."

    except BaseException as err:
        print(err)


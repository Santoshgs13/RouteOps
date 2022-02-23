# Other Helper Libraries
from math import radians, cos, sin, asin, sqrt
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from functools import partial
from six.moves import xrange
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
    """Creates callback to get total times between locations."""

    def service_time(data, node):
        """Gets the service time for the specified location."""
        return data['unloadtime']

    def travel_time(data, from_node, to_node):
        """Gets the travel times between two locations."""
        if from_node == to_node:
            travel_time = 0
        else:
            travel_time = data['time_matrix'][from_node][to_node]
        return travel_time

    _total_time = {}
    # precompute total time to have time callback in O(1)
    for from_node in xrange(data['num_locations']):
        _total_time[from_node] = {}
        for to_node in xrange(data['num_locations']):
            if from_node == to_node:
                _total_time[from_node][to_node] = 0
            else:
                _total_time[from_node][to_node] = int(
                    service_time(data, from_node) + travel_time(
                        data, from_node, to_node))

    def time_evaluator(manager, from_node, to_node):
        """Returns the total time between the two nodes"""
        return _total_time[manager.IndexToNode(from_node)][manager.IndexToNode(
            to_node)]

    return time_evaluator


def add_time_window_constraints(routing, manager, data, time_evaluator_index):
    """Add Global Span constraint"""
    time = 'Time'
    horizon = 0
    routing.AddDimension(
        time_evaluator_index,
        horizon,  # allow waiting time
        10000,  # maximum time per vehicle
        False,
        time)
    time_dimension = routing.GetDimensionOrDie(time)
    # Add time window constraints for each location except depot
    # and 'copy' the slack var in the solution object
    # (aka Assignment) to print it
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

    for vehicle_id in xrange(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        time_dimension.CumulVar(index).SetRange(data['time_windows'][0][0],
                                                data['time_windows'][0][1])
        routing.AddToAssignment(time_dimension.SlackVar(index))
    for i in xrange(data['num_vehicles']):
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
                data['unloadtime'], 'depot_interval'))
    depot_usage = [1 for i in range(len(intervals))]
    solver.Add(
        solver.Cumulative(
            intervals,
            depot_usage,
            data['depot_capacity'],
            'depot'
        )
    )


async def return_solution_excel(data, manager, routing, solution, nullflag=True):
    returndata = {}
    dropped_nodes = []
    for node in range(routing.Size()):
        if routing.IsStart(node) or routing.IsEnd(node):
            continue
        if solution.Value(routing.NextVar(node)) == node:
            dropped_nodes.append(manager.IndexToNode(node))
    returndata['dropped'] = []
    _ = {}
    _['Customer Code'] = None
    _['Customer Name'] = None
    _['Delivery Time Start'] = None
    _['Delivery Time End'] = None
    _['Order Weight'] = None
    if nullflag is True:
        returndata['dropped'].append(_)
    for inode in dropped_nodes:
        _ = {}
        _['Customer Code'] = data['customercode'][inode]
        _['Customer Name'] = data['routename'][inode]
        _['Delivery Time Start'] = data['startwindow'][inode]
        _['Delivery Time End'] = data['endwindow'][inode]
        _['Order Weight'] = data['absoluteweight'][inode]
        returndata['dropped'].append(_)
    time_dimension = routing.GetDimensionOrDie('Time')
    total_time = 0
    total_distance = 0
    total_load = 0
    total_cost = 0
    total_vehicles = data['num_vehicles']
    returndata['success'] = []
    returndata['stats'] = []
    _2 = {}
    _2['Vehicle Id'] = None
    _2['Total Cost'] = None
    _2['Total Distance'] = None
    _2['Total Load'] = None
    _2['Total Time'] = None
    _2['Cost / load'] = None
    _2['Cost / km'] = None
    if nullflag is True:
        returndata['stats'].append(_2)
    returndata['underutilized'] = []
    _3 = {}
    _3['Vehicle Id'] = None
    if nullflag is True:
        returndata['underutilized'].append(_3)
    _ = {}
    _['Vehicle Id'] = None
    _['Stop Number'] = None
    _['Customer Code'] = None
    _['Customer Name'] = None
    _['Delivery Time Start'] = None
    _['Delivery Time End'] = None
    _['Order Weight'] = None
    returntemp = []
    if nullflag is True:
        returntemp.append(_)
    returndata['success'] = returndata['success'] + returntemp
    for vehicle_id in range(data['num_vehicles']):
        returntemp = []
        index = routing.Start(vehicle_id)
        plan_output = 'Route for vehicle {}:\n'.format(vehicle_id)
        route_load = 0
        prev_load = 0
        route_distance = 0
        stopitr = 0
        while not routing.IsEnd(index):
            _ = {}
            previous_index = index
            node_index = manager.IndexToNode(index)
            route_load += data['absoluteweight'][node_index]
            time_var = time_dimension.CumulVar(index)
            plan_output += '{} [Time({},{}), '.format(
                manager.IndexToNode(index), solution.Min(time_var),
                solution.Max(time_var)+data['unloadtime'])
            plan_output += 'Load({},{})] -> '.format(prev_load, route_load)
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id)
            prev_load = route_load
            _['Vehicle Id'] = data['vehicle_id'][vehicle_id]
            _['Stop Number'] = stopitr
            stopitr += 1
            _['Customer Code'] = data['customercode'][node_index]
            _['Customer Name'] = data['routename'][node_index]
            _['Delivery Time Start'] = datetime.datetime.strptime(
                str(data['dateobj']),
                '%Y-%m-%d %H:%M:%S'
            ) + datetime.timedelta(minutes=solution.Min(time_var))
            _['Delivery Time Start'] = _['Delivery Time Start'].strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            _['Delivery Time End'] = datetime.datetime.strptime(
                str(data['dateobj']),
                '%Y-%m-%d %H:%M:%S'
            ) + datetime.timedelta(
                minutes=solution.Min(time_var)+data['unloadtime']
            )
            _['Delivery Time End'] = _['Delivery Time End'].strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            _['Order Weight'] = data['absoluteweight'][node_index]
            returntemp.append(_)
        time_var = time_dimension.CumulVar(index)
        plan_output += '{} [Time({},{}), '.format(
            manager.IndexToNode(index),
            solution.Min(time_var),
            solution.Max(time_var)+data['unloadtime'])
        plan_output += 'Load({},{})]\n\n'.format(prev_load, route_load)
        plan_output += 'Load of the route: {}\n'.format(route_load)
        plan_output += 'Time of the route: {} min\n'.format(
            solution.Min(time_var))
        plan_output += 'Distance of the route: {} km\n'.format(route_distance)
        _2 = {}
        if route_distance == 0:
            total_vehicles = total_vehicles - 1
            monetorycost = 0
            _3 = {}
            _3['Vehicle Id'] = data['vehicle_id'][vehicle_id]
            returndata['underutilized'].append(_3)
        elif route_distance < 100:
            monetorycost = data['vehicle_cost'][vehicle_id]
            _2['Vehicle Id'] = data['vehicle_id'][vehicle_id]
            _2['Total Cost'] = monetorycost
            _2['Total Distance'] = route_distance
            _2['Total Load'] = route_load
            _2['Total Time'] = solution.Min(time_var)
            _2['Cost / load'] = _2['Total Cost']/_2['Total Load']
            _2['Cost / km'] = _2['Total Cost']/_2['Total Distance']
            returndata['stats'].append(_2)
            returndata['success'] = returndata['success'] + returntemp
        else:
            monetorycost = data['vehicle_cost'][vehicle_id] + (
                    route_distance - 100
                ) * 14
            _2['Vehicle Id'] = data['vehicle_id'][vehicle_id]
            _2['Total Cost'] = monetorycost
            _2['Total Distance'] = route_distance
            _2['Total Load'] = route_load
            _2['Total Time'] = solution.Min(time_var)
            _2['Cost / load'] = _2['Total Cost']/_2['Total Load']
            _2['Cost / km'] = _2['Total Cost']/_2['Total Distance']
            returndata['stats'].append(_2)
            returndata['success'] = returndata['success'] + returntemp
        plan_output += 'Montory Cost of the route: {} Rs\n'.format(
            monetorycost
        )
        total_time += solution.Min(time_var)
        total_distance += route_distance
        total_load += route_load
        total_cost += monetorycost
    returndata['summary'] = {
        'Total Cost': None,
        'Total Distance': None,
        'Total Load': None,
        'Total Vehicles': None,
        'Avg Cost / Load': None,
        'Avg Cost / Distance': None
    }
    if total_load == 0 or total_distance == 0:
        return returndata
    returndata['summary']['Total Cost'] = total_cost
    returndata['summary']['Total Distance'] = total_distance
    returndata['summary']['Total Load'] = total_load
    returndata['summary']['Total Time'] = total_time
    returndata['summary']['Total Vehicles'] = total_vehicles
    returndata['summary']['Avg Cost / Load'] = total_cost / total_load
    returndata['summary']['Avg Cost / Distance'] = total_cost / total_distance
    return returndata


async def pandas_route_optimisation(data, nullflag=True):
    try:
        """Entry point of the program."""

        # Create the routing index manager.
        manager = pywrapcp.RoutingIndexManager(
            data['num_locations'],
            data['num_vehicles'],
            data['depot']
        )

        # Create Routing Model.
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
            """Returns the demand of the node."""
            # Convert from routing variable Index to demands NodeIndex.
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

        '''
        def crate_callback(from_index):
            """Returns the demand of the node."""
            # Convert from routing variable Index to demands NodeIndex.
            from_node = manager.IndexToNode(from_index)
            return data['demandcrate'][from_node]
        crate_callback_index = routing.RegisterUnaryTransitCallback(
            crate_callback
        )
        routing.AddDimensionWithVehicleCapacity(
            crate_callback_index,
            0,  # null capacity slack
            data['maxcrate'],  # vehicle maximum capacities
            True,  # start cumul to zero
            'Crate'
        )
        '''
        penalty = 1000
        for node in range(1, len(distance_matrix)):
            routing.AddDisjunction([manager.NodeToIndex(node)], penalty)
        # --------------------

        def distance_callback(from_index, to_index):
            """Returns the distance between the two nodes."""
            # Convert from routing variable Index to distance matrix NodeIndex.
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
        # distance_dimension = routing.GetDimensionOrDie(dimension_name)

        # Setting first solution heuristic.
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
        # search_parameters.local_search_metaheuristic = (
        #    routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
        # search_parameters.time_limit.FromSeconds(1)

        # Solve the problem.
        solution = routing.SolveWithParameters(search_parameters)

        # Print solution on console.
        if solution:
            return await return_solution_excel(
                data,
                manager,
                routing,
                solution,
                nullflag
            )
        else:
            return "No solution"
    except Exception as e:
        print(e)
        return "Kindly contact Analytics Team with file and params."

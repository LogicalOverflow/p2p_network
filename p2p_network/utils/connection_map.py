import json
from copy import deepcopy
from collections import Hashable


def get_node_index(node):
    return ':'.join('{:02x}'.format(e) for e in node)


def get_node_bytes(node_string):
    return bytes(map(lambda x: int(x, 16), node_string.split(':')))


class ConnectionMap:
    """
    class to manage list of all connection in a network
    adding to maps returns a new map, containing all connections in at least on of the two maps
    subtracting two maps returns a list of all connections in one of the maps but not in the others
    nodes returned and accepted are bytes or bytearrays
    """

    @classmethod
    def conn_mgr_from_json(cls, json_string):
        json_data = json.loads(json_string)
        connections = json_data['connections']
        nodes = json_data['nodes']
        return ConnectionMap(nodes, connections)

    def __init__(self, nodes=None, connections=None):
        self.nodes = nodes if isinstance(nodes, list) else []
        self.connections = connections if isinstance(connections, dict) else {}

    def __add__(self, other):
        map_sum = ConnectionMap()
        for node in self.connections:
            map_sum.nodes.append(node)
            map_sum.connections[node] = self.connections[node]
        for node in other.connections:
            if node not in map_sum.nodes:
                map_sum.nodes.append(node)
                map_sum.connections[node] = other.connections[node]
            else:
                map_sum.connections[node].extend(
                    add_node for add_node in other.connections[node] if add_node not in self.connections[node])

        return map_sum

    def __sub__(self, other):
        conn_diff = []
        for map_a, map_b in [(self, other), (other, self)]:
            for node_a in map_a.connections:
                for node_b in map_a.connections[node_a]:
                    if node_a not in map_b.nodes or node_b not in map_b.nodes or not map_b.direct(node_a, node_b):
                        conn_diff.append((get_node_bytes(node_a), get_node_bytes(node_b)))
        return conn_diff

    def direct(self, node_a, node_b):
        node_a = get_node_index(node_a)
        node_b = get_node_index(node_b)

        if node_a in self.connections[node_b] or node_b in self.connections[node_a]:
            return True
        return False

    def to_json_string(self):
        return json.dumps({'nodes': self.nodes, 'connections': self.connections})

    def add_connection(self, node_a, node_b):
        node_a = get_node_index(node_a)
        node_b = get_node_index(node_b)

        if not (isinstance(node_a, Hashable) and isinstance(node_b, Hashable)):
            return False

        if node_a not in self.nodes:
            self.nodes.append(node_a)
            self.connections[node_a] = []
        if node_b not in self.nodes:
            self.nodes.append(node_b)
            self.connections[node_b] = []

        if node_b not in self.connections[node_a]:
            self.connections[node_a].append(node_b)
        if node_a not in self.connections[node_b]:
            self.connections[node_b].append(node_a)
        return True

    def get_connection(self, node_a, node_b):
        node_a = get_node_index(node_a)
        node_b = get_node_index(node_b)

        paths = {node_a: [get_node_bytes(node_a)]}

        while True:
            new_paths = {}
            for end_node in paths:
                node_connections = deepcopy(self.connections[end_node])
                new_paths[end_node] = paths[end_node]
                for node in node_connections:
                    if node not in paths:
                        new_paths[node] = paths[end_node] + [get_node_bytes(node)]
            if set(paths.keys()) == set(new_paths.keys()):
                return None

            paths = new_paths
            if node_b in paths:
                break

        return paths[node_b]

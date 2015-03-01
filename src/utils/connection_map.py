import json
from copy import deepcopy


class ConnectionMap():
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

    def defaulting(self):
        self.add_connection('node_0', 'node_1')
        self.add_connection('node_1', 'node_2')
        self.add_connection('node_2', 'node_3')
        self.add_connection('node_3', 'node_4')
        self.add_connection('node_2', 'node_5')
        self.add_connection('node_5', 'node_6')
        self.add_connection('node_6', 'node_4')

    def to_json_string(self):
        return json.dumps({'nodes': self.nodes, 'connections': self.connections})

    def add_connection(self, node_a, node_b):
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

    def get_connection(self, node_a, node_b):
        paths = {node_a: [node_a]}

        while True:
            new_paths = {}
            for end_node in paths:
                node_connections = deepcopy(self.connections[end_node])
                new_paths[end_node] = paths[end_node]
                for node in node_connections:
                    if node not in paths:
                        new_paths[node] = paths[end_node] + [node]
            if set(paths.keys()) == set(new_paths.keys()):
                return None

            paths = new_paths
            if node_b in paths:
                break

        return paths[node_b]

import socket

from p2p_network.connection_mgr import ConnectionMgr


class Node:
    def __init__(self, node_ip, node_port, client_id):
        node_ip = socket.gethostbyname(node_ip)
        node_address = (node_ip, node_port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(node_address)

        self.conn_mgr = ConnectionMgr(s, client_id)

    def connect_to_node(self, partner, partner_port):
        if not isinstance(partner_port, int):
            return False
        try:
            partner_ip = socket.gethostbyname(partner)
        except socket.gaierror:
            return False

        return self.conn_mgr.connect((partner_ip, partner_port))

    def send_msg(self, receiver_id, byte_msg, msg_type='string'):
        self.conn_mgr.send_msg(receiver_id, byte_msg, msg_type)

    def send_broadcast(self, broadcast, msg_type='string'):
        self.conn_mgr.send_broadcast(broadcast, msg_type)
        pass
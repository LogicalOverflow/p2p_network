import socket

from p2p_network.connection_mgr import ConnectionMgr


class Node:
    def __init__(self, node_ip, node_port, client_id):
        """
        Network nodes

        :param string node_ip: The Ip of the node you're creating
        :param int node_port: The Port of the node you're creating
        :param int client_id: The client id of the node you're creating
        """
        node_ip = socket.gethostbyname(node_ip)
        node_address = (node_ip, node_port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(node_address)

        self.conn_mgr = ConnectionMgr(s, client_id)

    def connect_to_node(self, partner, partner_port):
        """
        Connect the node to another node

        :param string partner: The address of the node that this node should connect to
        :param int partner_port: The port of the node that this node should connect to
        """
        if not isinstance(partner_port, int):
            return False
        try:
            partner_ip = socket.gethostbyname(partner)
        except socket.gaierror:
            return False

        return self.conn_mgr.connect((partner_ip, partner_port))

    def send_msg(self, receiver_id, byte_msg, msg_type='string'):
        """
        Send a broadcast to all connected nodes

        :param msg: Data to send
        :param msg_type: Type of the data. See :class:`p2p_network.connection_mgr.ConnectionMgr` 
        """
        self.conn_mgr.send_msg(receiver_id, byte_msg, msg_type)

    def send_broadcast(self, broadcast, msg_type='string'):
        """
        Send a broadcast to all connected nodes

        :param msg: Data to send
        :param msg_type: Type of the data. See :class:`p2p_network.connection_mgr.ConnectionMgr` 
        """
        self.conn_mgr.send_broadcast(broadcast, msg_type)
        pass
import socket
import threading

from time import sleep
from connection_mgr import ConnectionMgr


class Node():
    def __init__(self, node_ip, node_port, client_id):
        node_address = (node_ip, node_port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(node_address)

        self.conn_mgr = ConnectionMgr(s, client_id)
        self.msg_q = self.conn_mgr.MSG_Q

    def connect_to_node(self, partner_ip, partner_port):
        if not isinstance(partner_port, int):
            return False
        try:
            socket.inet_aton(partner_ip)
        except OSError:
            return False

        return self.conn_mgr.connect((partner_ip, partner_port))

    def send_msg(self, receiver_id, byte_msg):
        self.conn_mgr.send_msg(receiver_id, byte_msg)

    def send_broadcast(self, broadcast):
        # self.conn_mgr.send_msg(self.conn_mgr.BROADCAST_ID, broadcast)
        pass


def new_message_callback(sender_id, msg, msg_type):
    print('< RECEIVED \'{0}\' from'.format(msg.decode('utf-8')), sender_id)


def sending_callback(receiver_id, msg, msg_type):
    print('> SENDING  \'{0}\' at  '.format(msg.decode('utf-8')), receiver_id)


def q_cleaner(q):
    while True:
        q.get(block=True)


if __name__ == '__main__':
    client_id_number = int(input('CLIENT  ID  : ') or '1')
    CLIENT_ID = bytes([client_id_number])
    print(CLIENT_ID)
    CLIENT_PORT = 13370 + client_id_number
    print(CLIENT_PORT)
    PARTNER_PORT = 13370 + int(input('PARTNER ID  : ') or '1')
    print(PARTNER_PORT)

    CLIENT_IP = input('CLIENT  IP  : ') or '127.0.0.1'
    print(CLIENT_IP)
    PARTNER_IP = input('PARTNER IP  : ') or '127.0.0.1'
    print(PARTNER_IP)

    node = Node(CLIENT_IP, CLIENT_PORT, CLIENT_ID)
    node.connect_to_node(PARTNER_IP, PARTNER_PORT)

    node.conn_mgr.CALLBACKS['message'] = new_message_callback
    node.conn_mgr.CALLBACKS['sending'] = sending_callback

    q_cleaner_thread = threading.Thread(target=q_cleaner, args=(node.msg_q, ))
    q_cleaner_thread.start()

    sleep(1)

    print('UP AND RUNNING')
    while True:
        send_data = input()
        target_id = bytes([int(send_data[0])])
        msg_str = send_data[2:]
        if target_id == 0:
            node.send_broadcast(msg_str.encode('utf-8'))
        else:
            node.send_msg(target_id, msg_str.encode('utf-8'))
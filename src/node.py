import socket
import multiprocessing as mp

from src.connection_mgr import ConnectionMgr
from time import sleep


PORT = 13379

START_IP = '127.0.0.1'
CLIENT_ID = bytes([0, 0, 0, 0])


class Node():
    def __init__(self, node_ip, node_port, client_id):
        node_address = (node_ip, node_port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(node_address)

        self.msg_q = mp.Queue()
        self.conn_mgr = ConnectionMgr(s, client_id, self.msg_q)

        self.conn_mgr.new_message_callback = self.new_message_callback
        self.conn_mgr.forward_callback = self.forward_callback
        self.conn_mgr.sending_callback = self.forward_callback

    def connect_to_node(self, partner_ip, partner_port):
        if not isinstance(partner_port, int):
            return False
        try:
            socket.inet_aton(partner_ip)
        except OSError:
            return False

        return self.conn_mgr.connect((partner_ip, partner_port))

    def new_message_callback(self, sender_id, msg):
        pass

    def forward_callback(self, sender_id, receiver_id, msg):
        pass

    def sending_callback(self, receiver_id, msg):
        print('> SENDING \'{0}\' at'.format(msg), receiver_id)

    def send_msg(self, receiver_id, byte_msg):
        self.conn_mgr.send_msg(receiver_id, byte_msg)

    def send_broadcast(self, broadcast):
        self.conn_mgr.send_msg(bytes(1), broadcast)


def print_out(msg_q):
    while True:
        msg_data = msg_q.get(block=True)
        sender = msg_data[0]
        msg = msg_data[1]
        print('<', sender, ':', msg)


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

    # node.send_msg(bytes([1]), 'HELLO THERE'.encode('utf-8'))
    print_out_process = mp.Process(target=print_out, args=(node.msg_q, ))
    print_out_process.start()

    sleep(5)
    node.send_msg(bytes([1]), 'HELLO THERE'.encode('utf-8'))

    print('UP AND RUNNING')
    while True:
        send_data = input()
        target_id = bytes([int(send_data[0])])
        msg_str = send_data[2:]
        node.send_msg(target_id, msg_str.encode('utf-8'))
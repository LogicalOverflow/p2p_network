import socket
import threading
import multiprocessing as mp

from utils.connection_map import ConnectionMap


def bytes_to_int(bytes_array):
    int_value = 0
    for byte in bytes_array:
        int_value *= 255
        int_value += byte

    return int_value


class ConnectionMgr():
    SPLIT_CHAR = bytes(2)
    BUFFER_SIZE = 32
    BROADCAST_ID = bytes(1)
    MSG_TYPES = {'string': bytes([1]),
                 'file': bytes([2]),
                 'json_string': bytes([3]),
                 'new_connection': bytes([4]),
                 'connection_map': bytes([5])}

    def __init__(self, sock, client_id):
        self.SOCK = sock
        self.CLIENT_ID = client_id
        self.CONNECTION_MAP = ConnectionMap()
        self.RECEIVED_BROADCASTS = []
        self.MANAGER = mp.Manager()
        self.SEND_Q = self.MANAGER.Queue()
        self.CONNECTION_Q = self.MANAGER.Queue()
        self.CALLBACKS = {'connect': lambda *args: None,
                          'message': lambda *args: None,
                          'forward': lambda *args: None,
                          'sending': lambda *args: None,
                          'disconnect': lambda *args: None}
        self.CONNECTION_PARTNERS = {}

        self.acceptor_thread = threading.Thread(target=self.acceptor)
        self.acceptor_thread.start()

        self.sender_thread = threading.Thread(target=self.msg_sender)
        self.sender_thread.start()

    # threaded methods

    def acceptor(self):
        self.SOCK.listen(2)
        while True:
            conn = self.SOCK.accept()[0]
            receiver_thread = threading.Thread(target=self.receiver, args=(conn, ))
            receiver_thread.start()

    def receiver(self, conn):
        def send_map(conn_map):
            map_str = conn_map.to_json_string()
            self.SEND_Q.put((self.MSG_TYPES['connection_map'], map_str.encode('utf-8'), self.CLIENT_ID, connected_id))

        client_id_len = len(self.CLIENT_ID)
        connected_id = bytearray(0)

        conn.send(self.CLIENT_ID)
        while len(connected_id) < client_id_len:
            connected_id += conn.recv(1)

        connected_id = bytes(connected_id)

        self.CONNECTION_Q.put((connected_id, conn))
        self.connection_internal_callback(connected_id, self.CLIENT_ID)

        send_first = bytes_to_int(connected_id) > bytes_to_int(self.CLIENT_ID)
        if send_first:
            send_map(self.CONNECTION_MAP)

        data_stream = bytearray(0)

        while True:
            try:
                new_data = conn.recv(self.BUFFER_SIZE)
                if new_data:
                    data_stream += new_data
                else:
                    break
            except socket.timeout:
                break
            if self.SPLIT_CHAR in data_stream:
                ind = data_stream.index(self.SPLIT_CHAR)
                header = bytes(data_stream[:ind])

                data_stream = data_stream[ind + len(self.SPLIT_CHAR):]
                msg_type = bytes(header[2:3])
                sender_id = bytes(header[3:client_id_len + 3])
                receiver_id = bytes(header[client_id_len + 3:2 * client_id_len + 3])
                msg_len_list = bytes(header[2 * client_id_len + 3:])
                msg_len = 0

                for byte in msg_len_list:
                    msg_len *= 255
                    msg_len += byte

                while len(data_stream) < msg_len:
                    data_stream += conn.recv(self.BUFFER_SIZE)

                msg = bytes(data_stream[:msg_len])
                data_stream = data_stream[msg_len:]
                packet = header + msg

                if receiver_id == self.CLIENT_ID or (
                        receiver_id == self.BROADCAST_ID and packet not in self.RECEIVED_BROADCASTS):
                    if receiver_id == self.BROADCAST_ID:
                        self.RECEIVED_BROADCASTS.append(packet)

                    if msg_type == self.MSG_TYPES['connection_map']:
                        other_map = ConnectionMap.conn_mgr_from_json(msg.decode('utf-8'))

                        for connected_partners in other_map - self.CONNECTION_MAP:
                            id_a = connected_partners[:client_id_len]
                            id_b = connected_partners[client_id_len:]
                            self.CONNECTION_MAP.add_connection(id_a, id_b)
                            if send_first:
                                self.connection_internal_callback(id_a, id_b)

                        if not send_first:
                            send_first = True
                            send_map(self.CONNECTION_MAP)

                    elif msg_type == self.MSG_TYPES['new_connection']:
                        self.CONNECTION_MAP.add_connection(msg[:client_id_len], msg[client_id_len:])
                    else:
                        self.new_message_internal_callback(sender_id, msg, msg_type)

                if receiver_id != self.CLIENT_ID:
                    self.SEND_Q.put((msg_type, msg, sender_id, receiver_id))
                    self.forward_internal_callback(sender_id, receiver_id, msg, msg_type)

        self.disconnect_internal_callback(connected_id)

    def msg_sender(self):
        def send(sock, send_msg):
            total_sent = 0
            while total_sent < len(send_msg):
                sent = sock.send(send_msg[total_sent:])
                total_sent += sent

        send_msgs = []
        connections = {}
        while True:
            if not self.CONNECTION_Q.empty():
                conn_id, conn = self.CONNECTION_Q.get()
                connections[conn_id] = conn
            if not self.SEND_Q.empty():
                msg_type, msg, sender_id, receiver_id = self.SEND_Q.get()
                if sender_id == self.CLIENT_ID:
                    self.sending_internal_callback(receiver_id, msg, msg_type)

                msg_len = len(msg)
                msg_len_list = []
                filler = bytes([5, 5])

                while msg_len > 0:
                    msg_len_list.insert(0, msg_len % 255)
                    msg_len //= 255
                header = bytes(filler + msg_type + sender_id + receiver_id + bytes(msg_len_list) + self.SPLIT_CHAR)
                to_send = bytes(header + msg)
                if receiver_id in connections:
                    send(connections[receiver_id], to_send)
                elif receiver_id == self.BROADCAST_ID and to_send not in send_msgs:
                    for conn_id in connections:
                        if conn_id != sender_id:
                            send(connections[conn_id], to_send)
                else:
                    path = self.CONNECTION_MAP.get_connection(self.CLIENT_ID, receiver_id)
                    if path:
                        send(connections[path[1]], to_send)

                send_msgs.append(to_send)

    # internal methods

    # callback methods

    def connection_internal_callback(self, id_a, id_b):
        self.CONNECTION_MAP.add_connection(id_a, id_b)
        self.send_broadcast(id_a + id_b, 'new_connection')
        return self.CALLBACKS['connect'](id_a, id_b)

    def new_message_internal_callback(self, sender_id, msg, msg_type):
        for type_name, type_id in self.MSG_TYPES.items():
            if type_id == msg_type:
                msg_type = type_name
                break

        if msg_type == 'new_connection':
            id_a = msg[:len(self.CLIENT_ID)]
            id_b = msg[len(self.CLIENT_ID):]
            self.CONNECTION_MAP.add_connection(id_a, id_b)
        else:
            return self.CALLBACKS['message'](sender_id, msg, msg_type)

    def forward_internal_callback(self, sender_id, receiver_id, msg, msg_type):
        return self.CALLBACKS['forward'](sender_id, receiver_id, msg, msg_type)

    def sending_internal_callback(self, receiver_id, msg, msg_type):
        return self.CALLBACKS['sending'](receiver_id, msg, msg_type)

    def disconnect_internal_callback(self, disconnected_id):
        return self.CALLBACKS['disconnect'](disconnected_id)

    # 'normal' methods

    def connect(self, target_address):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(target_address)
        receiver_thread = threading.Thread(target=self.receiver, args=(conn, ))
        receiver_thread.start()
        return True

    def send_broadcast(self, msg, msg_type='string'):
        self.send_msg(self.BROADCAST_ID, msg, msg_type)

    def send_msg(self, receiver_id, msg, msg_type='string'):
        self.SEND_Q.put((self.MSG_TYPES[msg_type], msg, self.CLIENT_ID, receiver_id))

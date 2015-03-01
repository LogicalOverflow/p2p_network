import socket
import threading
import multiprocessing as mp


class ConnectionMgr():
    BUFFER_SIZE = 32
    SPLIT_CHAR = bytes(2)
    BROADCAST_ID = bytes(1)
    MSG_TYPES = {'string': bytes([1]),
                 'json_string': bytes([2]),
                 'new_buddy': bytes([3])}

    def __init__(self, sock, client_id):
        self.HOST = True
        self.FIRST_LEVEL = True
        self.SOCK = sock
        self.MANAGER = mp.Manager()
        self.CLIENT_ID = client_id
        self.MSG_Q = self.MANAGER.Queue()
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
        client_id_len = len(self.CLIENT_ID)
        connected_id = bytearray(0)

        conn.send(self.CLIENT_ID)
        while len(connected_id) < client_id_len:
            connected_id += conn.recv(1)

        connected_id = bytes(connected_id)

        # TODO MAKE PARTNER LIST WORKING
        '''
        partners = bytearray(0)
        for partner in self.CONNECTION_PARTNERS:
            partners += partner
            partners = bytes(partners + self.SPLIT_CHAR)
        conn.send(partners)
        del partners

        connected_partners = []
        data_stream = bytearray(0)
        while True:
            data_stream += bytes(conn.recv(1))
            if self.SPLIT_CHAR in data_stream:
                break
        partner_list = data_stream[:data_stream.index(self.SPLIT_CHAR)]
        data_stream = data_stream[data_stream.index(self.SPLIT_CHAR) + 1:]
        while len(partner_list) > client_id_len:
            connected_partners.append(partner_list[:client_id_len])
            partner_list = partner_list[client_id_len:]

        print('GOT DEM PARTNERS!')
        '''

        self.CONNECTION_Q.put((connected_id, conn))
        self.connection_internal_callback(connected_id, 'BAUM')  # partners)

        data_stream = bytearray(0)

        while True:
            try:
                new_data = conn.recv(self.BUFFER_SIZE)
                if new_data:
                    data_stream += new_data
                else:
                    break  # TODO IS THIS WORKING?! -> TEST THIS
            except socket.timeout:
                break
            if self.SPLIT_CHAR in data_stream:
                ind = data_stream.index(self.SPLIT_CHAR)
                data = data_stream[:ind]

                data_stream = data_stream[ind + 1:]
                msg_type = bytes(data[:1])
                msg = bytes(data[1:-2 * client_id_len])
                sender_id = bytes(data[-2 * client_id_len:-client_id_len])
                receiver_id = bytes(data[-client_id_len:])

                if receiver_id == self.CLIENT_ID or receiver_id == self.BROADCAST_ID:
                    if msg_type == self.MSG_TYPES['new_buddy']:
                        # connected_partners.append(msg)
                        # self.CONNECTION_PARTNERS[connected_id].append(msg)
                        pass
                    else:
                        self.MSG_Q.put((sender_id, msg, msg_type))
                        self.new_message_internal_callback(sender_id, msg, msg_type)
                if receiver_id != self.CLIENT_ID:
                    self.SEND_Q.put((msg_type, msg, sender_id, receiver_id))
                    self.forward_internal_callback(sender_id, receiver_id, msg, msg_type)

        self.disconnect_internal_callback(connected_id)

    def msg_sender(self):
        send_q = self.SEND_Q
        connection_q = self.CONNECTION_Q
        split_char = self.SPLIT_CHAR

        def send(sock, send_msg):
            total_sent = 0
            while total_sent < len(send_msg):
                sent = sock.send(send_msg[total_sent:])
                total_sent += sent

        connections = {}
        send_msgs = []
        while True:
            if not connection_q.empty():
                conn_id, conn = connection_q.get()
                connections[conn_id] = conn
            if not send_q.empty():
                msg_type, msg, sender_id, receiver_id = send_q.get()
                to_send = bytes(msg_type + msg + sender_id + receiver_id + split_char)
                if to_send in send_msgs:
                    continue
                elif receiver_id in connections:
                    send(connections[receiver_id], to_send)
                else:
                    for conn_id in connections:
                        if conn_id != sender_id:
                            send(connections[conn_id], to_send)

                send_msgs.append(to_send)

    # internal methods

    # callback methods

    def connection_internal_callback(self, connected_id, connected_partners):
        self.CONNECTION_PARTNERS[connected_id] = connected_partners
        for receiver_id in self.CONNECTION_PARTNERS:
            self.MSG_Q.put((self.MSG_TYPES['new_buddy'], connected_id, self.CLIENT_ID, receiver_id))
        return self.CALLBACKS['connect'](connected_id, connected_partners)

    def new_message_internal_callback(self, sender_id, msg, msg_type):
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

    def send_msg(self, receiver_id, msg, msg_type='string'):
        self.sending_internal_callback(receiver_id, msg, msg_type)
        self.SEND_Q.put((self.MSG_TYPES[msg_type], msg, self.CLIENT_ID, receiver_id))

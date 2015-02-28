import socket
import multiprocessing as mp


def acceptor(conn_mgr):
    conn_mgr.SOCK.listen(2)
    while True:
        conn = conn_mgr.SOCK.accept()[0]
        receiver_process = mp.Process(target=receiver, args=(conn, conn_mgr, ))
        receiver_process.start()


def receiver(conn, conn_mgr):
    client_id_len = len(conn_mgr.CLIENT_ID)
    connected_id = bytearray(0)

    conn.send(conn_mgr.CLIENT_ID)
    while len(connected_id) < client_id_len:
        connected_id += conn.recv(1)

    connected_id = bytes(connected_id)

    # partners = bytearray(0)
    # for partner in conn_mgr.CONNECTIONS:
    # partners += partner
    # partners = bytes(partners + conn_mgr.SPLIT_CHAR)
    # conn.send(partners)

    # connected_partners = []
    # last_ids = bytearray(0)
    # while True:
    # next_partner = bytes(conn.recv(client_id_len))
    # if next_partner == conn_mgr.SPLIT_CHAR:
    # break
    # else:
    # last_ids += next_partner
    # last_ids = last_ids[:len(conn_mgr.SPLIT_CHAR)]
    # if last_ids == conn_mgr.SPLIT_CHAR:
    # break
    # connected_partners.append(next_partner)
    # del connected_partners[-len(conn_mgr.SPLIT_CHAR)//client_id_len:]
    # del last_ids

    conn_mgr.CONNECTION_Q.put((connected_id, conn))
    conn_mgr.CALLBACK_Q.put(('connect_int', (connected_id, conn)))

    data_stream = bytearray(0)
    while True:
        try:
            new_data = conn.recv(conn_mgr.BUFFER_SIZE)
            if new_data:
                data_stream += new_data
            else:
                break  # TODO IS THIS WORKING?! -> TEST THIS
        except socket.timeout:
            break
        if conn_mgr.SPLIT_CHAR in data_stream:
            ind = data_stream.index(conn_mgr.SPLIT_CHAR)
            data = data_stream[:ind]

            data_stream = data_stream[ind + 1:]
            msg_type = bytes(data[:1])
            msg = bytes(data[1:-2 * client_id_len])
            sender_id = bytes(data[-2 * client_id_len:-client_id_len])
            receiver_id = bytes(data[-client_id_len:])

            if receiver_id == conn_mgr.CLIENT_ID or receiver_id == conn_mgr.BROADCAST_ID:
                if msg_type == conn_mgr.MSG_TYPES['new_buddy']:
                    # connected_partners.append(msg)
                    pass
                else:
                    conn_mgr.MSG_Q.put((sender_id, msg, msg_type))
                    conn_mgr.CALLBACK_Q.put(('message_int', (sender_id, msg, msg_type)))
            if receiver_id != conn_mgr.CLIENT_ID:
                conn_mgr.SEND_Q.put((msg_type, msg, sender_id, receiver_id))
                conn_mgr.CALLBACK_Q.put(('forward_int', (sender_id, receiver_id, msg, msg_type)))

    conn_mgr.CALLBACK_Q.put(('disconnect_int', (connected_id)))


def msg_sender(conn_mgr):
    send_q = conn_mgr.SEND_Q
    connection_q = conn_mgr.CONNECTION_Q
    split_char = conn_mgr.SPLIT_CHAR

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


def start_conn_mgr(conn_mgr):
    print(0)
    conn_mgr.HOST = False
    print(1)
    while True:
        print(2)
        callback_data = conn_mgr.CALLBACK_Q.get(block=True)
        print(3)
        conn_mgr.CALLBACKS[callback_data[0]](*callback_data[1])
        print(4)


class ConnectionMgr():
    BUFFER_SIZE = 32
    SPLIT_CHAR = bytes(2)
    BROADCAST_ID = bytes(1)
    MSG_TYPES = {'string': bytes([1]),
                 'json_string': bytes([2]),
                 'new_buddy': bytes([3])}

    def __init__(self, sock, client_id):
        self.RUNNING = False
        self.HOST = True
        self.FIRST_LEVEL = True
        self.SOCK = sock
        self.MANAGER = mp.Manager()
        self.CLIENT_ID = client_id
        self.MSG_Q = self.MANAGER.Queue()
        self.SEND_Q = self.MANAGER.Queue()
        self.CALLBACK_Q = self.MANAGER.Queue()
        self.CONNECTION_Q = self.MANAGER.Queue()
        self.CONNECTION_PARTNERS = {}
        self.CALLBACKS = {'connect_int': self.connection_internal_callback,
                          'message_int': self.new_message_internal_callback,
                          'forward_int': self.forward_internal_callback,
                          'sending_int': self.sending_internal_callback,
                          'disconnect_int': self.disconnect_internal_callback,
                          'connect': self.connection_callback,
                          'message': self.new_message_callback,
                          'forward': self.forward_callback,
                          'sending': self.sending_callback,
                          'disconnect': self.disconnect_callback,
                          'attr': self.pipe_attribute}
        self.acceptor_process = None
        self.sender_process = None

    def __getattribute__(self, item):
        attr = object.__getattribute__(self, item)
        if callable(attr):
            return attr
        try:
            running = object.__getattribute__(self, 'RUNNING')
            host = object.__getattribute__(self, 'HOST')
        except AttributeError:
            return attr

        if running and host:
            parent_conn, child_conn = mp.Pipe()
            q = object.__getattribute__(self, 'CALLBACK_Q')
            q.put(('attr', (item, child_conn)))
            return parent_conn.recv()
        else:
            return attr

    def start(self):
        # TODO WHAT IS GOING ON?! 'OSError: handle is closed' WHY THE FUCK
        self.acceptor_process = mp.Process(target=acceptor, args=(self, ))
        self.acceptor_process.start()

        self.sender_process = mp.Process(target=msg_sender, args=(self, ))
        self.sender_process.start()

        self.RUNNING = True

        process = mp.Process(target=start_conn_mgr, args=(self, ))
        process.start()
        return process

    def pipe_attribute(self, attr_name, pipe):
        pipe.send(getattr(self, attr_name, None))

    def connection_internal_callback(self, connected_id, connected_partners):
        self.CONNECTION_PARTNERS[connected_id] = connected_partners
        for receiver_id in self.CONNECTION_PARTNERS:
            self.MSG_Q.put((self.MSG_TYPES['new_buddy'], connected_id, self.CLIENT_ID, receiver_id))
        return self.CALLBACKS['connect'](connected_id, connected_partners)

    def connection_callback(self, connected_id, connection):
        pass

    def new_message_internal_callback(self, sender_id, msg, msg_type):
        return self.CALLBACKS['message'](sender_id, msg, msg_type)

    def new_message_callback(self, sender_id, msg, msg_type):
        print(sender_id, ':', msg)
        pass

    def forward_internal_callback(self, sender_id, receiver_id, msg, msg_type):
        return self.CALLBACKS['forward'](sender_id, receiver_id, msg, msg_type)

    def forward_callback(self, sender_id, receiver_id, msg, msg_type):
        pass

    def sending_internal_callback(self, receiver_id, msg):
        return self.CALLBACKS['sending'](receiver_id, msg)

    def sending_callback(self, receiver_id, msg):
        pass

    def disconnect_internal_callback(self, disconnected_id):
        return self.CALLBACKS['disconnect'](disconnected_id)

    def disconnect_callback(self, disconnected_id):
        pass

    def connect(self, target_address):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(target_address)
        receiver_process = mp.Process(target=receiver, args=(conn, self, ))
        receiver_process.start()
        return True

    def send_msg(self, receiver_id, msg, msg_type='string'):
        self.CALLBACK_Q.put(('sending', (receiver_id, msg)))
        self.SEND_Q.put((self.MSG_TYPES[msg_type], msg, self.CLIENT_ID, receiver_id))

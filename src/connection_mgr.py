import socket
import multiprocessing as mp


def acceptor(s, conn_mgr):
    s.listen(2)
    while True:
        conn = s.accept()[0]
        receiver_process = mp.Process(target=receiver, args=(conn, conn_mgr, ))
        receiver_process.start()


def receiver(conn, conn_mgr):

    connection_callback = getattr(conn_mgr, 'connection_internal_callback', None)
    new_message_callback = getattr(conn_mgr, 'new_message_internal_callback', None)
    forward_callback = getattr(conn_mgr, 'forward_internal_callback', None)
    disconnect_callback = getattr(conn_mgr, 'disconnect_internal_callback', None)
    client_id_len = len(conn_mgr.CLIENT_ID)
    connected_id = bytearray(0)

    conn.send(conn_mgr.CLIENT_ID)
    while len(connected_id) < client_id_len:
        connected_id += conn.recv(1)

    connected_id = bytes(connected_id)

    conn_mgr.CONNECTION_Q.put((connected_id, conn))
    if callable(connection_callback):
        connection_callback(connected_id, conn)

    data_stream = bytearray(0)
    while True:
        data_stream += conn.recv(conn_mgr.BUFFER_SIZE)
        if conn_mgr.SPLIT_CHAR in data_stream:
            ind = data_stream.index(conn_mgr.SPLIT_CHAR)
            data = data_stream[:ind]

            data_stream = data_stream[ind + 1:]
            msg = bytes(data[:-2 * client_id_len])
            sender_id = bytes(data[-2 * client_id_len:-client_id_len])
            receiver_id = bytes(data[-client_id_len:])

            if receiver_id == conn_mgr.CLIENT_ID or receiver_id == conn_mgr.BROADCAST_ID:
                conn_mgr.MSG_Q.put((sender_id, msg))
                if callable(new_message_callback):
                    new_message_callback(sender_id, msg)
            if receiver_id != conn_mgr.CLIENT_ID:
                conn_mgr.SEND_Q.put((msg, sender_id, receiver_id))
                if callable(new_message_callback):
                    forward_callback(sender_id, receiver_id, msg)

    disconnect_callback(connected_id)


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
            msg, sender_id, receiver_id = send_q.get()
            to_send = bytes(msg + sender_id + receiver_id + split_char)
            if to_send in send_msgs:
                continue
            elif receiver_id in connections:
                send(connections[receiver_id], to_send)
            else:
                for conn_id in connections:
                    if conn_id != sender_id:
                        send(connections[conn_id], to_send)

            send_msgs.append(to_send)


class ConnectionMgr():
    BUFFER_SIZE = 32
    SPLIT_CHAR = bytes(2)
    BROADCAST_ID = bytes(1)

    def __init__(self, sock, client_id, msg_queue):
        self.CLIENT_ID = client_id
        self.CONNECTION_Q = mp.Queue()
        self.SEND_Q = mp.Queue()
        self.MSG_Q = msg_queue
        self.CONNECTIONS = {}

        self.acceptor_process = mp.Process(target=acceptor, args=(sock, self, ))
        self.acceptor_process.start()

        self.sender_process = mp.Process(target=msg_sender, args=(self, ))
        self.sender_process.start()

    def connection_internal_callback(self, connected_id, connection):
        return self.connection_callback(connected_id, connection)

    def connection_callback(self, connected_id, connection):
        pass

    def new_message_internal_callback(self, sender_id, msg):
        return self.new_message_callback(sender_id, msg)

    def new_message_callback(self, sender_id, msg):
        pass

    def forward_internal_callback(self, sender_id, receiver_id, msg):
        return self.forward_callback(sender_id, receiver_id, msg)

    def forward_callback(self, sender_id, receiver_id, msg):
        pass

    def sending_internal_callback(self, receiver_id, msg):
        return self.sending_callback(receiver_id, msg)

    def sending_callback(self, receiver_id, msg):
        pass

    def disconnect_internal_callback(self, receiver_id):
        return self.disconnect_callback(receiver_id)

    def disconnect_callback(self, receiver_id):
        pass

    def connect(self, target_address):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(target_address)
        receiver_process = mp.Process(target=receiver, args=(conn, self, ))
        receiver_process.start()
        return True

    def send_msg(self, receiver_id, msg):
        self.sending_internal_callback(receiver_id, msg)
        self.SEND_Q.put((msg, self.CLIENT_ID, receiver_id))
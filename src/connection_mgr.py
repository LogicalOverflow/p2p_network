import socket
import multiprocessing as mp


def acceptor(s, conn_mgr):
    s.listen(2)
    while True:
        conn = s.accept()[0]
        receiver_process = mp.Process(target=receiver, args=(conn, conn_mgr, ))
        receiver_process.start()


def receiver(conn, conn_mgr):
    # TODO make callbacks called in main process
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

    # partners = bytearray(0)
    # for partner in conn_mgr.CONNECTIONS:
    #    partners += partner
    # partners = bytes(partners + conn_mgr.SPLIT_CHAR)
    # conn.send(partners)

    # connected_partners = []
    # last_ids = bytearray(0)
    # while True:
    #   next_partner = bytes(conn.recv(client_id_len))
    #   if next_partner == conn_mgr.SPLIT_CHAR:
    #       break
    #   else:
    #       last_ids += next_partner
    #       last_ids = last_ids[:len(conn_mgr.SPLIT_CHAR)]
    #       if last_ids == conn_mgr.SPLIT_CHAR:
    #           break
    #   connected_partners.append(next_partner)
    # del connected_partners[-len(conn_mgr.SPLIT_CHAR)//client_id_len:]
    # del last_ids

    conn_mgr.CONNECTION_Q.put((connected_id, conn))
    if callable(connection_callback):
        connection_callback(connected_id, conn)

    data_stream = bytearray(0)
    while True: # TODO while 'socket.is_connected'
        data_stream += conn.recv(conn_mgr.BUFFER_SIZE)
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
                    if callable(new_message_callback):
                        new_message_callback(sender_id, msg, msg_type)
            if receiver_id != conn_mgr.CLIENT_ID:
                conn_mgr.SEND_Q.put((msg_type, msg, sender_id, receiver_id))
                if callable(new_message_callback):
                    forward_callback(sender_id, receiver_id, msg, msg_type)

    # disconnect_callback(connected_id)


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


class ConnectionMgr():
    BUFFER_SIZE = 32
    SPLIT_CHAR = bytes(2)
    BROADCAST_ID = bytes(1)
    MSG_TYPES = {'string': bytes([1]),
                 'json_string': bytes([2]),
                 'new_buddy': bytes([3])}

    def __init__(self, sock, client_id, msg_queue):
        self.CLIENT_ID = client_id
        self.CONNECTION_Q = mp.Queue()
        self.SEND_Q = mp.Queue()
        self.MSG_Q = msg_queue
        self.CONNECTION_PARTNERS = {}
        # TODO write connections + partners into main process

        self.acceptor_process = mp.Process(target=acceptor, args=(sock, self, ))
        self.acceptor_process.start()

        self.sender_process = mp.Process(target=msg_sender, args=(self, ))
        self.sender_process.start()

    def connection_internal_callback(self, connected_id, connected_partners):
        self.CONNECTION_PARTNERS[connected_id] = connected_partners
        for receiver_id in self.CONNECTION_PARTNERS:
            self.MSG_Q.put((self.MSG_TYPES['new_buddy'], connected_id, self.CLIENT_ID, receiver_id))
        return self.connection_callback(connected_id, connected_partners)

    def connection_callback(self, connected_id, connection):
        pass

    def new_message_internal_callback(self, sender_id, msg, msg_type):
        return self.new_message_callback(sender_id, msg, msg_type)

    def new_message_callback(self, sender_id, msg, msg_type):
        pass

    def forward_internal_callback(self, sender_id, receiver_id, msg, msg_type):
        return self.forward_callback(sender_id, receiver_id, msg, msg_type)

    def forward_callback(self, sender_id, receiver_id, msg, msg_type):
        pass

    def sending_internal_callback(self, receiver_id, msg):
        return self.sending_callback(receiver_id, msg)

    def sending_callback(self, receiver_id, msg):
        pass

    def disconnect_internal_callback(self, disconnected_id):
        return self.disconnect_callback(disconnected_id)

    def disconnect_callback(self, disconnected_id):
        pass

    def connect(self, target_address):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(target_address)
        receiver_process = mp.Process(target=receiver, args=(conn, self, ))
        receiver_process.start()
        return True

    def send_msg(self, receiver_id, msg, msg_type='string'):
        # TODO self.sending_internal_callback(receiver_id, msg) - make it work! wtf is wrong here?
        self.SEND_Q.put((self.MSG_TYPES[msg_type], msg, self.CLIENT_ID, receiver_id))
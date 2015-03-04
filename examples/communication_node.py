from time import sleep

from p2p_network.node import Node


def new_message_callback(sender_id, msg, msg_type):
    if msg_type == 'string':
        print('< RECEIVED \'{0}\' FROM'.format(msg.decode('utf-8')), sender_id)
    if msg_type == 'file':
        print('< RECEIVED FILE')
        with open('received', 'bw') as write_out:
            write_out.write(msg)


def sending_callback(receiver_id, msg, msg_type):
    if msg_type == 'string':
        print('> SENDING  \'{0}\' TO'.format(msg.decode('utf-8')), receiver_id)
    elif msg_type == 'file':
        print('> SENDING  FILE TO', receiver_id)


if __name__ == '__main__':
    client_id_number = int(input('CLIENT  ID  : ') or '1')
    CLIENT_ID = bytes([client_id_number])
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

    sleep(0.25)

    print('UP AND RUNNING')
    while True:
        send_data = input()
        if send_data[0].upper() == 'B':
            target_id = node.conn_mgr.BROADCAST_ID
        else:
            target_id = bytes([int(send_data[0])])
        msg_str = send_data[2:]
        if msg_str[:4].upper() == 'FILE':
            path = msg_str[5:]
            with open(path, 'br') as f:
                file_data = f.read()
            node.send_msg(target_id, file_data, 'file')
        else:
            if target_id == node.conn_mgr.BROADCAST_ID:
                node.send_broadcast(msg_str.encode('utf-8'))
            else:
                node.send_msg(target_id, msg_str.encode('utf-8'))
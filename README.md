# Peer to Peer Network
## General
A python peer to peer network, connected through tcp connections. Early stage

## What is curently possible:
- sending data over multiple nodes to anyone in the network (forwarded), using the path with the least nodes inbetween (path finding)
- broadcasting data to everyone in the network

## Installation
installation throght setup.py
packet name is p2p_network

## example (communication_node.py) usage:
### on startup:
- client id - integer (less than 10) defines your client id and the port used (port is 1337[ID]; default: 1, must not be 0)
- partner id - integer (less than 10) defines the partners id and the port to connect to (default: 1, must not be 0)
- client ip - ip the server waiting for new connections will listen to (default: 127.0.0.1)
- partner ip - the ip to connect to (default: 127.0.0.1)

### sending messages and files:
- [ID] [MESSAGE] - ID must only have 1 digit, message is the message send to the id, id 0 will send a broadcast
- [ID] FILE [PATH] - path is a absolute path or a path relative to the working directory, id is the id the file is send to, files received will be saved as 'received' (without a file extension) in the working dircetory

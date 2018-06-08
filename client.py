""" Simple client for MDB access server connection
"""
import socket
import logging

import msgpack
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

def client(msg):
    """ Simple Client
    """
    server_address = ('localhost', 8090)
    with socket.socket(socket.AF_INET,
                       socket.SOCK_STREAM,
                       socket.IPPROTO_TCP) as sock:
        sock.connect(server_address)
        response = bytes()
        sock.send(msg)
        response = sock.recv(64)
        logging.debug("RAW Response %s", response)

        return response, sock

def main():
    """ Main Entry Point
    """
    while True:
        msg = input("DBNumber: ")

        raw_response, sock = client(str.encode(msg))
        response = msgpack.unpackb(raw_response, raw=False)
        logging.info("Response returned: %s", response)

if __name__ == '__main__':
    main()

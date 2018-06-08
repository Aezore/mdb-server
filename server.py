"""
Server Side for MSACCESS connection for the ACUTE UI
"""
import socket
import logging

import msgpack
import pyodbc

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

CONN_STR = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=I:\database.accdb;')

PORT = 8090

def connect_db():
    """ Connects to ACCDB and returns a cursor for queries
    """
    # Establecer la conexión.
    conn = pyodbc.connect(CONN_STR)
    # Crear cursor para ejecutar consultas.
    cursor = conn.cursor()
    return cursor

def fetch_data(dbnumber, cursor):
    """ FETCH DATA FROM DB
    """
    cursor.execute("SELECT PartNo FROM tblJobRecords WHERE ID = ?", dbnumber)
    row = cursor.fetchone()
    cursor.execute('SELECT SystemType, SubSystem, PM, PartNo, [2ndPartNo] '\
                   'FROM tblMasterPartsList WHERE PartNo = ?', row[0])
    row = cursor.fetchone()
    return row

def create_socket():
    """ Socket Creation
    """
    _socket = socket.socket()
    _socket.bind(('0.0.0.0', PORT))
    _socket.listen(0)
    return _socket

def main():
    """ MAIN LOOP
    """
    data = list()
    logging.info("Starting server in %s", socket.gethostname())

    with create_socket() as _socket:
        logging.info("Server started")

        while True:
            logging.info("Waiting for connection")
            try:
                client, addr = _socket.accept()
            except KeyboardInterrupt:
                logging.info("Abort by user")

            logging.info("Connected address: %s", addr)

            while True:
                dbnumber = client.recv(32)
                dbnumber = dbnumber.decode("utf-8")
                if not dbnumber:
                    logging.warning("Empty packet received")
                    break

                else:
                    logging.info("DB NUMBER QUERY: %s", dbnumber)
                    cursor = connect_db()
                    row = fetch_data(dbnumber, cursor)
                    data = list(row)
                    logging.info("Data fetched: %s", data)
                    logging.info("Bosch Number: %s", data[3])
                    packed_data = msgpack.packb(data, use_bin_type=True)
                    logging.debug("Packed data: %s", packed_data)
                    client.send(packed_data)
                    logging.info("Data Sent to: %s", addr)
                    data[:] = []

    logging.info("Closing connection")
    client.close()

if __name__ == "__main__":
    main()

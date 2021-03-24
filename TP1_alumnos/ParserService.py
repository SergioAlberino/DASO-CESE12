#!/usr/bin/python3

# ParserService para TP! de Diseño de Aplicaciones 12CESE
# Alumno: Sergio Alberino
import socket
import sys
import json
import csv
import signal
import time

# General
filename ="datos.csv"
port = 10000

# Signal Handler
def signal_handler(sig, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


# Create a UDP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Connect the socket to the port where the server is listening
server_address = ('localhost', port)
print('connecting to {} port {}'.format(server_address[0],server_address[1]))
sock.connect(server_address)

jsonArray = []

while True:
    #read csv file
    with open(filename, 'r') as data:
        reader = csv.DictReader(data)
        for row in reader:
            formLine = {"id": int(row['id']), "value1": float(row['compra']), "value2": float(row['venta']), "name": row['nombre']}
            print(formLine)
            jsonArray.append(formLine)
    
    print("********")
    jsonString = json.dumps(jsonArray)
    print ('JSONstring:', jsonString)
    print("********")

    sock.sendall(bytes(jsonString ,encoding="utf-8"))

    # Receive response
    print('waiting to receive')
    data, server = sock.recvfrom(4096)
    print('received {!r}'.format(data))

    # Wait 30 sec
    time.sleep(30)



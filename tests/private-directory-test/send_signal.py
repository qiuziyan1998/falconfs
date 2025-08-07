#!/usr/bin/env python3
import argparse
import sys
import os
import socket
import random
import struct

# TODO: change to your own path
os.sys.path.append('/usr/lib/python3.9/site-packages')

def send_signal(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    source_port = random.randint(49152, 65535)
    sock.bind(('0.0.0.0', source_port))

    message = "hello"
    dest_port = int(port)
    dest_addr = (ip, dest_port)

    sock.sendto(message.encode('utf-8'), dest_addr)
    print(f"send signal to {ip}")

    sock.close()

if __name__ == '__main__':
    send_signal(sys.argv[1], sys.argv[2])

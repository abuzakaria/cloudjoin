__author__ = 'Zakaria'

import asyncio
from packet import Packet
import constants
import pickle


@asyncio.coroutine
def tcp_echo_client(packet, loop):
    """

    :param packet:
    :param loop:
    """
    reader, writer = yield from asyncio.open_connection(constants.SOURCE_HOST,
                                                        constants.SOURCE_PORT,
                                                        loop=loop)
    writer.write(pickle.dumps(packet))
    writer.close()


def send_packet(packet):
    """

    :param packet:
    """
    packet.sender = {'host': "127.0.0.1", 'port': 0, 'name': "interface"}
    loop.run_until_complete(tcp_echo_client(packet, loop))


def get_int(text):
    """

    :param text:
    :return:
    """
    x = input(text)
    while x.isdigit() is False and int(x) > 1:
        print("Not a valid number")
        x = input(text)
    return x

loop = asyncio.get_event_loop()
while True:
    p = None
    inp = input("***************************\nCommand: ")
    if inp == constants.INPUT_EXIT:
        loop.close()
        break

    elif inp == constants.INPUT_START_STREAM:
        p = Packet(constants.SIGNAL_START_STREAM)
        send_packet(p)

    elif inp == constants.INPUT_ADD_NODE:
        n = get_int("Number of nodes: ")
        p = Packet(constants.SIGNAL_ADD_NODE)
        p.append_data(n)
        send_packet(p)

    elif inp == constants.INPUT_STORE_PROTOCOL:
        i = get_int("index of node: ")
        n = get_int("number of elements to process in each cycle: ")
        p = Packet(constants.SIGNAL_STORE_PROTOCOL)
        p.append_data(i)
        p.append_data(n)
        send_packet(p)

    elif inp == constants.INPUT_STORE_PROTOCOL_INIT:
        p = Packet(constants.SIGNAL_STORE_PROTOCOL_INIT)
        send_packet(p)

    elif inp == constants.INPUT_INCREASE_SUBWINDOW:
        i = get_int("index of node: ")
        n = get_int("increase sub-window by: ")
        p = Packet(constants.SIGNAL_INCREASE_SUBWINDOW)
        p.append_data(i)
        p.append_data(n)
        send_packet(p)

    elif inp == constants.INPUT_DECREASE_SUBWINDOW:
        i = get_int("index of node: ")
        n = get_int("decrease sub-window by: ")
        mode = input("decrease mode: ")
        p = Packet(constants.SIGNAL_DECREASE_SUBWINDOW)
        p.append_data(i)
        p.append_data(n)
        p.append_data(mode)
        send_packet(p)

    else:
        print("Undefined command. Try again. Type 'exit' to exit.")
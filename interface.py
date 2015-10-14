__author__ = 'Zakaria'

import asyncio
from packet import Packet
import parameters
import pickle


@asyncio.coroutine
def tcp_echo_client(packet, loop):
    """

    :param packet:
    :param loop:
    """
    reader, writer = yield from asyncio.open_connection(parameters.SOURCE_HOST,
                                                        parameters.SOURCE_PORT,
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
    if inp == parameters.INPUT_EXIT:
        loop.close()
        break

    elif inp == parameters.INPUT_START_STREAM:
        p = Packet(parameters.SIGNAL_START_STREAM)
        send_packet(p)

    elif inp.startswith(parameters.INPUT_ADD_NODE):
        args = inp.split()
        n = int(args[1])
        p = Packet(parameters.SIGNAL_ADD_NODE)
        p.append_data(n)
        send_packet(p)

    elif inp.startswith(parameters.INPUT_SET_SUBWINDOW_SIZE):
        args = inp.split()
        n = int(args[1])
        p = Packet(parameters.SIGNAL_SET_SUBWINDOW_SIZE)
        p.append_data(n)
        send_packet(p)

    elif inp.startswith(parameters.INPUT_SET_SUBWINDOW_TIME):
        args = inp.split()
        n = int(args[1])
        p = Packet(parameters.SIGNAL_SET_SUBWINDOW_TIME)
        p.append_data(n)
        send_packet(p)

    elif inp.startswith(parameters.INPUT_MODE):
        # Sample input considering default subwindow size 10
        # mode A 127.0.0.1 12345 -3
        # mode B 127.0.0.1 12345 3 127.0.0.1 12346
        # mode C 127.0.0.1 12345
        # mode D 1 3 127.0.0.1 12345 -5
        # mode E 127.0.0.1 12345 3 127.0.0.1 12346 -3
        # mode F 1 3 127.0.0.1 12345 -3
        # mode G 127.0.0.1 12345 10 127.0.0.1 12346
        # mode H 1 10 127.0.0.1 12345
        # mode I 1 10 127.0.0.1 12345 -3
        # mode J 127.0.0.1 12345 3
        # mode K 127.0.0.1 12345 13 127.0.0.1 12346
        # mode L 1 7
        args = inp.split()[1:]  # arg list without the mode keyword
        p = Packet(parameters.SIGNAL_MODE)
        p.append_data(args)
        send_packet(p)

    else:
        print("Undefined command. Try again. Type 'exit' to exit.")
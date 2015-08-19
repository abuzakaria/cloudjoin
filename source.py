__author__ = 'Zakaria'
import node
import asyncio
import constants
from packet import Packet
from collections import deque
from membership_manager import MembershipManager


#source or first node of the network
class Source(node.Node):
    nodes = []
    nodes_protocol = []
    packet_buffer = deque()

    def __init__(self, name=None, host=None, port=None):
        """
        Initializes node with name host and port
        :param name: string to identify a node
        :param host: host of node. i.e.: 127.0.0.1
        :param port: port of node
        """
        if name:
            self.name = name
        if host:
            self.host = host
        if port:
            self.port = port
        self.loop = asyncio.get_event_loop()
        self.load_packet_buffer()
        self.membership_manager = MembershipManager(self.loop)

    def distribute(self, packet):
        """
        distributes packet to different processor nodes
        :param packet: packet to distribute
        """

        # print(self.nodes)
        for row in self.nodes:
            (host, port) = row
            self.send(packet, host, port)

    def send_store_protocol_packet(self, processor_node, n, out_of):
        """

        :param processor_node:
        :param n:
        :param out_of:
        """
        print(processor_node)
        print(n)
        print(out_of)
        p = Packet(constants.DATATYPE_PROTOCOL)
        p.append_data(n)
        p.append_data(out_of)
        self.send(p, processor_node[0], processor_node[1])

    def send_store_protocol_packet_batch(self):
        number_of_nodes = len(self.nodes)
        out_of = 0
        for x in range(number_of_nodes):
            out_of += self.nodes_protocol[x]
        for x in range(number_of_nodes):
            self.send_store_protocol_packet(self.nodes[x], self.nodes_protocol[x], out_of)

    def send_delete_packet(self, processor_node):
        """

        :param processor_node:
        """
        p = Packet(constants.DATATYPE_DELETE)
        self.send(p, processor_node)

    # def change_subwindow_size_of_node(self, n, size):
    #     """
    #
    #     :param n: node to be changed
    #     :param size: new window size
    #     """
    #     p = Packet(constants.DATATYPE_SUBWINDOW_SIZE)
    #     p.append_data(size)
    #     self.send(p, n[0], n[1])

    def increase_subwindow_size_of_node(self, n, val):
        """

        :param n: node to be changed
        :param val: increase by val
        """
        p = Packet(constants.DATATYPE_SUBWINDOW_SIZE_INC)
        p.append_data(val)
        self.send(p, n[0], n[1])

    def decrease_subwindow_size_of_node(self, n, val, mode):
        """


        :param mode: which mode to decrease
        :param n: node to be changed
        :param val: increase by val
        """
        p = Packet(constants.DATATYPE_SUBWINDOW_SIZE_DEC)
        p.append_data(val)
        p.append_data(mode)
        self.send(p, n[0], n[1])


    @asyncio.coroutine
    def start_streaming(self):
        """
        start sending packets from buffer
        """
        while len(self.packet_buffer) > 0:
            self.distribute(self.packet_buffer.popleft())
            yield from asyncio.sleep(0.5)
        # self.temp_flag_sending = False
        # self.run_server()
        # src.send(pack, '127.0.0.1', 12350)

    def do(self, packet):
        """

        :param packet:
        """
        if packet.type == constants.DATATYPE_HEARTBEAT:
            self.membership_manager.handle_heartbeat(packet)

        elif packet.type == constants.SIGNAL_STORE_PROTOCOL:
            self.nodes_protocol[int(packet.data[0])] = int(packet.data[1])
            self.send_store_protocol_packet_batch()

        elif packet.type == constants.SIGNAL_STORE_PROTOCOL_INIT:
            self.send_store_protocol_packet_batch()

        elif packet.type == constants.SIGNAL_START_STREAM:
            asyncio.async(self.start_streaming())

        elif packet.type == constants.SIGNAL_ADD_NODE:
            temp_node_list = self.membership_manager.get_nodes(int(packet.data[0]))
            if temp_node_list:
                self.nodes.extend(temp_node_list)
                self.nodes_protocol.extend(1 for n in temp_node_list)

        elif packet.type == constants.SIGNAL_INCREASE_SUBWINDOW:
            temp_node = self.nodes[int(packet.data[0])]
            inc_by = int(packet.data[1])
            self.increase_subwindow_size_of_node(temp_node, inc_by)

        elif packet.type == constants.SIGNAL_DECREASE_SUBWINDOW:
            temp_node = self.nodes[int(packet.data[0])]
            dec_by = int(packet.data[1])
            mode = packet.data[2]
            self.decrease_subwindow_size_of_node(temp_node, dec_by, mode)

    def load_packet_buffer(self):
        """
        temporary function to fill packet buffer.
        """
        with open("data.txt") as f:
            lines = f.read().splitlines()
            l = len(lines)
            for i in range(0, l, 3):    # loop through 3 lines at a time
                pack = Packet(lines[i])
                pack.append_data(lines[i+1])
                pack.append_data(lines[i+2])
                self.packet_buffer.append(pack)
###########################################################

if __name__ == '__main__':
    src = Source('source', constants.SOURCE_HOST, constants.SOURCE_PORT)
    src.run_server()

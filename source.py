from membership_manager import MembershipManager

__author__ = 'Zakaria'
import node
import asyncio
import constants
from packet import Packet
from collections import deque
from copy import deepcopy

# column index of nodes list
COL_NODE = 0
COL_SUBW = 1


#source or first node of the network
class Source(node.Node):
    packet_buffer = deque()

    nodes = []
    nodes_copy = []
    index1 = -1
    index2 = -1
    flag_copy_load_complete = False

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

    def add_node(self, n):
        """
        Adds node from membership manager to source's own list
        :param n: number of nodes
        """
        temp_node_list = self.membership_manager.get_nodes(n)
        if temp_node_list:
            self.nodes.extend(temp_node_list)
        print(self.nodes)

    def send_delete_packet(self, processor_node):
        """
        send packet to delete element and decrease size of processor node
        :param processor_node:
        """
        p = Packet(constants.DATATYPE_DELETE)
        self.send(p, processor_node[0], processor_node[1])

    def change_subwindow_size_of_node(self, n, size):
        """
        change subwindow size of a node
        :param n: node to be changed
        :param size: new window size
        """
        p = Packet(constants.DATATYPE_SUBWINDOW_SIZE)
        p.append_data(size)
        self.send(p, n[0], n[1])

    def get_saver(self):
        """
        Find the next saver of a packet according to subwindows of nodes

        :return: node: host and port tuple
        """
        if len(self.nodes_copy) == 0:       # assign flag if copy is loaded
            self.flag_copy_load_complete = False

        if self.flag_copy_load_complete is False:   # if copy not loaded, load copy
            self.index1 += 1
            self.index1 %= len(self.nodes)
            self.nodes_copy.append(deepcopy(self.nodes[self.index1]))   # deep copy
            if len(self.nodes) == len(self.nodes_copy):
                self.flag_copy_load_complete = True

        self.index2 += 1
        self.index2 %= len(self.nodes_copy)
        if self.nodes_copy[self.index2][COL_SUBW] > 1:  # if subw more than 1, reduce and return node
            self.nodes_copy[self.index2][COL_SUBW] -= 1
        elif self.nodes_copy[self.index2][COL_SUBW] == 1:   # if subw is 1, remove element, adjust index, return node
            el = self.nodes_copy[self.index2]
            del self.nodes_copy[self.index2]
            self.index2 -= 1
            return el

        return self.nodes_copy[self.index2][COL_NODE]

    @asyncio.coroutine
    def start_streaming(self):
        """
        start sending packets from buffer
        """
        # pack = None
        while len(self.packet_buffer) > 0:
            pack = self.packet_buffer.popleft()
            pack.saver = self.get_saver()
            self.distribute(pack)
            yield from asyncio.sleep(0.5)
        # self.temp_flag_sending = False
        # self.run_server()
        # src.send(pack, '127.0.0.1', 12350)

    def distribute(self, packet):
        """
        distributes packet to different processor nodes
        :param packet: packet to distribute
        """

        # print(self.nodes)
        for row in self.nodes:
            (host, port) = row
            self.send(packet, host, port)

    def do(self, packet):
        """
        packet handling method
        :param packet:
        """
        if packet.type == constants.DATATYPE_HEARTBEAT:
            self.membership_manager.handle_heartbeat(packet)

        elif packet.type == constants.SIGNAL_START_STREAM:
            asyncio.async(self.start_streaming())

        elif packet.type == constants.SIGNAL_ADD_NODE:
            self.add_node(int(packet.data[0]))

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

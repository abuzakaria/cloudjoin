import json
from membership_manager import MembershipManager

__author__ = 'Zakaria'
import utils
import node
import asyncio
import parameters
from packet import Packet
from collections import deque
from copy import deepcopy

# column index of nodes list
COL_NODE = 0
COL_SUBW = 1
COL_CHANGE = 2


# source or first node of the network
class Source(node.Node):
    packet_buffer = deque()

    nodes = []
    nodes_copy = []
    index_main = -1
    index_copy = -1
    flag_copy_load_complete = False

    pending_nodes = []

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

    def add_node(self, n, mode=parameters.MODE_ADD_NODE_DEFAULT):
        """
        Adds node from membership manager to source's own list
        :param mode: pending node, or active node list
        :param n: number of nodes
        :return temp_node_list: added nodes or blank array
        """
        temp_node_list = self.membership_manager.get_nodes(n)
        if temp_node_list:
            if mode == parameters.MODE_ADD_NODE_DEFAULT:  # add node to node list
                self.nodes.extend(temp_node_list)
                print("After acquiring nodes:\n\t" + ("\n\t".join(map(str, self.nodes))))
            elif mode == parameters.MODE_ADD_NODE_PENDING:  # add node to pending node list
                self.pending_nodes.extend(temp_node_list)
                print("After acquiring pending nodes:\n\t" + ("\n\t".join(map(str, self.pending_nodes))))
        return temp_node_list

    def add_pending_nodes(self):
        """
        Adds pending nodes to main nodes list and empties pending nodes

        """
        if len(self.pending_nodes) > 0:
            self.nodes.extend(self.pending_nodes)
            self.pending_nodes.clear()

    def send_delete_packet(self, processor_node):
        """
        send packet to delete element and decrease size of processor node
        :param processor_node:
        """

        p = Packet(parameters.DATATYPE_DELETE)
        print(p.type + ' ' + str(processor_node))
        self.send(p, processor_node[0], processor_node[1])

    def send_change_subwindow_size(self, n, change):
        """
        change subwindow size of a node
        :param change: change to be applied
        :param n: node to be changed
        """
        p = Packet(parameters.DATATYPE_CHANGE_SUBWINDOW_SIZE)
        p.append_data(change)
        print(p.type + ' ' + str(n) + str(change))
        self.send(p, n[0], n[1])

    def send_set_subwindow_size_of_node(self, n, size):
        """
        set subwindow size of a node, only if current size is 0
        :param change: change to be applied
        :param n: node to be changed
        """
        p = Packet(parameters.DATATYPE_SET_SUBWINDOW_SIZE)
        p.append_data(size)
        print(p.type + ' ' + str(n) + str(size))
        self.send(p, n[0], n[1])

    def get_next_saver(self):
        """
        Find the next saver of a packet

        :return: node: host and port tuple
        """
        if parameters.parameter_mode == parameters.MODE_TIME:
            # if time based, just rotate turn

            self.add_pending_nodes()  # doesnt matter when add new node.

            self.index_main += 1
            self.index_main %= len(self.nodes)
            return self.nodes[self.index_main][COL_NODE]

        elif parameters.parameter_mode == parameters.MODE_COUNT:
            # else if count based, complicated process

            if len(self.nodes_copy) == 0:  # assign flag if copy is loaded
                self.flag_copy_load_complete = False
                self.add_pending_nodes()  # only add pending node after end of a cycle

            if self.flag_copy_load_complete is False:  # if copy not loaded, load copy
                self.index_main += 1
                self.index_main %= len(self.nodes)
                temp_row = deepcopy(self.nodes[self.index_main])
                self.nodes_copy.append(temp_row)

                # after copying done, apply change in main list
                self.nodes[self.index_main][COL_SUBW] += self.nodes[self.index_main][COL_CHANGE]
                self.nodes[self.index_main][COL_CHANGE] = 0
                if self.nodes[self.index_main][COL_SUBW] == 0:  # empty subw, node deleted
                    del self.nodes[self.index_main]
                    self.index_main -= 1

                # reducing window size in copy beforehand
                if self.nodes_copy[self.index_main][COL_CHANGE] < 0:
                    self.nodes_copy[self.index_main][COL_SUBW] += self.nodes_copy[self.index_main][COL_CHANGE]

                # update flag true if copy done
                if len(self.nodes) == len(self.nodes_copy):
                    self.flag_copy_load_complete = True

            self.index_copy += 1
            self.index_copy %= len(self.nodes_copy)

            # if subw more than 1, reduce and return node
            if self.nodes_copy[self.index_copy][COL_SUBW] > 1:
                self.nodes_copy[self.index_copy][COL_SUBW] -= 1
                return self.nodes_copy[self.index_copy][COL_NODE]

            # if subw is 1, change is 0, remove element, adjust index, return node
            # if subw is 1, change not 0, put change in subw, make change 0, return node
            elif self.nodes_copy[self.index_copy][COL_SUBW] == 1:
                el = self.nodes_copy[self.index_copy]
                if self.nodes_copy[self.index_copy][
                    COL_CHANGE] == 0:  # change is 0, remove element, adjust index, return node
                    del self.nodes_copy[self.index_copy]
                    self.index_copy -= 1
                else:  # put change in subw, make change 0, return node
                    self.nodes_copy[self.index_copy][COL_SUBW] = self.nodes_copy[self.index_copy][COL_CHANGE]
                    if self.nodes_copy[self.index_copy][
                        COL_CHANGE] > 0:  # send change size packet if > 1. otherwise handle later with drop packet
                        self.send_change_subwindow_size(self.nodes_copy[self.index_copy][COL_NODE],
                                                        self.nodes_copy[self.index_copy][COL_CHANGE])
                    self.nodes_copy[self.index_copy][COL_CHANGE] = 0  # after applying change, make change 0

                return el[COL_NODE]

            elif self.nodes_copy[self.index_copy][COL_SUBW] < 0:
                self.send_delete_packet(self.nodes_copy[self.index_copy][COL_NODE])
                if self.nodes_copy[self.index_copy][COL_SUBW] < -1:
                    self.nodes_copy[self.index_copy][COL_SUBW] += 1
                else:
                    del self.nodes_copy[self.index_copy]
                    self.index_copy -= 1
                return self.get_next_saver()

    @asyncio.coroutine
    def start_streaming(self):
        """
        start sending packets from buffer
        """
        for pack in self.packet_buffer:
            pack.saver = self.get_next_saver()
            self.distribute(pack)
            yield from asyncio.sleep(0.5)

    def distribute(self, packet):
        """
        distributes packet to different processor nodes
        :param packet: packet to distribute
        """

        print(packet.type + packet.data[0] + str(packet.saver))
        for row in self.nodes:
            (host, port) = row[COL_NODE]
            self.send(packet, host, port)

    def set_sw_change(self, host, port, change):
        """
        Set non-zero change to change column,
        zero means change = -subwindow_size, which will remove node once empty
        :param host:
        :param port:
        :param change:
        """
        for row in self.nodes:
            if (host, port) == row[COL_NODE]:
                if change:
                    row[COL_CHANGE] = change
                else:
                    row[COL_CHANGE] = -1 * row[COL_SUBW]
                # print(row)
                return

    def process_modes(self, args):
        """
        Processes 12 modes
        :param args: array of arguments
        """
        mode = args[0]

        if mode == "A" or mode == "J":
            # ['A', '127.0.0.1', '12345', '-3']
            # ['J', '127.0.0.1', '12345', '3']
            if len(args) != 4:
                return
            self.mode_function_change_subwindow(args[1], int(args[2]), int(args[3]))

        elif mode == "B" or mode == "G" or mode == "K":
            # ['B', '127.0.0.1', '12345', '3', '127.0.0.1', '12346']
            # ['G', '127.0.0.1', '12345', '10', '127.0.0.1', '12346']
            # ['K', '127.0.0.1', '12345', '13', '127.0.0.1', '12346']
            if len(args) != 6:
                return
            self.mode_function_change_subwindow(args[1], int(args[2]), int(args[3]))
            self.mode_function_remove_node(args[4], args[5])

        elif mode == "C":
            # ['C', '127.0.0.1', '12345']
            if len(args) != 3:
                return
            self.mode_function_remove_node(args[1], int(args[2]))

        elif mode == "D" or mode == "F" or mode == "I":
            # ['D', '1', '3', '127.0.0.1', '12345', '-5']
            # ['F', '1', '3', '127.0.0.1', '12345', '-3']
            # ['I', '1', '10', '127.0.0.1', '12345', '-3']
            if len(args) != 6:
                return
            self.mode_function_add_node(int(args[1]), int(args[2]))
            self.mode_function_change_subwindow(args[3], int(args[4]), int(args[5]))

        elif mode == "E":
            # ['E', '127.0.0.1', '12345', '-3', '127.0.0.1', '12346', '3']
            if len(args) != 7:
                return
            self.mode_function_change_subwindow(args[1], int(args[2]), int(args[3]))
            self.mode_function_change_subwindow(args[4], int(args[5]), int(args[6]))

        elif mode == "H":
            # ['H', '1', '10', '127.0.0.1', '12345']
            if len(args) != 5:
                return
            self.mode_function_add_node(int(args[1]), int(args[2]))
            self.mode_function_remove_node(args[3], int(args[4]))

        elif mode == "L":
            # ['L', '1', '7']
            if len(args) != 3:
                return
            self.mode_function_add_node(int(args[1]), int(args[2]))

    def mode_function_change_subwindow(self, host, port, sw_change):
        """
        Changes subwindow of a node by sw_change with  host and port

        :param host: string
        :param port: integer
        :param sw_change: integer
        """
        if sw_change:
            self.set_sw_change(host, port, sw_change)

    def mode_function_add_node(self, count, sw_size):
        """
        Adds 'count' number of nodes with size 'sw_size'
        :param count: int
        :param sw_size: int
        """
        new_nodes = self.add_node(count, mode=parameters.MODE_ADD_NODE_PENDING)
        for nd in new_nodes:  # send to processor size info
            self.send_set_subwindow_size_of_node(nd, sw_size)
        for nd in self.pending_nodes:  # update size in pending, which will later be added to source array
            if nd in new_nodes:
                nd[COL_SUBW] = sw_size

    def mode_function_remove_node(self, host, port):
        """
        Removes node with host and port
        :param host: string
        :param port: integer
        """
        self.set_sw_change(host, port, 0)

    def do(self, packet):
        """
        packet handling method
        :param packet: Packet type
        """
        if packet.type == parameters.DATATYPE_HEARTBEAT:
            self.membership_manager.handle_heartbeat(packet)

        elif packet.type == parameters.SIGNAL_START_STREAM:
            asyncio.async(self.start_streaming())

        elif packet.type == parameters.SIGNAL_ADD_NODE:
            self.add_node(int(packet.data[0]), mode=parameters.MODE_ADD_NODE_DEFAULT)

        elif packet.type == parameters.SIGNAL_SET_SUBWINDOW_SIZE and \
                        parameters.parameter_mode == parameters.MODE_COUNT:
            size = int(packet.data[0])
            for row in self.nodes:
                n = row[COL_NODE]
                row[COL_SUBW] = size
                self.send_set_subwindow_size_of_node(n, size)

        elif packet.type == parameters.SIGNAL_MODE:
            args = packet.data[0]
            self.process_modes(args)

    def load_packet_buffer(self):
        """
        temporary function to fill packet buffer.
        """
        with open("data.txt") as f:
            lines = f.read().splitlines()
            l = len(lines)
            for i in range(0, l, 3):  # loop through 3 lines at a time
                pack = Packet(lines[i])
                pack.append_data(lines[i + 1])
                pack.append_data(lines[i + 2])
                self.packet_buffer.append(pack)


# ##########################################################

if __name__ == '__main__':
    src = Source('source', parameters.SOURCE_HOST, parameters.SOURCE_PORT)
    src.run_server()

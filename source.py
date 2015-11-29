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
from math import ceil
# column index of nodes list
COL_NODE = 0
COL_SUBW = 1
COL_CHANGE = 2


# source or first node of the network
class Source(node.Node):

    def __init__(self, name=None, host=None, port=None):
        """
        Initializes node with name host and port
        :param name: string to identify a node
        :param host: host of node. i.e.: 127.0.0.1
        :param port: port of node
        """
        super().__init__(name, host, port)

        self.cached_length_nodes = 0
        self.data_packets_array = deque()

        self.nodes = []
        self.index_of_nodes = [-1, -1]

        self.nodes_copy = [[], []]
        self.index_of_nodes_copy = [-1, -1]
        self.flag_copy_load_complete = [False, False]

        self.flag_apply_change = []

        self.pending_nodes = []

        self.load_data_packets_from_file()
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
                self.send_merger_node_serial()
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
            self.send_merger_node_serial()

    def send_merger_node_serial(self):
        """
        Sends merger nodes in serial. For merge, merger needs this info.

        """
        p = Packet(parameters.DATATYPE_NODE_SERIAL)
        for nd in self.nodes:
            p.data.append(nd[COL_NODE])
        asyncio.async(self.send(p, parameters.MERGER_HOST, parameters.MERGER_PORT))

    def send_delete_packet(self, processor_node, storage_type):
        """
        send packet to delete element and decrease size of processor node
        :param storage_type: int. 0 means delete R, 1 means delete S
        :param processor_node: node
        """
        if storage_type == 0:
            p = Packet(parameters.DATATYPE_DELETE_R)
        elif storage_type == 1:
            p = Packet(parameters.DATATYPE_DELETE_S)
        else:
            print("invalid delete packet")
            return
        print(p.type + ' ' + str(processor_node))
        asyncio.async(self.send(p, processor_node[0], processor_node[1]))

    def send_increase_subwindow_size(self, n, change, region):
        """
        change subwindow size of a node
        :param region: which region to change size
        :param change: change to be applied
        :param n: node to be changed
        """
        if region == 0:
            p = Packet(parameters.DATATYPE_INCREASE_SUBWINDOW_SIZE_R)
        elif region == 1:
            p = Packet(parameters.DATATYPE_INCREASE_SUBWINDOW_SIZE_S)
        else:
            print("invalid increase sw packet")
            return
        p.append_data(change)
        print(p.type + ' ' + str(n) + str(change))
        asyncio.async(self.send(p, n[0], n[1]))

    def send_set_subwindow_size_of_node(self, n, size):
        """
        set subwindow size of a node
        :param size: int
        :param n: node to be changed
        """
        p = Packet(parameters.DATATYPE_SET_SUBWINDOW_SIZE)
        p.append_data(size)
        print(p.type + ' ' + str(n) + str(size))
        asyncio.async(self.send(p, n[0], n[1]))

    def del_node_from_main_node_list(self, index):
        """
        Receive exactly 2 confirmations and then apply change.
        :param index: int. index of node/flag array to check whether to update
        """
        if self.flag_apply_change[index] == 0:  # if only one command to apply change, wait for another
            self.flag_apply_change[index] += 1
        elif self.flag_apply_change[index] == 1:  # if already one command appeared before, apply change, reset flag
            self.flag_apply_change[index] = 0
            # after copying done, apply change in main list
            for i in range(len(self.index_of_nodes)):
                self.nodes[self.index_of_nodes[i]][COL_SUBW] += self.nodes[self.index_of_nodes[i]][COL_CHANGE]
                self.nodes[self.index_of_nodes[i]][COL_CHANGE] = 0
                if self.nodes[self.index_of_nodes[i]][COL_SUBW] == 0:  # empty subw, node deleted
                    del self.nodes[self.index_of_nodes[i]]
                    del self.flag_apply_change[index]  # delete corresponding flag for node
                    self.send_merger_node_serial()
                    self.index_of_nodes[i] -= 1
        print(self.flag_apply_change)

    @asyncio.coroutine
    def get_next_saver(self, packet_type):
        """
        Find the next saver of a packet

        :return: node: host and port tuple
        """
        # set index for either r or s type
        if packet_type == parameters.DATATYPE_R_STREAM:
            r_or_s = 0
        elif packet_type == parameters.DATATYPE_S_STREAM:
            r_or_s = 1
        else:
            return None

        # if time based, just rotate turn
        if parameters.join_mode == parameters.MODE_TIME:

            self.add_pending_nodes()  # doesnt matter when add new node.

            self.index_of_nodes[r_or_s] += 1
            self.index_of_nodes[r_or_s] %= len(self.nodes)
            if self.nodes[self.index_of_nodes[r_or_s]][COL_CHANGE] == -1:
                return self.get_next_saver(packet_type)
            else:
                return self.nodes[self.index_of_nodes[r_or_s]][COL_NODE]

        # else if count based, complicated process
        elif parameters.join_mode == parameters.MODE_COUNT:
            # this block is for (re)initializing before each cycle starts
            if len(self.nodes_copy[r_or_s]) == 0:  # assign flag if copy is loaded
                self.flag_copy_load_complete[r_or_s] = False
                self.index_of_nodes[r_or_s] = -1  # init main index when copy again
                self.index_of_nodes_copy[r_or_s] = -1   # init copy index
                self.cached_length_nodes = len(self.nodes)      # cache nodes length, so that deleting node does not affect flag_copy_load_complete becoming true
                self.add_pending_nodes()  # only add pending node after end of a cycle
                self.flag_apply_change = [0] * len(self.nodes)  # init apply change flag with 0

            if self.flag_copy_load_complete[r_or_s] is False:  # copy will be loaded one by one from main node list
                self.index_of_nodes[r_or_s] += 1
                self.index_of_nodes[r_or_s] %= len(self.nodes)
                temp_row = deepcopy(self.nodes[self.index_of_nodes[r_or_s]])
                self.nodes_copy[r_or_s].append(temp_row)

                # reducing window size in copy beforehand
                if self.nodes_copy[r_or_s][self.index_of_nodes[r_or_s]][COL_CHANGE] < 0:
                    self.nodes_copy[r_or_s][self.index_of_nodes[r_or_s]][COL_SUBW] += self.nodes_copy[r_or_s][self.index_of_nodes[r_or_s]][COL_CHANGE]

                # after copying done, if subwindow 0, remove node from main list (after 2 prompt from r and s)
                if self.nodes_copy[r_or_s][self.index_of_nodes[r_or_s]][COL_SUBW] == 0:
                    self.del_node_from_main_node_list(self.index_of_nodes[r_or_s])

                # update flag true if copy done. compare length with cached length, because nodes maybe already deleted in main node list
                if self.cached_length_nodes == len(self.nodes_copy[r_or_s]):
                    self.flag_copy_load_complete[r_or_s] = True

            self.index_of_nodes_copy[r_or_s] += 1
            self.index_of_nodes_copy[r_or_s] %= len(self.nodes_copy[r_or_s])

            # if subw more than 1, reduce and return node
            if self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_SUBW] > 1:
                self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_SUBW] -= 1
                return self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_NODE]

            # if subw is 1, change is 0, remove element, adjust index, return node
            # if subw is 1, change not 0, put change in subw, make change 0, return node
            elif self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_SUBW] == 1:
                el = self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]]

                # if change is 0, remove element, adjust index, return node
                if self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_CHANGE] == 0:
                    del self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]]
                    self.index_of_nodes_copy[r_or_s] -= 1

                # put change in subw, make change 0, return node
                else:
                    self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_SUBW] = self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_CHANGE]
                    if self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_CHANGE] > 0:
                        # send increase size packet if > 1. otherwise handle later with drop packet
                        self.send_increase_subwindow_size(self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_NODE],
                                                          self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_CHANGE],
                                                          r_or_s)

                    # after applying change, make change 0
                    self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_CHANGE] = 0

                return el[COL_NODE]

            else:
                self.send_delete_packet(self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_NODE], r_or_s)
                if self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_CHANGE] < -1:
                    self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]][COL_CHANGE] += 1
                else:
                    del self.nodes_copy[r_or_s][self.index_of_nodes_copy[r_or_s]]
                    self.index_of_nodes_copy[r_or_s] -= 1
                return self.get_next_saver(packet_type)

    @asyncio.coroutine
    def start_streaming(self):
        """
        start sending packets from buffer
        """
        for pack in self.data_packets_array:
            pack.saver = yield from self.get_next_saver(pack.type)
            # print(pack.type + pack.data[0] + ' saver: ' + str(pack.saver))
            yield from self.distribute(pack)
            # yield from asyncio.sleep(0.5)		# only to test

    @asyncio.coroutine
    def distribute(self, packet):
        """
        distributes packet to different processor nodes
        :param packet: packet to distribute
        """
        for row in self.nodes:
            (host, port) = row[COL_NODE]
            print(packet.type + packet.data[0] + " " + str(port))
            yield from self.send(packet, host, port)

    @asyncio.coroutine
    def timed_wait_to_delete_node_from_main_list(self, host, port):
        """
        Deletes node from main node list after waiting for the node to expire all stored tuples
        :param host: string
        :param port: int
        :return:
        """
        yield from asyncio.sleep(ceil(parameters.subwindow_time / 1000))   # wait for seconds, divide by 1000 to convert ms to s
        for i in range(len(self.nodes)):
            if self.nodes[i][COL_NODE] == (host, port):
                del self.nodes[i]
                print(self.nodes)
                self.send_merger_node_serial()
                return

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
                    if parameters.join_mode == parameters.MODE_COUNT:
                        row[COL_CHANGE] = -1 * row[COL_SUBW]
                    elif parameters.join_mode == parameters.MODE_TIME:
                        row[COL_CHANGE] = -1
                        asyncio.async(self.timed_wait_to_delete_node_from_main_list(host, port))

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
        if parameters.join_mode == parameters.MODE_COUNT:
            new_nodes = self.add_node(count, mode=parameters.MODE_ADD_NODE_PENDING)
            if new_nodes:
                for nd in new_nodes:  # send to processor size info
                    self.send_set_subwindow_size_of_node(nd[COL_NODE], sw_size)
                    nd[COL_SUBW] = sw_size
                    # for nd in self.pending_nodes:  # update size in pending, which will later be added to source array
                    # if nd in new_nodes:
                    # nd[COL_SUBW] = sw_size
        elif parameters.join_mode == parameters.MODE_TIME:
            self.add_node(count)

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

        elif packet.type == parameters.SIGNAL_SET_SUBWINDOW_TIME and parameters.join_mode == parameters.MODE_TIME:
            t = int(packet.data[0])
            utils.set_subwindow_time(t)

        elif packet.type == parameters.SIGNAL_SET_SUBWINDOW_SIZE and parameters.join_mode == parameters.MODE_COUNT:
            size = int(packet.data[0])
            for row in self.nodes:
                n = row[COL_NODE]
                row[COL_SUBW] = size
                self.send_set_subwindow_size_of_node(n, size)

        elif packet.type == parameters.SIGNAL_MODE:
            args = packet.data[0]
            self.process_modes(args)

    def load_data_packets_from_file(self):
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
                self.data_packets_array.append(pack)


# ##########################################################
from sys import argv
if __name__ == '__main__':
    src = Source('source', argv[1], argv[2])
    src.run_server()

import asyncio
import parameters

__author__ = 'Zakaria'

import node
import json


#destination or sink or last node of the network
class Destination(node.Node):
    nodes = []
    node_index_r_join = 0
    node_index_s_join = 0

    r_join_results_queue = []
    s_join_results_queue = []

    is_merging_r = False
    is_merging_s = False

    @asyncio.coroutine
    def merge_r(self):
        print("merge r")
        if self.is_merging_r:
            return
        self.is_merging_r = True
        for packet in self.r_join_results_queue:
            if (packet.sender['host'], packet.sender['port']) == self.nodes[self.node_index_r_join]:
                self.node_index_r_join += 1
                self.node_index_r_join %= len(self.nodes)
                yield from self.print_packet(packet)
        self.is_merging_r = False

    @asyncio.coroutine
    def merge_s(self):
        print("merge s")
        if self.is_merging_s:
            return
        self.is_merging_s = True
        for packet in self.s_join_results_queue:
            if (packet.sender['host'], packet.sender['port']) == self.nodes[self.node_index_s_join]:
                self.node_index_s_join += 1
                self.node_index_s_join %= len(self.nodes)
                yield from self.print_packet(packet)
        self.is_merging_s = False

    @asyncio.coroutine
    def print_packet(self, packet):
        print(packet.type + str(len(packet.data)))
        # print(packet.type + str(len(packet.data)))
        for p in packet.data:
            print(packet.type + ' : ' + json.dumps(p))
        print("----------------------------------------")

    def do(self, packet):
        """

        :param packet:
        :param sender:
        """
        if packet.type == parameters.DATATYPE_R_JOIN:
            self.r_join_results_queue.append(packet)
            asyncio.async(self.merge_r())

        elif packet.type == parameters.DATATYPE_S_JOIN:
            self.s_join_results_queue.append(packet)
            asyncio.async(self.merge_s())

        elif packet.type == parameters.DATATYPE_NODE_SERIAL:
            self.nodes = packet.data[:]
            print(self.nodes)



#Test run
if __name__ == '__main__':
    dest = Destination('destination', '127.0.0.1', 12350)
    dest.run_server()

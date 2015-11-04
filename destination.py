import asyncio
import parameters

__author__ = 'Zakaria'

import node
import json


#destination or sink or last node of the network
class Destination(node.Node):

    def __init__(self, name, host, port):
        super().__init__(name, host, port)
        self.nodes = []
        self.node_index_r_join = 0
        self.node_index_s_join = 0

        self.r_join_results_queue = []
        self.s_join_results_queue = []

        self.is_merging_r = False
        self.is_merging_s = False

    @asyncio.coroutine
    def merge_r(self, packet):
        print("merge r")
        if (packet.sender['host'], packet.sender['port']) == self.nodes[self.node_index_r_join]:
            self.node_index_r_join += 1
            self.node_index_r_join %= len(self.nodes)
            # yield from self.print_packet(packet)

    @asyncio.coroutine
    def merge_s(self, packet):
        print("merge s")
        if (packet.sender['host'], packet.sender['port']) == self.nodes[self.node_index_s_join]:
            self.node_index_s_join += 1
            self.node_index_s_join %= len(self.nodes)
            # yield from self.print_packet(packet)

    @asyncio.coroutine
    def print_packet(self, packet, filename="_result.txt"):
        # print(packet.type + str(len(packet.data)))
        # print(packet.type + str(len(packet.data)))
        with open(filename, 'a') as f:
            for p in packet.data:
                print(packet.type + ' : ' + json.dumps(p), file=f)
            print("----------------------------------------", file=f)

    @asyncio.coroutine
    def update_node_serial(self, packet):
        self.nodes = packet.data[:]
        print(self.nodes)

    def do(self, packet):
        """

        :param packet:
        """
        if packet.type == parameters.DATATYPE_R_JOIN:
            asyncio.async(self.merge_r(packet))
            # asyncio.async(self.print_packet(packet, "_log_dest.txt"))

        elif packet.type == parameters.DATATYPE_S_JOIN:
            asyncio.async(self.merge_s(packet))
            # asyncio.async(self.print_packet(packet, "_log_dest.txt"))

        elif packet.type == parameters.DATATYPE_NODE_SERIAL:
            asyncio.async(self.update_node_serial(packet))
        # print(packet.type)

#Test run
if __name__ == '__main__':
    destination = Destination('destination', '127.0.0.1', 12350)
    destination.run_server()

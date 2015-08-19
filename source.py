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
        p = Packet(constants.DATATYPE_PROTOCOL)
        p.append_data(n)
        p.append_data(out_of)
        self.send(p, processor_node[0], processor_node[1])

    def send_delete_packet(self, processor_node):
        """

        :param processor_node:
        """
        p = Packet(constants.DATATYPE_DELETE)
        self.send(p, processor_node)

    def change_window_size(self, n, size):
        """

        :param n: node to be changed
        :param size: new window size
        """
        p = Packet(constants.DATATYPE_WINDOW_SIZE)
        p.append_data(size)
        self.send(p, n[0], n[1])

    def send_store_protocol_packet_batch(self):
        number_of_nodes = len(self.nodes)
        for x in range(number_of_nodes):
            self.send_store_protocol_packet(self.nodes[x], x, number_of_nodes)

    def do(self, packet):
        """

        :param packet:
        """
        if packet.type == constants.DATATYPE_HEARTBEAT:
            self.membership_manager.handle_heartbeat(packet)

        elif packet.type == constants.SIGNAL_STORE_PROTOCOL:
            self.send_store_protocol_packet(self.nodes[int(packet.data[0])],
                                            int(packet.data[1]),
                                            int(packet.data[2]))

        elif packet.type == constants.SIGNAL_STORE_PROTOCOL_BATCH:
            self.send_store_protocol_packet_batch()

        elif packet.type == constants.SIGNAL_START_STREAM:
            asyncio.async(self.start_streaming())

        elif packet.type == constants.SIGNAL_GET_NODE:
            self.nodes = self.membership_manager.get_nodes(int(packet.data[0]))

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

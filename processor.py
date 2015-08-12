__author__ = 'Zakaria'

from node import Node
from region import Region
import asyncio
import constants


#cores or joining nodes.
class Processor(Node):
    next_node = None
    n_th = 0
    mod_by = 0
    data_packet_counter = 0

    def __init__(self, name=None, host=None, port=None, window_size=constants.WINDOW_SIZE):
        """

        :param name:
        :param host:
        :param port:
        :param window_size:
        """
        if host:
            self.host = host
        if port:
            self.port = port
        if name:
            self.name = name
        self.LR = Region(constants.DATATYPE_S_STREAM, window_size)
        self.RR = Region(constants.DATATYPE_R_STREAM, window_size)
        self.loop = asyncio.get_event_loop()

    def set_storing_protocol(self, n, out_of):
        self.n_th = n
        self.mod_by = out_of

    def store_flag(self, packet_no):
        return packet_no % self.mod_by == self.n_th

    def do(self, packet):
        """

        :param packet:
        """
        print(packet.sender["name"] + ' >| ' + packet.type + ' |> ' + self.name)
        print("SIZE:" + str(self.LR.window_size) + ' ' + str(self.RR.window_size))
        join_result = None

        if packet.type == constants.DATATYPE_R_STREAM:
            if self.mod_by == 0:
                print("No store protocol defined")
                return
            self.data_packet_counter += 1
            if self.store_flag(self.data_packet_counter):
                self.RR.store(packet)      # store r
            join_result = self.LR.process(packet)
        elif packet.type == constants.DATATYPE_S_STREAM:
            if self.mod_by == 0:
                print("No store protocol defined")
                return
            self.data_packet_counter += 1
            if self.store_flag(self.data_packet_counter):
                self.LR.store(packet)      # store s
            join_result = self.RR.process(packet)
        elif packet.type == constants.DATATYPE_DELETE:
            self.LR.decrease_size()
            self.RR.decrease_size()
        elif packet.type == constants.DATATYPE_PROTOCOL:
            self.set_storing_protocol(packet.data[0], packet.data[1])

        if join_result and len(join_result.data) > 0:
            self.send(join_result, self.next_node[0], self.next_node[1])
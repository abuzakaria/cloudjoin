__author__ = 'Zakaria'

from node import Node
from region import Region
import asyncio
import constants
from packet import Packet


# cores or joining nodes.
class Processor(Node):
    next_node = None

    def __init__(self, name=None, host=None, port=None,
                 subwindow_size=constants.SUBWINDOW_SIZE,
                 reliability=constants.COST_FUNCTION_DEFAULT_PARAM,
                 availability=constants.COST_FUNCTION_DEFAULT_PARAM,
                 throughput=constants.COST_FUNCTION_DEFAULT_PARAM,
                 power_consumption=constants.COST_FUNCTION_DEFAULT_PARAM,
                 processing_latency=constants.COST_FUNCTION_DEFAULT_PARAM,
                 transmission_latency=constants.COST_FUNCTION_DEFAULT_PARAM,
    ):
        """

        :param name:
        :param host:
        :param port:
        :param subwindow_size:
        """
        if host:
            self.host = host
        if port:
            self.port = port
        if name:
            self.name = name    # used only for printing purpose, no logic
        self.LR = Region(constants.DATATYPE_S_STREAM, subwindow_size)
        self.RR = Region(constants.DATATYPE_R_STREAM, subwindow_size)
        self.cost_value = (reliability * availability * throughput) / \
                          (power_consumption * processing_latency * transmission_latency)
        self.loop = asyncio.get_event_loop()

    def send_heartbeat(self, interval):
        """
        Sends a heartbeat to manager after a defined interval
        :param interval: gap between heartbeats
        """
        p = Packet(constants.DATATYPE_HEARTBEAT)
        p.append_data(self.LR.subwindow_size)
        p.append_data(self.cost_value)
        self.send(p, self.membership_manager[0], self.membership_manager[1])
        self.loop.call_later(interval, self.send_heartbeat, interval)

    def set_subwindow_size(self, n):
        """
        Set subwindow size of the regions
        :param n: new size
        """
        self.LR.subwindow_size = n
        self.RR.subwindow_size = n

    def do(self, packet):
        """

        :param packet:
        """
        print(packet.sender["name"] + ' >| ' + packet.type + ' |> ' + self.name)
        # print("SIZE:" + str(self.LR.subwindow_size) + ' ' + str(self.RR.subwindow_size))
        join_result = None

        if packet.type == constants.DATATYPE_R_STREAM:  # r packet
            if packet.saver == (self.host, self.port):
                self.RR.store(packet)  # store r
            join_result = self.LR.process(packet)

        elif packet.type == constants.DATATYPE_S_STREAM:    # s packet
            if packet.saver == (self.host, self.port):
                self.LR.store(packet)  # store s
            join_result = self.RR.process(packet)

        elif packet.type == constants.DATATYPE_DELETE:  # delete packet
            if packet.saver == (self.host, self.port):
                self.LR.decrease_size()
                self.RR.decrease_size()

        # separate if: send result if exists
        if join_result and len(join_result.data) > 0:
            self.send(join_result, self.next_node[0], self.next_node[1])
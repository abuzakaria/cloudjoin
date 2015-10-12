__author__ = 'Zakaria'

from node import Node
from region import Region
import asyncio
import parameters
from packet import Packet
import sys


# cores or joining nodes.
class Processor(Node):
    next_node = None

    def __init__(self, name=None, host=None, port=None,
                 reliability=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 availability=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 throughput=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 power_consumption=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 processing_latency=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 transmission_latency=parameters.COST_FUNCTION_DEFAULT_PARAM,
    ):
        """


        :param reliability:
        :param availability:
        :param throughput:
        :param power_consumption:
        :param processing_latency:
        :param transmission_latency:
        :param name:
        :param host:
        :param port:
        """
        if host:
            self.host = host
        if port:
            self.port = port
        if name:
            self.name = name    # used only for printing purpose, no logic
        self.LR = Region(parameters.DATATYPE_S_STREAM)
        self.RR = Region(parameters.DATATYPE_R_STREAM)
        self.cost_value = (reliability * availability * throughput) / \
                          (power_consumption * processing_latency * transmission_latency)
        self.loop = asyncio.get_event_loop()
        sys.stdout = open('_log_' + self.name + '.txt', 'w')

    def send_heartbeat(self, interval):
        """
        Sends a heartbeat to manager after a defined interval
        :param interval: gap between heartbeats
        """
        p = Packet(parameters.DATATYPE_HEARTBEAT)
        p.append_data(self.cost_value)
        self.send(p, self.membership_manager[0], self.membership_manager[1])
        self.loop.call_later(interval, self.send_heartbeat, interval)

    def do(self, packet):
        """

        :param packet:
        """
        # print(packet.sender["name"] + ' >| ' + packet.type + ' |> ' + self.name)
        # print("SIZE:" + str(self.LR.subwindow_size) + ' ' + str(self.RR.subwindow_size))
        join_result = None

        if packet.type == parameters.DATATYPE_R_STREAM:  # r packet
            print(packet.type + packet.data[0] + str(packet.saver))
            if packet.saver == (self.host, self.port):
                self.RR.store(packet)  # store r
            join_result = self.LR.process(packet)

        elif packet.type == parameters.DATATYPE_S_STREAM:    # s packet
            print(packet.type + packet.data[0] + str(packet.saver))
            if packet.saver == (self.host, self.port):
                self.LR.store(packet)  # store s
            join_result = self.RR.process(packet)

        elif packet.type == parameters.DATATYPE_DELETE:  # delete packet
            print(packet.type)
            self.LR.decrease_size()
            self.RR.decrease_size()

        elif packet.type == parameters.DATATYPE_CHANGE_SUBWINDOW_SIZE:
            change = packet.data[0]
            print(packet.type + ' ' + change)
            self.LR.increase_size(change)
            self.RR.increase_size(change)

        elif packet.type == parameters.DATATYPE_SET_SUBWINDOW_SIZE:
            size = packet.data[0]
            print(packet.type + ' ' + str(size))
            self.LR.set_size(size)
            self.RR.set_size(size)

        # separate if: send result if exists
        if join_result and len(join_result.data) > 0:
            self.send(join_result, self.next_node[0], self.next_node[1])


if __name__ == '__main__':
    p = Processor('12345', '127.0.0.1', 12345)
    p.register_membership(('127.0.0.1', 12344))
    p.next_node = ('127.0.0.1', 12350)
    p.run_server()

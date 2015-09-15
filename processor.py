__author__ = 'Zakaria'

from node import Node
from region import Region
import asyncio
import constants
from packet import Packet


# cores or joining nodes.
class Processor(Node):
    next_node = None
    n_th = 0
    mod_by = 0
    data_packet_counter = 0

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
            self.name = name
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
        p.append_data(self.cost_value)
        self.send(p, self.membership_manager[0], self.membership_manager[1])
        self.loop.call_later(interval, self.send_heartbeat, interval)

    def set_storing_protocol(self, n, out_of):
        """

        :param n:
        :param out_of:
        """
        print("STOREPROTOCOL: " + str(n) + ' ' + str(out_of))
        self.n_th = n
        self.mod_by = out_of

    def store_flag(self, packet_no):
        """

        :param packet_no:
        :return:
        """
        return packet_no % self.mod_by == self.n_th

    def increase_subwindow_size(self, n):
        """

        :param n:
        """
        self.LR.subwindow_size += n
        self.RR.subwindow_size += n
        print(self.LR.subwindow_size)
        print(self.RR.subwindow_size)

    def decrease_subwindow_size(self, n, mode):
        """

        :param n:
        :param mode:
        """
        if mode == constants.MODE_SUBW_DEC_LOSSY:
            self.LR.subwindow_size -= n
            self.RR.subwindow_size -= n
            self.LR.decrease_size(n)
            self.RR.decrease_size(n)

    def do(self, packet):
        """

        :param packet:
        """
        print(packet.sender["name"] + ' >| ' + packet.type + ' |> ' + self.name)
        # print("SIZE:" + str(self.LR.subwindow_size) + ' ' + str(self.RR.subwindow_size))
        join_result = None

        if packet.type == constants.DATATYPE_R_STREAM:
            if self.mod_by == 0:
                print("No store protocol defined")
                return
            self.data_packet_counter += 1
            if self.store_flag(self.data_packet_counter):
                self.RR.store(packet)  # store r
            join_result = self.LR.process(packet)

        elif packet.type == constants.DATATYPE_S_STREAM:
            if self.mod_by == 0:
                print("No store protocol defined")
                return
            self.data_packet_counter += 1
            if self.store_flag(self.data_packet_counter):
                self.LR.store(packet)  # store s
            join_result = self.RR.process(packet)

        # elif packet.type == constants.DATATYPE_DELETE:
        #     n = int(packet.data[0])
        #     self.LR.decrease_size(n)
        #     self.RR.decrease_size(n)

        elif packet.type == constants.DATATYPE_SUBWINDOW_SIZE_INC:
            val = int(packet.data[0])
            self.increase_subwindow_size(val)

        elif packet.type == constants.DATATYPE_SUBWINDOW_SIZE_DEC:
            val = int(packet.data[0])
            mode = packet.data[1]
            self.decrease_subwindow_size(val, mode)

        elif packet.type == constants.DATATYPE_PROTOCOL:
            self.set_storing_protocol(packet.data[0], packet.data[1])

        #send result if exists
        if join_result and len(join_result.data) > 0:
            self.send(join_result, self.next_node[0], self.next_node[1])
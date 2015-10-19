import copy
import utils

__author__ = 'Zakaria'

from node import Node
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
        self.host = host
        self.port = port
        self.name = name  # used only for printing purpose, no logic
        self.S_Storage = []
        self.R_Storage = []
        self.subwindow_size = parameters.SUBWINDOW_DEFAULT_SIZE
        if reliability and availability and throughput and power_consumption and processing_latency and transmission_latency:
            self.cost_value = (reliability * availability * throughput) / \
                              (power_consumption * processing_latency * transmission_latency)
        self.loop = asyncio.get_event_loop()
        # sys.stdout = open('_log_' + self.name + '.txt', 'w')

    def send_heartbeat(self, interval):
        """
        Sends a heartbeat to manager after a defined interval
        :param interval: gap between heartbeats
        """
        p = Packet(parameters.DATATYPE_HEARTBEAT)
        p.append_data(self.cost_value)
        self.send(p, self.membership_manager[0], self.membership_manager[1])
        self.loop.call_later(interval, self.send_heartbeat, interval)

    def store(self, packet):
        """

        :param packet:
        """
        # determine in which queue to store packet
        if packet.type == parameters.DATATYPE_R_STREAM:
            storage = self.R_Storage
            current_size = len(self.R_Storage)
        elif packet.type == parameters.DATATYPE_S_STREAM:
            storage = self.S_Storage
            current_size = len(self.S_Storage)
        else:
            return False

        # if count based join, check both queues' length, if less then store,
        # if equal then drop the oldest of the two queue and store,
        # else something wrong
        if parameters.parameter_mode == parameters.MODE_COUNT:
            if current_size == self.subwindow_size and self.subwindow_size > 0:
                storage.pop(0)
            elif current_size > self.subwindow_size:
                return False    # error case
            storage.append(packet)  # storing here

        # if time based join, just store. deletion will take place during joining
        elif parameters.parameter_mode == parameters.MODE_TIME:
            packet.store_time = utils.get_millisecond()
            storage.append(packet)

    def process_joining(self, guest):
        """



        :param guest: packet
        """
        if guest.type == parameters.DATATYPE_R_STREAM:
            host_storage = self.S_Storage
            join_result = Packet(parameters.DATATYPE_R_JOIN)
        elif guest.type == parameters.DATATYPE_S_STREAM:
            host_storage = self.R_Storage
            join_result = Packet(parameters.DATATYPE_S_JOIN)
        else:
            return None

        i = 0
        while i < len(host_storage):
            print(str(len(host_storage)) + ' , ' + str(i))
            host = host_storage[i]
            # deletion in time based. count based deletion is during store.
            if parameters.parameter_mode == parameters.MODE_TIME:
                if utils.get_millisecond() > host.store_time + parameters.SUBWINDOW_DEFAULT_TIME:
                    host_storage.remove(host)
                    # print("removed: " + str(host.type) + str(host.data[0]))
                    continue

            print(host.type + str(host.data[0]) + ' X ' + guest.type + str(guest.data[0]))

            if host.data[parameters.JOIN_CRITERION_INDEX] == guest.data[parameters.JOIN_CRITERION_INDEX]:
                join_result.data.append(host.data + guest.data)
                empty_result = False

            i += 1

        return join_result


    # def drop_oldest_packet(self):
    #     """
    #
    #     Drops the oldest packet from R-storage or S-storage whoever has the oldest
    #     """
    #     len_r = len(self.R_Storage)
    #     len_s = len(self.S_Storage)
    #     if len_s == 0 and len_r == 0:
    #         return
    #     elif len_r == self.subwindow_size:    # if no S, but R is full
    #         self.R_Storage.pop(0)
    #     elif len_s == self.subwindow_size:  # if no R, but S is full
    #         self.S_Storage.pop(0)
    #     # first element of S is older than first element of R
    #     elif self.R_Storage[0].store_time > self.S_Storage[0].store_time:
    #         self.S_Storage.pop(0)
    #     # first element of R is older than first element of S
    #     elif self.S_Storage[0].store_time >= self.R_Storage[0].store_time:
    #         self.R_Storage.pop(0)

    def decrease_size(self):
        """
        remove number of packets and decrease size by 1
        """
        if parameters.parameter_mode == parameters.MODE_COUNT:
            self.R_Storage.pop(0)
            self.S_Storage.pop(0)
            self.subwindow_size -= 1

    def increase_size(self, change):
        """
        Increase subwindow size by change
        :param change: int
        """
        if parameters.parameter_mode == parameters.MODE_COUNT:
            if change > 0:
                self.subwindow_size += change

    def emit_result(self, join_result):
        """

        Emits result to next node, for merging
        """
        # if precision value crossed, punctuate, send packet, and clear result packet data
        if len(join_result.data):
            self.send(join_result, self.next_node[0], self.next_node[1])

    def do(self, packet):
        """

        :param packet:
        """
        # print(packet.sender["name"] + ' >| ' + packet.type + ' |> ' + self.name)
        # print("SIZE:" + str(self.S_Storage.subwindow_size) + ' ' + str(self.R_Storage.subwindow_size))

        if packet.type == parameters.DATATYPE_R_STREAM or packet.type == parameters.DATATYPE_S_STREAM:  # r packet
            print(packet.type + packet.data[0] + str(packet.saver))
            if packet.saver == (self.host, self.port):
                self.store(packet)  # store r
            join_result = self.process_joining(packet)
            self.emit_result(join_result)

        elif packet.type == parameters.DATATYPE_DELETE:  # delete packet
            print(packet.type)
            self.decrease_size()

        elif packet.type == parameters.DATATYPE_CHANGE_SUBWINDOW_SIZE:
            change = int(packet.data[0])
            print(packet.type + ' ' + change)
            self.increase_size(change)
            self.increase_size(change)

        elif packet.type == parameters.DATATYPE_SET_SUBWINDOW_SIZE:
            size = packet.data[0]
            print(packet.type + ' ' + str(size))
            self.subwindow_size = size




if __name__ == '__main__':
    p = Processor('12345', '127.0.0.1', 12345)
    p.register_membership(('127.0.0.1', 12344))
    p.next_node = ('127.0.0.1', 12350)
    p.run_server()

import utils

__author__ = 'Zakaria'

from node import Node
import asyncio
import parameters
from packet import Packet
from sys import argv, exit

# cores or joining nodes.
class Processor(Node):

    def __init__(self, name=None, host=None, port=None, reliability=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 availability=parameters.COST_FUNCTION_DEFAULT_PARAM, throughput=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 power_consumption=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 processing_latency=parameters.COST_FUNCTION_DEFAULT_PARAM,
                 transmission_latency=parameters.COST_FUNCTION_DEFAULT_PARAM):
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
        super().__init__(name, host, port)
        self.S_Storage = []
        self.R_Storage = []
        self.next_node = None
        self.subwindow_size_r = self.subwindow_size_s = parameters.SUBWINDOW_DEFAULT_SIZE
        if reliability and availability and throughput and power_consumption and processing_latency and transmission_latency:
            self.cost_value = (reliability * availability * throughput) / \
                              (power_consumption * processing_latency * transmission_latency)

    def register_membership(self, manager):
        """

        :param manager:
        """
        print("registering to membership membership_manager")
        self.membership_manager = manager
        self.send_heartbeat(parameters.HEARTBEAT_INTERVAL)

    def send_heartbeat(self, interval):
        """
        Sends a heartbeat to manager after a defined interval
        :param interval: gap between heartbeats
        """
        p = Packet(parameters.DATATYPE_HEARTBEAT)
        p.append_data(self.cost_value)
        asyncio.async(self.send(p, self.membership_manager[0], self.membership_manager[1]))
        self.loop.call_later(interval, self.send_heartbeat, interval)

    def store(self, packet):
        """

        :param packet:
        """
        # determine in which queue to store packet
        if packet.type == parameters.DATATYPE_R_STREAM:
            storage = self.R_Storage
            current_size = len(self.R_Storage)
            max_size = self.subwindow_size_r
        elif packet.type == parameters.DATATYPE_S_STREAM:
            storage = self.S_Storage
            current_size = len(self.S_Storage)
            max_size = self.subwindow_size_s
        else:
            return False

        # if count based join, check both queues' length, if less then store,
        # if equal then drop the oldest of the two queue and store,
        # else something wrong
        if parameters.join_mode == parameters.MODE_COUNT:
            if current_size == max_size and max_size > 0:
                storage.pop(0)
            elif current_size > max_size:
                return False    # error case
            storage.append(packet)  # storing here

        # if time based join, just store. deletion will take place during joining
        elif parameters.join_mode == parameters.MODE_TIME:
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
            # print(str(len(host_storage)) + ' , ' + str(i))
            host = host_storage[i]
            # deletion in time based. count based deletion is during store.
            if parameters.join_mode == parameters.MODE_TIME:
                if utils.get_millisecond() > host.store_time + parameters.subwindow_time:
                    host_storage.remove(host)
                    # print("removed: " + str(host.type) + str(host.data[0]))
                    continue

            print(host.type + str(host.data[0]) + ' X ' + guest.type + str(guest.data[0]))

            if host.data[parameters.JOIN_CRITERION_INDEX] == guest.data[parameters.JOIN_CRITERION_INDEX]:
                join_result.data.append(host.data + guest.data)


            i += 1
        join_result.latency = guest.latency
        join_result.latency_type = str(guest.data[0])
        asyncio.async(self.send(join_result, self.next_node[0], self.next_node[1]))

    def decrease_size(self, region):
        """
        remove number of packets and decrease size by 1
        """
        if parameters.join_mode == parameters.MODE_COUNT:
            if region == 0:
                if len(self.R_Storage):
                    self.R_Storage.pop(0)
                self.subwindow_size_r -= 1
            elif region == 1:
                if len(self.S_Storage):
                    self.S_Storage.pop(0)
                self.subwindow_size_s -= 1

    def increase_size(self, change, region):
        """
        Increase subwindow size by change
        :param change: int
        """
        if parameters.join_mode == parameters.MODE_COUNT:
            if change > 0:
                if region == 0:
                    self.subwindow_size_r += change
                elif region == 1:
                    self.subwindow_size_s += change

    def do(self, packet):
        """

        :param packet:
        """
        # print(packet.sender["name"] + ' >| ' + packet.type + ' |> ' + self.name)
        # print("SIZE:" + str(self.S_Storage.subwindow_size) + ' ' + str(self.R_Storage.subwindow_size))

        if packet.type == parameters.DATATYPE_R_STREAM or packet.type == parameters.DATATYPE_S_STREAM:  # r packet
            print(packet.type + packet.data[0] + str(packet.saver))
            if packet.saver == (self.host, self.port):
                self.store(packet)  # store if saver matches
            self.process_joining(packet)

        elif packet.type == parameters.DATATYPE_DELETE_R:  # delete packet for R region
            print(packet.type)
            self.decrease_size(0)

        elif packet.type == parameters.DATATYPE_DELETE_S:  # delete packet for S region
            print(packet.type)
            self.decrease_size(1)

        elif packet.type == parameters.DATATYPE_INCREASE_SUBWINDOW_SIZE_R:
            change = int(packet.data[0])
            print(packet.type + ' ' + change)
            self.increase_size(change, 0)

        elif packet.type == parameters.DATATYPE_INCREASE_SUBWINDOW_SIZE_S:
            change = int(packet.data[0])
            print(packet.type + ' ' + change)
            self.increase_size(change, 1)

        elif packet.type == parameters.DATATYPE_SET_SUBWINDOW_SIZE:
            size = packet.data[0]
            print(packet.type + ' ' + str(size))
            self.subwindow_size_r = self.subwindow_size_s = size

if __name__ == '__main__':
    count = len(argv)
    if count == 4:
        p = Processor(str(argv[1]), str(argv[2]), int(argv[3]))
    elif count == 10:
        p = Processor(str(argv[1]), str(argv[2]), int(argv[3]), int(argv[4]), int(argv[5]), int(argv[6]), int(argv[7]), int(argv[8]), int(argv[9]))
    else:
        exit()
    p.register_membership((parameters.SOURCE_HOST, parameters.SOURCE_PORT))
    p.next_node = (parameters.MERGER_HOST, parameters.MERGER_PORT)
    p.run_server()

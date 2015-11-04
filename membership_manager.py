import asyncio

__author__ = 'Zakaria'

import utils
import parameters

#active node row structure. column indexes below
COL_NODE = 0
COL_TIME = 1
COL_USED = 2
COL_COST = 3


class MembershipManager:
    active_nodes = []
    is_membership_protocol_running = False
    loop = None

    def __init__(self, loop):
        """

        :param loop:
        """
        self.loop = loop

    def add_node_to_list(self, reporting_node, cost_value):
        """
        adds node to list if it is a new node. else time is updated of
        matching node
        :param reporting_node:
        """
        for row in self.active_nodes:
            if row:
                if reporting_node == row[COL_NODE]:
                    row[COL_TIME] = utils.get_second()
                    return

        self.active_nodes.append([reporting_node, utils.get_second(), False, cost_value])
        self.active_nodes.sort(key=lambda x: x[COL_COST], reverse=True)     # sorting active nodes by descending cost
        print("After adding, available nodes:\n\t" + ("\n\t".join(map(str, self.active_nodes))))

    @asyncio.coroutine
    def check_active_nodes(self, interval):
        """
        Checks inactive node after interval
        :param interval:
        """
        current_time = utils.get_second()
        for row in self.active_nodes:
            if row[COL_TIME] + interval < current_time:     # inactive node remove
                self.active_nodes.remove(row)
                print("After removing inactive node, Active nodes:\n\t" + ("\n\t".join(map(str, self.active_nodes))))
        yield from asyncio.sleep(interval)
        yield from self.check_active_nodes(interval)

    def handle_heartbeat(self, packet):
        """

        :param packet:
        """
        self.add_node_to_list((packet.sender['host'], packet.sender['port']), packet.data[0])
        if self.is_membership_protocol_running is False:
            self.is_membership_protocol_running = True
            asyncio.async(self.check_active_nodes(parameters.CHECK_INTERVAL))

    def get_nodes(self, n):
        """

        :param n:
        :return:
        """
        nodelist = []

        for row in self.active_nodes:
            if n > 0 and row[COL_USED] is False:
                row[COL_USED] = True
                # send node initialized with default subwindow and change = 0
                nodelist.append([row[COL_NODE], parameters.SUBWINDOW_DEFAULT_SIZE, 0])
                n -= 1

        if n <= 0:
            return nodelist

        return None
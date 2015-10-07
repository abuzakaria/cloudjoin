__author__ = 'Zakaria'

from collections import deque
from packet import Packet
import constants


class Region():
    def __init__(self, store_type):
        """

        :param store_type:
        :param subwindow_size:
        """
        self.subwindow_size = 0
        self.store_type = store_type
        self.queue = deque()      #readonly maxlen is available if needed.

    def store(self, packet):
        """

        :param packet:
        :return:
        """
        print("Subw: " + str(self.subwindow_size))
        if packet.type != self.store_type:
            return False

        if len(self.queue) < self.subwindow_size:   # if space free in queue, append
            self.queue.append(packet)
        elif len(self.queue) == self.subwindow_size:  # if queue full, pop then add
            self.queue.popleft()
            self.queue.append(packet)
        else:
            # if queue size greater than subwindow size, just pop. error case.
            # should not be here. rather delete packet and decrease size
            self.decrease_size()
            return False

        return True

    def decrease_size(self):
        """
        remove number of packets and decrease size by 1
        """
        if self.queue:
            self.queue.popleft()
            self.subwindow_size -= 1
        print("After decrease size: " + str(len(self.queue)) + " " + str(self.subwindow_size))

    def increase_size(self, change):
        """
        increase subwindow size
        :param change: int
        """
        if change > 0:
            self.subwindow_size -= change
        print("After increase size, (q,size): " + str(len(self.queue)) + " " + str(self.subwindow_size))

    def set_size(self, size):
        """
        set subwindow size if 0
        :param size:
        """
        if self.subwindow_size == 0:
            self.subwindow_size = size

    def process(self, guest):
        """

        :param guest:
        :return:
        """
        join_result = Packet(constants.DATATYPE_JOIN)
        is_empty = True
        if guest.type == self.store_type:
            return None

        for host in self.queue:
            print(host.type + str(host.data[0]) + ' X ' + guest.type + str(guest.data[0]))

            if host.data[0] == guest.data[0]:   # join matching by id, assumption id is in data[0]
                join_result.data.append(host.data + guest.data)
                is_empty = False

            # join_result.data.append(host.data + guest.data)
            # is_empty = False

        if is_empty is False:
            join_result.data.append(constants.PUNCTUATION)
        else:
            print("---------------------")

        return join_result
__author__ = 'Zakaria'

from collections import deque
from packet import Packet
import constants


class Region():
    def __init__(self, store_type, window_size=constants.WINDOW_SIZE):
        self.window_size = window_size
        self.store_type = store_type
        self.queue = deque()      #readonly maxlen is available if needed.

    def store(self, packet):
        if packet.type != self.store_type:
            return False
        if len(self.queue) >= self.window_size:
            self.queue.popleft()
            self.queue.append(packet)
        else:
            self.queue.append(packet)
        return True

    def decrease_size(self, n=1):
        """
        remove number of packets
        :param n: number of packet
        """
        while n > 0:
            self.queue.popleft()
            n -= 1

    def process(self, guest):
        join_result = Packet(constants.DATATYPE_JOIN)
        is_empty = True
        if guest.type == self.store_type:
            return None

        for host in self.queue:
            print(host.type + str(host.data[0]) + ' X ' + guest.type + str(guest.data[0]))

            if host.data[0] == guest.data[0]:   # join matching by id, assumption id is in data[0]
                join_result.data.append(host.data + guest.data)
                is_empty = False
        if is_empty is False:
            join_result.data.append(constants.PUNCTUATION)
        else:
            print("---------------------")

        return join_result

    def change_window_size(self, n):
        self.window_size = n

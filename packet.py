__author__ = 'Zakaria'


class Packet(object):
    def __init__(self, packet_type):
        self.type = packet_type
        self.sender = ''
        self.data = []

    def set_type(self, packet_type):
        self.type = packet_type

    def append_data(self, data):
        self.data.append(data)

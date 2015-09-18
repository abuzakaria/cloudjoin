__author__ = 'Zakaria'


class Packet(object):
    def __init__(self, packet_type):
        """

        :param packet_type:
        """
        self.type = packet_type
        self.sender = ''
        self.saver = ''
        self.data = []

    def set_type(self, packet_type):
        """

        :param packet_type:
        """
        self.type = packet_type

    def append_data(self, data):
        """

        :param data:
        """
        self.data.append(data)

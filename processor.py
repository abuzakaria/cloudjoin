__author__ = 'Zakaria'

from node import Node
from region import Region
import asyncio
import constants


#cores or joining nodes.
class Processor(Node):
    next_node = None

    def __init__(self, name=None, host=None, port=None, window_size=constants.WINDOW_SIZE):
        """

        :param name:
        :param host:
        :param port:
        :param window_size:
        """
        if host:
            self.host = host
        if port:
            self.port = port
        if name:
            self.name = name
        self.LR = Region(constants.DATATYPE_S_STREAM, window_size)
        self.RR = Region(constants.DATATYPE_R_STREAM, window_size)
        self.loop = asyncio.get_event_loop()

    def set_window_size(self, n):
        """

        :param n:
        """
        self.LR.change_window_size(n)
        self.RR.change_window_size(n)

    def do(self, packet):
        """

        :param packet:
        """
        # print(str(packet.sender) + ' >| ' + packet.type + str(packet.data[0]) + ' |> ' + str(self.port))

        join_result = None
        if packet.type == constants.DATATYPE_R_STREAM:
            if packet.store is True:
                self.RR.store(packet)      # store r
            join_result = self.LR.process(packet)

        elif packet.type == constants.DATATYPE_S_STREAM:
            if packet.store is True:
                self.LR.store(packet)      # store s
            join_result = self.RR.process(packet)

        if len(join_result.data) > 0:
            self.send(join_result, self.next_node[0], self.next_node[1])

    # def send(self, payload, receiver_host, receiver_port):
    #     payload.sender = self.name
    #     loop = asyncio.get_event_loop()
    #     asyncio.async(self.tcp_client(payload, receiver_host, receiver_port, loop))

if __name__ == '__main__':
    p = Processor('12346', '127.0.0.1', 12346)
    p.register_membership(('127.0.0.1', 12344))
    p.next_node = ('127.0.0.1', 12350)
    # p.add_neighbour(('127.0.0.1', 12350))
    p.run_server()


#     hst = '127.0.0.1'
#     for prt in range(12345, 12349):
#         processor = Processor(hst, prt)
#         processor.add_neighbour((hst, 12350))
#         loop = asyncio.get_event_loop()
#         t = Thread(target=processor.run_server, args=(loop,))
#         t.start()

#
# from processor import Processor
# p = Processor('127.0.0.1', )
# p.add_neighbour(('127.0.0.1', 12350))
# p.run_server()
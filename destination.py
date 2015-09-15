__author__ = 'Zakaria'

import node
import json


#destination or sink or last node of the network
class Destination(node.Node):

    def do(self, packet):
        """

        :param packet:
        :param sender:
        """
        print(packet.type + str(len(packet.data)))
        # print(packet.type + str(len(packet.data)))
        for p in packet.data:
            print(packet.type + ' : ' + json.dumps(p))
        print("----------------------------------------")


#Test run
if __name__ == '__main__':
    dest = Destination('destination', '127.0.0.1', 12350)
    dest.run_server()

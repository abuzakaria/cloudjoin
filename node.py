__author__ = 'Zakaria'


from packet import Packet
import asyncio
import pickle
import constants


#superclass for all types of nodes
class Node:
    name = None
    host = None
    port = None
    membership_manager = None
    loop = None

    def __init__(self, name=None, host=None, port=None):
        """
        Initializes node with name host and port
        :param name: string to identify a node
        :param host: host of node. i.e.: 127.0.0.1
        :param port: port of node
        """
        if name:
            self.name = name
        if host:
            self.host = host
        if port:
            self.port = port
        self.loop = asyncio.get_event_loop()

    def register_membership(self, manager):
        """

        :param manager:
        """
        print("registering to membership membership_manager")
        self.membership_manager = manager
        self.send_heartbeat(constants.HEARTBEAT_INTERVAL)

    def send_heartbeat(self, interval):
        """
        Sends a heartbeat to manager after a defined interval
        :param interval: gap between heartbeats
        """
        p = Packet('H')
        self.send(p, self.membership_manager[0], self.membership_manager[1])
        self.loop.call_later(interval, self.send_heartbeat, interval)

    def send(self, payload, receiver_host, receiver_port):
        """
        asyncio wrapper to send packet
        :param payload: packet or data to send
        :param receiver_host: host of receiver
        :param receiver_port: port of receiver
        """
        payload.sender = {'host': self.host, 'port': self.port, 'name': self.name}
        asyncio.async(self.tcp_client(payload, receiver_host, receiver_port))


    @asyncio.coroutine
    def tcp_client(self, message, receiver_host, receiver_port):
        """
        Lower level function to send message to receiver
        :param message: message to send
        :param receiver_host: host of receiver
        :param receiver_port: port of receiver
        """
        # print(self.name + ' >| ' + message.type + str(message.data[0]) + ' |> ' + str(receiver_port))
        try:
            reader, writer = yield from asyncio.open_connection(receiver_host, receiver_port, loop=self.loop)
            writer.write(pickle.dumps(message))
            yield from writer.drain()

            print('----------------')
            writer.close()
        except:
            print("ERROR: Sending Failed")

    def run_server(self):
        """
        Wrapper to run server that receives packets
        """
        job = asyncio.start_server(self.handle_packet, self.host, self.port, loop=self.loop)
        server = self.loop.run_until_complete(job)

        print('MSG: Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        self.loop.close()

    @asyncio.coroutine
    def handle_packet(self, reader, writer):
        """
        Low level function to receive packet and unpickle and forward
        to a do function for futher processing. Sends ack if necessary
        :param reader: asyncio reader
        :param writer: asyncio writer
        """
        data = yield from reader.read()
        packet = pickle.loads(data)
        # sender = writer.get_extra_info('peername')
        if packet:
            self.do(packet)

        # print("Send: %r" % message)
        # writer.write(data)
        # yield from writer.drain()

        # print("MSG: Close receiving socket")
        writer.close()

    def do(self, packet):
        """
        Override this method in child types. This method will
        handle packet and do stuff
        :param packet: packet data
        """
        print("Override this method")
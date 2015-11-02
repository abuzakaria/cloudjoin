__author__ = 'Zakaria'


from packet import Packet
import asyncio
import pickle
import parameters


#superclass for all types of nodes
class Node:
    name = None     # used only for printing purpose, no logic
    host = None
    port = None
    membership_manager = None
    loop = None
    network_buffer = None
    clients = {}

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
        self.network_buffer = asyncio.Queue()

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
        print(self.name + ' >| ' + message.type + ' |> ' + str(receiver_port))
        try:
            reader, writer = yield from asyncio.open_connection(receiver_host, receiver_port, loop=self.loop)
            writer.write(pickle.dumps(message))
            yield from writer.drain()

            writer.close()
        except Exception as e:
            print(e)

    def run_server(self):
        """
        Wrapper to run server that receives packets
        """
        job = asyncio.start_server(self.handle_packet, self.host, self.port, loop=self.loop, reuse_address=True)
        server = self.loop.run_until_complete(job)
        asyncio.async(self.watch_buffer())

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
        print(data)
        yield from self.network_buffer.put(data)

    @asyncio.coroutine
    def watch_buffer(self):
        while True:
            row = yield from self.network_buffer.get()
            self.do(pickle.loads(row))

    def do(self, packet):
        """
        Override this method in child types. This method will
        handle packet and do stuff
        :param packet: packet data
        """
        raise NotImplementedError
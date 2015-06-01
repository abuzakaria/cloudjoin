__author__ = 'Zakaria'
import node
import asyncio


class Source(node.Node):
    neighbours = set()

    def add_neighbour(self, neighbour):
        self.neighbours.add(neighbour)

    def remove_neighbour(self, neighbour):
        self.neighbours.remove(neighbour)

    def count_neighbours(self):
        return len(self.neighbours)

    def send(self, payload, receiver_host, receiver_port):
        payload = "Hello world"
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tcp_echo_client(payload, receiver_host, receiver_port, loop))
        loop.close()

    @asyncio.coroutine
    def tcp_echo_client(self, message, receiver_host, receiver_port, loop):
        reader, writer = yield from asyncio.open_connection(receiver_host, receiver_port, loop=loop)

        print('Send: %r' % message)
        writer.write(message.encode())

        data = yield from reader.read(self.window_size)
        print('Received: %r' % data.decode())

        print('Close the socket')
        writer.close()
__author__ = 'Zakaria'
import node
import asyncio


class Destination(node.Node):

    def run_server(self):
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self.handle_echo, self.host, self.port, loop=loop)
        server = loop.run_until_complete(coro)

        print('Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()

    @asyncio.coroutine
    def handle_echo(self, reader, writer):
        data = yield from reader.read(self.window_size)
        message = data.decode()
        sender = writer.get_extra_info('peername')
        print("Received %r from %r" % (message, sender))

        print("Send: %r" % message)
        writer.write(data)
        yield from writer.drain()

        print("Close the client socket")
        writer.close()
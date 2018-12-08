import asyncio
import signal
from multiprocessing import Process
from functools import partial
from .reaper import Reaper
from ..hooks import Events
from ..utils import asynclib


class RequestHandler(Process):

    def __init__(self, app, sock_path=None):
        super().__init__()
        self.app = app
        self.daemon = True
        self.socket_path = sock_path

    def run(self):
        # Creating a new event loop using a faster loop.
        loop = asynclib.new_event_loop()
        loop.app = self.app
        self.app.loop = loop
        self.app.components.add(loop)
        asyncio.set_event_loop(loop)

        # Starting the connection reaper.
        self.app.reaper = Reaper(app=self.app)
        self.app.reaper.start()

        # Registering routes, blueprints, handlers, callbacks, everything is delayed until now.
        self.app.initialize()

        # Calling before server start hooks (sync/async)
        loop.run_until_complete(self.app.call_hooks(Events.BEFORE_SERVER_START, components=self.app.components))

        # Creating the server.
        handler = partial(self.app.handler, app=self.app, loop=loop, worker=self)
        ss = loop.create_unix_server(handler, path=self.socket_path)

        # Calling after server hooks (sync/async)
        loop.run_until_complete(ss)
        loop.run_until_complete(self.app.call_hooks(Events.AFTER_SERVER_START, components=self.app.components))

        async def stop_server(timeout=30):

            # Stop the reaper.
            self.app.reaper.has_to_work = False

            # Calling the before server stop hook.
            await self.app.call_hooks(Events.BEFORE_SERVER_STOP, components=self.app.components)

            # Ask all connections to finish as soon as possible.
            for connection in self.app.connections.copy():
                connection.stop()

            # Waiting all connections finish their tasks, after the given timeout
            # the connection will be closed abruptly.
            while timeout:
                all_closed = True
                for connection in self.app.connections:
                    if not connection.is_closed():
                        all_closed = False
                        break
                if all_closed:
                    break
                timeout -= 1
                await asyncio.sleep(1)

            loop.stop()

        def handle_kill_signal():
            self.socket.close()  # Stop receiving new connections.
            loop.create_task(stop_server(10))

        try:
            loop.add_signal_handler(signal.SIGTERM, handle_kill_signal)
            loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            loop.stop()

import pickle
import socket
import struct
import threading

import eventlet
from eventlet.greenio import GreenSocket
from ryu.controller.event import EventBase
from ryu.lib import hub

assert hasattr(socket, "MSG_WAITALL"), "socket.MSG_WAITALL is missing on this platform"


def _dark_green_pipe():
    s1, s2 = socket.socketpair()
    s1.setblocking(True)
    s2.setblocking(True)
    return s1, GreenSocket(s2)


class RyuAppEventLoopScheduler:
    """Ryu app uses eventlet (abstracted away as ryu.lib.hub) for
    event processing, spawning separate event loops per ryu application
    which process events from an eventlet's Queue. The Queue is not
    thread-safe. This class creates a piped connection between threads
    to provide a thread-safe way to enqueue an event to the Queue.
    The instance of this class must be created in the same thread as
    the Ryu application (i.e. the main thread).
    """

    def __init__(self, ryu_app):
        self._ryu_app = ryu_app
        self._lock = threading.Lock()
        self._green_thread = None
        self._dark, self._green = None, None

    def __enter__(self):
        assert hub.HUB_TYPE == "eventlet"
        with self._lock:
            self._dark, self._green = _dark_green_pipe()
        self._green_thread = eventlet.spawn(self._eventlet_loop)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            self._dark.close()
        self._green.close()
        self._green_thread.wait()

    def _eventlet_loop(self):
        while True:
            size = self._green.recv(4, socket.MSG_WAITALL)  # size of "!i" is 4 bytes
            if not size:
                break
            size, = struct.unpack("!i", size)
            buf = self._green.recv(size, socket.MSG_WAITALL)
            if not buf:
                break
            ev = pickle.loads(buf)
            self._ryu_app.send_event_to_observers(ev)

    def send_event_to_observers(self, ev):
        assert isinstance(ev, EventBase)
        data = pickle.dumps(ev)
        header = struct.pack("!i", len(data))
        with self._lock:
            self._dark.sendall(header + data)
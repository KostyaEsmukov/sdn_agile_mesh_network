import threading
import unittest

import eventlet
from ryu.controller.event import EventBase
from ryu.lib import hub

from agile_mesh_network.ryu.events_scheduler import RyuAppEventLoopScheduler


class RyuAppEventLoopSchedulerTestCase(unittest.TestCase):
    def test(self):
        sync = threading.Event()
        ryu_app = DummyRyuApp()
        ryu_app.start()
        try:
            with RyuAppEventLoopScheduler(ryu_app) as s:

                def run():
                    for i in range(5):
                        s.send_event_to_observers(DummyEvent(i))
                    sync.set()

                threading.Thread(target=run).start()

                eventlet.sleep(0)
                sync.wait()
                eventlet.sleep(0)
        finally:
            ryu_app.stop()
        self.assertListEqual(list(range(5)), [ev.i for ev in ryu_app.processed_events])


class DummyEvent(EventBase):
    def __init__(self, i):
        self.i = i


class DummyRyuApp:
    _event_stop = object()

    def __init__(self):
        self.events = hub.Queue(128)
        self.is_active = True
        self.threads = []
        self.processed_events = []

    def start(self):
        self.threads.append(hub.spawn(self._event_loop))

    def stop(self):
        self.is_active = False
        self.send_event_to_observers(self._event_stop)
        hub.joinall(self.threads)

    def send_event_to_observers(self, ev):
        self.events.put((ev, None))

    def _event_loop(self):
        while self.is_active or not self.events.empty():
            ev, state = self.events.get()
            if ev is self._event_stop:
                continue
            self.processed_events.append(ev)

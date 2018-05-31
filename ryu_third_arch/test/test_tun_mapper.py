import unittest
from agile_mesh_network.common.tun_mapper import mac_to_tun_name


class TunMapperTestCase(unittest.TestCase):

    def test_mapper(self):
        self.assertEqual("tapangmzseeic01", mac_to_tun_name("02:11:22:33:33:01"))
        self.assertEqual("tapangmzseeicfa", mac_to_tun_name("02:11:22:33:33:fa"))
        self.assertEqual("tapangmaceeia00", mac_to_tun_name("00:11:22:00:33:00"))
        with self.assertRaises(AssertionError):
            mac_to_tun_name("02:11:22:33:33:QQ")

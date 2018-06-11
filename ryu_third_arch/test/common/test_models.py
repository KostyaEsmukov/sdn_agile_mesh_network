import json
from unittest import TestCase

from agile_mesh_network.common.models import (
    LayersDescriptionModel, LayersDescriptionRpcModel, NegotiationIntentionModel,
    TunnelModel
)

TUNNEL_DATA = {
    "src_mac": "00:11:22:33:44:00",
    "dst_mac": "00:11:22:33:44:01",
    "layers": ["openvpn"],
    "is_dead": False,
    "is_tunnel_active": True,
}

LAYERS_DESCRIPTION_DATA = {
    "protocol": "tcp",
    "dest": ["192.168.9.2", 1194],
    "layers": {"openvpn": {"lz": False}},
}

NEGOTIATION_INTENTION_DATA = {
    "src_mac": "00:11:22:33:44:00",
    "dst_mac": "00:11:22:33:44:01",
    "layers": {"openvpn": {"lz": False}},
}


class ModelsTestCases(TestCase):
    def test_tunnel(self):
        mod = TunnelModel(**TUNNEL_DATA)

        self.assertEqual(mod.src_mac, TUNNEL_DATA["src_mac"])
        with self.assertRaises(TypeError):
            mod["src_mac"]

        data = mod.asdict()
        self.assertDictEqual(data, TUNNEL_DATA)
        json.dumps(data)  # must be serializable

    def test_layers(self):
        default_data = LAYERS_DESCRIPTION_DATA.copy()
        default_data.pop("dest")

        mod = LayersDescriptionModel(**default_data)

        self.assertEqual(mod.protocol, default_data["protocol"])
        with self.assertRaises(TypeError):
            mod["protocol"]

        data = mod.asdict()
        self.assertDictEqual(data, default_data)
        json.dumps(data)  # must be serializable

    def test_layers_rpc(self):
        mod = LayersDescriptionRpcModel(**LAYERS_DESCRIPTION_DATA)

        self.assertEqual(mod.protocol, LAYERS_DESCRIPTION_DATA["protocol"])
        with self.assertRaises(TypeError):
            mod["protocol"]

        data = mod.asdict()
        self.assertDictEqual(data, LAYERS_DESCRIPTION_DATA)
        json.dumps(data)  # must be serializable

    def test_negotiation(self):
        mod = NegotiationIntentionModel(**NEGOTIATION_INTENTION_DATA)
        nonce = mod.nonce
        self.assertEqual(16, len(nonce))

        self.assertEqual(mod.src_mac, NEGOTIATION_INTENTION_DATA["src_mac"])
        with self.assertRaises(TypeError):
            mod["src_mac"]

        data = mod.asdict()
        self.assertDictEqual(data, {**NEGOTIATION_INTENTION_DATA, "nonce": nonce})
        json.dumps(data)  # must be serializable

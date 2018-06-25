LOCAL_MAC = "00:11:22:33:00:00"
SECOND_MAC = "00:11:22:33:44:01"
THIRD_MAC = "00:11:22:33:44:02"
UNK_MAC = "98:99:99:88:88:88"
SWITCH_ENTITY_RELAY_DATA = {
    "hostname": "relay1",
    "is_relay": True,
    "mac": LOCAL_MAC,
    "layers_config": {
        "negotiator": {
          "tcp": ["192.0.2.0", 65000],
        },
        "layers": {"openvpn": {}},
    },
}
SWITCH_ENTITY_BOARD_DATA = {
    "hostname": "board1",
    "is_relay": False,
    "mac": SECOND_MAC,
    "layers_config": {
        "negotiator": {
          "tcp": ["192.0.2.1", 65000],
        },
        "layers": {"openvpn": {}},
    },
}
TOPOLOGY_DATABASE_DATA = [SWITCH_ENTITY_RELAY_DATA, SWITCH_ENTITY_BOARD_DATA]

TUNNEL_MODEL_RELAY_DATA = {
    "src_mac": LOCAL_MAC,
    "dst_mac": SWITCH_ENTITY_RELAY_DATA["mac"],
    "is_dead": False,
    "is_tunnel_active": True,
    "layers": ["openvpn"],
}

TUNNEL_MODEL_BOARD_DATA = {
    **TUNNEL_MODEL_RELAY_DATA,
    "dst_mac": SWITCH_ENTITY_BOARD_DATA["mac"],
}

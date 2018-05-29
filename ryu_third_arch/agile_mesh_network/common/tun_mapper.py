import re

_mac_pattern = re.compile(r"^(?:[0-9a-f]{2}:){5}[0-9a-f]{2}$")
_key = "amn"


def mac_to_tun_name(mac, tun_type="tap"):
    assert tun_type in ("tap", "tun")
    assert _mac_pattern.match(mac)
    return f"{tun_type}{_key}{mac.replace(':', '')}"

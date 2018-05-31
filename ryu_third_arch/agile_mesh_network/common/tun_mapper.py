import base64
import re

_mac_pattern = re.compile(r"^(?:[0-9a-f]{2}:){5}[0-9a-f]{2}$")
_key = "an"

# Max network interface name length, including the trailing zero byte.
# https://github.com/torvalds/linux/blob/ead751507de86d90fa250431e9990a8b881f713c/include/uapi/linux/if.h#L33
IFNAMSIZ = 16


def compact_hex_number(hex_num: str):
    i = int(hex_num[:-2], 16)
    # base32 shrinks 10 hex digits string (5 bytes) to 8 [a-z0-9] chars.
    prefix = base64.b32encode(i.to_bytes(5, "little")).decode().strip("=")
    # Leave last 2 MAC digits as-is, for better readability.
    return prefix + hex_num[-2:]


def mac_to_tun_name(mac, tun_type="tap"):
    assert tun_type in ("tap", "tun")
    assert _mac_pattern.match(mac)
    name = f"{tun_type}{_key}{compact_hex_number(mac.replace(':', ''))}".lower()
    assert len(name) <= IFNAMSIZ - 1, f"{name} is longer than {IFNAMSIZ - 1} bytes."
    return name

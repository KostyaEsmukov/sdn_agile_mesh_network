from typing import NewType

# L2 Hardware address (mac address). Example: '02:03:04:01:02:03'
MACAddress = NewType("MACAddress", str)

# /dev/net/tun device name. These devices (also called `ports` in
# Open vSwitch) are added to the bridge. Example: 'tun1'.
TUNPortName = NewType("TUNPortName", str)

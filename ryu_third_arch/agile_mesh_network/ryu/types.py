"""Type definitions."""

# L2 Hardware address (mac address). Example: '02:03:04:01:02:03'
MAC_ADDRESS = str

# OpenFlow port. Each port in Open vSwitch bridge has a numeric index,
# which is called OFPORT, and is used when working with OpenFlow flows.
# Can be found out with a command:
# `ovs-vsctl list Interface'
#
# There are some special values, see ryu.ofproto.ofproto_v1_4.OFPP_*
OFPORT = int

# /dev/net/tun device name. These devices (also called `ports` in
# Open vSwitch) are added to the bridge. Example: `tun1`.
TUN_NAME = str

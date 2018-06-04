from typing import NewType

# OpenFlow port. Each port in Open vSwitch bridge has a numeric index,
# which is called OFPort, and is used when working with OpenFlow flows.
# Can be found out with a command:
# `ovs-vsctl list Interface'
#
# Value less than zero should be treated differently: it means
# a non-existing port.
#
# There are some special values, see ryu.ofproto.ofproto_v1_4.OFPP_*
OFPort = NewType("OFPort", int)

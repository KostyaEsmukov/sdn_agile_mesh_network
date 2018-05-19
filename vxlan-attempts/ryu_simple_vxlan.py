"""


ovs-vsctl add-br brvxm

ifconfig brvxm 10.10.0.1/24
ifconfig brvxm 10.10.0.2/24

ovs-vsctl set bridge brvxm other-config:datapath-id=0000000000000008
ovs-vsctl set bridge brvxm other-config:datapath-id=0000000000000009

ovs-vsctl set-controller brvxm tcp:192.168.56.101:6653

# Disable in-band flows for ARP to let them reach the Controller
ovs-vsctl set bridge brvxm other-config:disable-in-band=true

ovs-vsctl set-manager ptcp:6640
"""


from ryu.app import simple_switch_13
from ryu.app.ofctl import api as ofctl_api
from ryu.app.rest_vtep import OVSDB_PORT, OFPortNotFound, to_int, DatapathNotFound, \
    TABLE_ID_INGRESS, PRIORITY_D_PLANE, TABLE_ID_EGRESS
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib.ovs import bridge as ovs_bridge
from ryu.lib.packet import packet, ethernet, ether_types, arp


VXLAN_VNI = 11
LOCAL_IP_TO_EXTERNAL = {
    "10.10.0.1": "192.168.56.102",
    "10.10.0.2": "192.168.56.103",
}

# todo get rid of these
EXT_IP_TO_LOCAL_BR_MAC = {
    "192.168.56.102": "e6:e8:10:a5:a7:48",
    "192.168.56.103": "ae:06:bd:96:ce:49",
}
EXT_IP_TO_DATAPATH_ID = {
    "192.168.56.102": 8,
    "192.168.56.103": 9,
}

class LearningVxlan(simple_switch_13.SimpleSwitch13):

    def __init__(self, *args, **kwargs):
        super(LearningVxlan, self).__init__(*args, **kwargs)
        self.mac_to_ip = {}
        self.ovs = None  # OVSBridge instance instantiated later

    def _add_vxlan(self, ip1, ip2):

        for ovsdb_ip, remote_ip in [(ip1, ip2), (ip2, ip1)]:
            datapath_id = EXT_IP_TO_DATAPATH_ID[ovsdb_ip]
            vxlan_port = self._add_vxlan_port(ovsdb_ip, datapath_id, remote_ip, VXLAN_VNI)

            datapath = self._get_datapath(datapath_id)
            if datapath is None:
                raise DatapathNotFound(dpid=datapath_id)

            port = self._get_ofport(ovsdb_ip, datapath_id, 'brvxm')
            if port is None:
                try:
                    port = to_int(port)
                except ValueError:
                    raise OFPortNotFound(port_name=port)

            # todo fetch macs via ovsdb/of

            # self._add_network_ingress_flow(
            #     datapath=datapath,
            #     tag=VXLAN_VNI,
            #     in_port=port,
            #     eth_src=EXT_IP_TO_LOCAL_BR_MAC[ovsdb_ip])
            #
            # # self._add_l2_switching_flow(
            # #     datapath=datapath,
            # #     tag=VXLAN_VNI,
            # #     eth_dst=EXT_IP_TO_LOCAL_BR_MAC[ovsdb_ip],
            # #     out_port=port)
            #
            # self._add_network_ingress_flow(
            #     datapath=datapath,
            #     tag=VXLAN_VNI,
            #     in_port=vxlan_port)

        # todo wait until the tunnel is created????

    def _packet_arp(self, arp_packet, src):
        if arp_packet.opcode in (arp.ARP_REQUEST, arp.ARP_REPLY):
            if src in self.mac_to_ip and self.mac_to_ip[src] != arp_packet.src_ip:
                self.logger.error("Possible mac address collision: overwriting "
                                  "%s with %s for %s", self.mac_to_ip[src],
                                  arp_packet.src_ip, src)
            self.mac_to_ip[src] = arp_packet.src_ip

        if arp_packet.opcode == arp.ARP_REQUEST:
            # arp(dst_ip='10.10.0.2',dst_mac='00:00:00:00:00:00',hlen=6,hwtype=1,
            # opcode=1,plen=4,proto=2048,src_ip='10.10.0.1',src_mac='e6:e8:10:a5:a7:48')
            self.logger.info("ARP Req! %s -> %s", arp_packet.src_ip, arp_packet.dst_ip)
            ext_src = LOCAL_IP_TO_EXTERNAL.get(arp_packet.src_ip)
            ext_dst = LOCAL_IP_TO_EXTERNAL.get(arp_packet.dst_ip)
            if ext_src and ext_dst:
                self.logger.info("Install vxlan tunnels after ARP! %s <-> %s", ext_src, ext_dst)
                self._add_vxlan(ext_src, ext_dst)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return
        dst = eth.dst
        src = eth.src

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        self.logger.info("packet in %s %s %s %s %r", dpid, src, dst, in_port, list(pkt))

        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            arp_packet = pkt.get_protocols(arp.arp)[0]
            self._packet_arp(arp_packet, src)

        local_ip_src = self.mac_to_ip.get(src)
        local_ip_dst = self.mac_to_ip.get(dst)
        if local_ip_src and local_ip_dst:
            ext_src = LOCAL_IP_TO_EXTERNAL.get(local_ip_src)
            ext_dst = LOCAL_IP_TO_EXTERNAL.get(local_ip_dst)
            if ext_src and ext_dst:
                self.logger.info("Install vxlan tunnels learned! %s <-> %s", ext_src, ext_dst)
                self._add_vxlan(ext_src, ext_dst)

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            # TODO proper learning !!!!!!!!!!!
            # out_port = self.mac_to_port[dpid][dst]
            out_port = ofproto.OFPP_FLOOD
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            # verify if we have a valid buffer_id, if yes avoid to send both
            # flow_mod & packet_out
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions)
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        self.logger.info("OUTPACKET, %r" % dict(datapath=datapath, buffer_id=msg.buffer_id,
                                                in_port=in_port, actions=actions))
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # Code below is copied from the `rest_vtep.RestVtep` class.

    def _get_datapath(self, dpid):
        return ofctl_api.get_datapath(self, dpid)

    def _get_ovs_bridge(self, ovsdb_ip, dpid):
        datapath = self._get_datapath(dpid)
        if datapath is None:
            self.logger.debug('No such datapath: %s', dpid)
            return None

        ovsdb_addr = 'tcp:%s:%d' % (ovsdb_ip, OVSDB_PORT)
        if (self.ovs is not None
                and self.ovs.datapath_id == dpid
                and self.ovs.vsctl.remote == ovsdb_addr):
            return self.ovs

        try:
            self.ovs = ovs_bridge.OVSBridge(
                CONF=self.CONF,
                datapath_id=datapath.id,
                ovsdb_addr=ovsdb_addr)
            self.ovs.init()
        except Exception as e:
            self.logger.exception('Cannot initiate OVSDB connection: %s', e)
            return None

        return self.ovs

    def _get_ofport(self, ovsdb_ip, dpid, port_name):
        ovs = self._get_ovs_bridge(ovsdb_ip, dpid)
        if ovs is None:
            return None

        try:
            return ovs.get_ofport(port_name)
        except Exception as e:
            self.logger.debug('Cannot get port number for %s: %s',
                              port_name, e)
            return None

    def _get_vxlan_port(self, ovsdb_ip, dpid, remote_ip, key):
        # Searches VXLAN port named 'vxlan_<remote_ip>_<key>'
        return self._get_ofport(ovsdb_ip, dpid, 'vxlan_%s_%s' % (remote_ip, key))

    def _add_vxlan_port(self, ovsdb_ip, dpid, remote_ip, key):
        # If VXLAN port already exists, returns OFPort number
        vxlan_port = self._get_vxlan_port(ovsdb_ip, dpid, remote_ip, key)
        if vxlan_port is not None:
            return vxlan_port

        ovs = self._get_ovs_bridge(ovsdb_ip, dpid)
        if ovs is None:
            return None

        # Adds VXLAN port named 'vxlan_<remote_ip>_<key>'
        ovs.add_vxlan_port(
            name='vxlan_%s_%s' % (remote_ip, key),
            remote_ip=remote_ip,
            key=key)

        # Returns VXLAN port number
        return self._get_vxlan_port(ovsdb_ip, dpid, remote_ip, key)
    #
    # def _del_vxlan_port(self, dpid, remote_ip, key):
    #     ovs = self._get_ovs_bridge(dpid)
    #     if ovs is None:
    #         return None
    #
    #     # If VXLAN port does not exist, returns None
    #     vxlan_port = self._get_vxlan_port(dpid, remote_ip, key)
    #     if vxlan_port is None:
    #         return None
    #
    #     # Adds VXLAN port named 'vxlan_<remote_ip>_<key>'
    #     ovs.del_port('vxlan_%s_%s' % (remote_ip, key))
    #
    #     # Returns deleted VXLAN port number
    #     return vxlan_port

    @staticmethod
    def _add_flow(datapath, priority, match, instructions,
                  table_id=TABLE_ID_INGRESS):
        parser = datapath.ofproto_parser

        mod = parser.OFPFlowMod(
            datapath=datapath,
            table_id=table_id,
            priority=priority,
            match=match,
            instructions=instructions)

        datapath.send_msg(mod)

    def _add_network_ingress_flow(self, datapath, tag, in_port, eth_src=None):
        parser = datapath.ofproto_parser

        if eth_src is None:
            match = parser.OFPMatch(in_port=in_port)
        else:
            match = parser.OFPMatch(in_port=in_port, eth_src=eth_src)
        instructions = [
            parser.OFPInstructionWriteMetadata(
                metadata=tag, metadata_mask=parser.UINT64_MAX),
            parser.OFPInstructionGotoTable(1)]

        self._add_flow(datapath, PRIORITY_D_PLANE, match, instructions)

    def _add_l2_switching_flow(self, datapath, tag, eth_dst, out_port):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch(metadata=(tag, parser.UINT64_MAX),
                                eth_dst=eth_dst)
        actions = [parser.OFPActionOutput(out_port)]
        instructions = [
            parser.OFPInstructionActions(
                ofproto.OFPIT_APPLY_ACTIONS, actions)]

        self._add_flow(datapath, PRIORITY_D_PLANE, match, instructions,
                       table_id=TABLE_ID_EGRESS)

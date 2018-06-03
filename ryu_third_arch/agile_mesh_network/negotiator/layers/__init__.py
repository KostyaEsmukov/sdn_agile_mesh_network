from .base import ProcessManager, add_layers_managers
from .openvpn import (
    OpenvpnInitiatorProcessManager, OpenvpnResponderProcessManager, openvpn_config
)

add_layers_managers(
    {"openvpn"}, OpenvpnResponderProcessManager, OpenvpnInitiatorProcessManager
)

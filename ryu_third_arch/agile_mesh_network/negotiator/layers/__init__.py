from .base import ProcessManager, add_layers_managers
from .openvpn import (
    OpenvpnInitiatorProcessManager, OpenvpnResponderProcessManager, openvpn_config
)
from .socat import (
    SocatInitiatorProcessManager, SocatResponderProcessManager, socat_config
)

add_layers_managers(
    {"openvpn"}, OpenvpnResponderProcessManager, OpenvpnInitiatorProcessManager
)
add_layers_managers(
    {"socat"}, SocatResponderProcessManager, SocatInitiatorProcessManager
)

import os
from ast import literal_eval

REMOTE_DATABASE_MONGO_URI = os.getenv(
    "AMN_REMOTE_DATABASE_MONGO_URI", "mongodb://localhost:27017/"
)
NEGOTIATOR_RPC_UNIX_SOCK_PATH = os.getenv(
    "AMN_NEGOTIATOR_RPC_UNIX_SOCK_PATH", "/var/run/amn_negotiator.sock"
)
TOPOLOGY_DATABASE_SYNC_INTERVAL_SECONDS = int(
    os.getenv("AMN_TOPOLOGY_DATABASE_SYNC_INTERVAL_SECONDS", 10)
)


fernet_keys_strings = os.getenv(
    "AMN_LAYERS_MANAGER_BALANCER_FERNET_KEYS",
    (
        "MVIwbyoflAwxZsz_joRFCxQBjHLr47K1mSxozN2Eq1c=;"
        "VeHBFxXZU3kK9Lq2yZ9XQYlVrsNscIwL3d9g4IjIZBA=;"
        "5lZ8rjWLlaKUeslVfi0XHiVkG9-68mNMBswJJv88VLo=;"
    ),
).split(";")

LAYERS_MANAGER_BALANCER_FERNET_KEYS = [x.encode() for x in fernet_keys_strings if x]

OVS_DATAPATH_ID = int(os.getenv("AMN_OVS_DATAPATH_ID", "0000001122333301"), 16)

IS_RELAY = literal_eval(os.getenv("AMN_IS_RELAY", "False"))

FLOW_INDIRECT_MAX_LIFETIME_SECONDS = int(
    os.getenv("AMN_FLOW_INDIRECT_MAX_LIFETIME_SECONDS", 60)
)

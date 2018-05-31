

## Installation on Debian 9

This package requires Python 3.6.

## Packaging

    ./setup.py sdist bdist_wheel

    # Transfer dist/*

    . /opt/amn/venv/bin/activate
    pip install --upgrade <...>.tar.gz


## Setup Network Topology Database

Should be run on a single machine, where the centralized MongoDB instance should run.

    # TODO mongo auth https://docs.mongodb.com/manual/administration/security-checklist/

    apt install mongodb-server

    mongo

    use topology_database
    db.switch_collection.find()

    # Add relays and switches...
    db.switch_collection.save({
      "hostname": "ryucontroller",
      "is_relay": true,
      "mac": "02:11:22:33:33:01",
      "layers_config": {
        "dest": ["192.168.56.10", 11194],
        "protocol": "tcp",
        "layers": {
          "openvpn": {}
        }
      }
    })


## Openvpn certificates

Should be done once on a single machine, like a controller.

    apt install easy-rsa
    cp -aR /usr/share/easy-rsa/ /root/openvpn-easy-rsa
    cd /root/openvpn-easy-rsa
    cp openssl-1.0.0.cnf openssl.cnf
    . ./vars
    ./clean-all
    ./build-ca --batch
    ./build-dh
    ./build-key-server --batch server
    ./build-key --batch client

    tar czf ../openvpn-easy-rsa.tar.gz keys/{ca.crt,dh2048.pem,client.crt,client.key,server.crt,server.key}
    # Transfer this archive to all switches.

## Setup Switch

### Python 3.6

    sudo apt install virtualenv

    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda

    virtualenv /opt/amn/venv --python /opt/miniconda/bin/python3

    . /opt/amn/venv/bin/activate

### OVS

Openvswitch 2.6, currently shipped in Debian stable, has a [bug][ovs_bug]
which doesn't allow to use configurations in /etc/network/interfaces without
tinkering with systemd units. This is fixed in Debian testing, so we need to
install it from there.

[ovs_bug]: https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=878757

    echo "" >> /etc/apt/sources.list
    echo "deb http://httpredir.debian.org/debian testing main" >> /etc/apt/sources.list
    echo 'APT::Default-Release "stable";' | tee -a /etc/apt/apt.conf.d/00local
    apt update
    apt install -t testing openvswitch-switch

### OVS bridges

[Docs](https://github.com/openvswitch/ovs/blob/master/debian/openvswitch-switch.README.Debian).

    MAC_ADDRESS="02:11:22:33:33:01"
    IP_ADDRESS="192.168.128.1"

    cat << EOF >> /etc/network/interfaces

    auto bramn
    allow-ovs bramn
    iface bramn inet static
        ovs_type OVSBridge
        ovs_extra set bridge bramn other-config:hwaddr="${MAC_ADDRESS}"
        address ${IP_ADDRESS}
        netmask 255.255.255.0
    EOF

    ifup --allow=ovs bramn

### Openvpn

    apt install openvpn
    systemctl disable openvpn.service

    mkdir -p /etc/openvpn
    cd /etc/openvpn
    tar xaf openvpn-easy-rsa.tar.gz

    cat << EOF > /etc/openvpn/server.conf
    server-bridge
    key keys/server.key
    ca keys/ca.crt
    cert keys/server.crt
    dh keys/dh2048.pem
    keepalive 10 60
    max-clients 2
    user nobody
    group nogroup
    comp-lzo no
    duplicate-cn
    EOF

    cat << EOF > /etc/openvpn/client.conf
    client
    nobind
    remote-cert-tls server
    key keys/client.key
    ca keys/ca.crt
    cert keys/client.crt
    dh keys/dh2048.pem
    user nobody
    group nogroup
    comp-lzo no
    EOF

### Negotiator systemd service

Should be run on each Switch (+ Relays).

    cat << EOF > /etc/systemd/system/amn_negotiator.service
    [Unit]
    Description=Agile Mesh Network negotiator

    [Service]
    Type=simple
    ExecStart=/bin/bash -lc ". /opt/amn/venv/bin/activate && exec negotiator"

    [Install]
    WantedBy=multi-user.target
    EOF

    systemctl daemon-reload
    systemctl start amn_negotiator.service
    systemctl enable amn_negotiator.service


## Switch usage

    # TODO ENV
    ryu-manager --verbose /opt/amn/venv/lib/python3.6/site-packages/agile_mesh_network/ryu_app.py



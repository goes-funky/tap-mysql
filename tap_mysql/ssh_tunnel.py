from sshtunnel import SSHTunnelForwarder
from paramiko import RSAKey
import singer
import io

ssh_tunnel = None


def open_tunnel(conn_config):
    global ssh_tunnel
    if isinstance(ssh_tunnel, SSHTunnelForwarder):
        return apply_tunnel_config(conn_config)
    singer.logger.log_info("ssh tunnel enabled, establish connection")
    ssh_config = conn_config["ssh_tunnel"]
    key = ssh_config["key"]

    key_file = io.StringIO(key)

    private_key_object = RSAKey.from_private_key(key_file)

    ssh_tunnel = SSHTunnelForwarder(
            (ssh_config["host"], int(ssh_config["port"])),
            ssh_username=ssh_config["user"],
            ssh_pkey=private_key_object,
            remote_bind_address=(conn_config["host"], int(conn_config["port"]))
    )
    ssh_tunnel.start()
    singer.logger.log_info("established tunnel at local port " + str(ssh_tunnel.local_bind_port))
    conn_config = apply_tunnel_config(conn_config)
    return conn_config


def apply_tunnel_config(conn_config):
    global ssh_tunnel
    if not isinstance(ssh_tunnel, SSHTunnelForwarder):
        raise Exception("ssh_tunnel not available")

    conn_config["host"] = "127.0.0.1"
    conn_config["port"] = ssh_tunnel.local_bind_port
    return conn_config


def close():
    global ssh_tunnel
    if isinstance(ssh_tunnel, SSHTunnelForwarder):
        ssh_tunnel.close()

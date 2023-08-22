import zmq.auth


def generate_keys(key_dir: str) -> None:
    """
    Generate all public/private keys needed for this demo.
    :param key_dir: path to keys directory
    """
    zmq.auth.create_certificates(key_dir, "server")
    zmq.auth.create_certificates(key_dir, "client")

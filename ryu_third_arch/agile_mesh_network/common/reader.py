from cryptography.fernet import Fernet, MultiFernet

from agile_mesh_network import settings


class NewlineReader:
    def __init__(self):
        self.buf = b''

    def feed_data(self, data):
        self.buf += data

    def __iter__(self):
        """Yields complete lines, stripping them off the buf on the go."""
        while True:
            line_rest = self.buf.split(b'\n', 1)
            if len(line_rest) != 2:
                break
            line, self.buf = line_rest
            yield line


class EncryptedNewlineReader(NewlineReader):
    def __init__(self, fernet_keys=None):
        super().__init__()
        fernet_keys = fernet_keys or settings.LAYERS_MANAGER_BALANCER_FERNET_KEYS
        self.fernet = MultiFernet([Fernet(key) for key in fernet_keys])

    def __iter__(self):
        for token in super().__iter__():
            line = self.fernet.decrypt(token)
            assert b'\n' not in line
            yield line

    def encrypt_line(self, line):
        assert line and b'\n' not in line
        token = self.fernet.encrypt(line)
        # Fernet.decrypt returns a URL-safe string, so it's guaranteed
        # that there will be no newlines.
        assert b'\n' not in token
        return token

import unittest
from urllib.request import urlopen
from client import read_plain
from source import blocks, response_bytes, serve


class SourceTests(unittest.TestCase):
    def test_network_plain(self):
        with serve() as base:
            body, parsed = read_plain(base + '/plain')
        self.assertEqual(parsed['data']['company']['id'], 'local-1')
        self.assertIn(b'data', body)

    def test_blocks_preserve_bytes(self):
        body = response_bytes()
        self.assertEqual(b''.join(blocks(body, 1)), body)
        self.assertIn('合成'.encode(), body)

    def test_invalid_block_size(self):
        with self.assertRaises(ValueError):
            list(blocks(b'body', 0))

    def test_fault_scenarios_have_real_content(self):
        self.assertIn(b'unregistered', response_bytes('unknown'))
        self.assertIn(b'errors', response_bytes('errors'))
        self.assertGreater(len(response_bytes('retransmit')), len(response_bytes()))

    def test_network_multipart(self):
        with serve() as base:
            with urlopen(base + '/incremental') as response:
                self.assertEqual(response.headers.get_content_type(), 'multipart/mixed')
                self.assertEqual(response.read(), response_bytes())

    def test_plain_entry_rejects_multipart(self):
        with serve() as base:
            with self.assertRaises(ValueError):
                read_plain(base + '/incremental')

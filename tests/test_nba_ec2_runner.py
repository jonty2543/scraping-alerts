import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[1] / 'scripts/run_nba_props_ec2.sh'


class EC2RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.env = dict(os.environ, PATH=str(self.bin) + ':/usr/bin:/bin',
                        NBA_STATE_DIR=str(self.root / 'state'), NBA_BOOKMAKERS='Betr TAB',
                        RUNNER_CAPTURE=str(self.root / 'capture.json'),
                        XVFB_PID_FILE=str(self.root / 'xvfb.pid'))
        self.env.pop('DISPLAY', None)
        self.env.pop('XAUTHORITY', None)
        self.program('flock', '#!/bin/sh\nexit 0\n')
        self.program('xauth', '#!/bin/sh\nexit 0\n')
        self.program('python', f'#!{sys.executable}\n' + '''import json, os, sys
if len(sys.argv) > 1 and sys.argv[1] == '-c':
    exec(sys.argv[2])
else:
    with open(os.environ['RUNNER_CAPTURE'], 'w') as handle:
        json.dump({'display': os.getenv('DISPLAY'), 'auth': os.getenv('XAUTHORITY'),
                   'auth_exists': os.path.isfile(os.getenv('XAUTHORITY', '')),
                   'args': sys.argv[1:]}, handle)
    sys.exit(int(os.getenv('RUNNER_EXIT_CODE', '0')))
''')
        self.env['NBA_PYTHON'] = str(self.bin / 'python')

    def program(self, name, text):
        path = self.bin / name
        path.write_text(text)
        path.chmod(0o755)

    def run_script(self):
        return subprocess.run(['/bin/bash', str(RUNNER), '--write'], env=self.env,
                              capture_output=True, text=True, timeout=15)

    def fake_xvfb(self):
        # Model Xvfb's Unix socket and lifecycle without installing an X server.
        self.program('Xvfb', f'#!{sys.executable}\n' + '''import os, signal, socket, sys, time
directory = '/tmp/.X11-unix'
created = not os.path.isdir(directory)
os.makedirs(directory, exist_ok=True)
path = directory + '/X' + sys.argv[1].lstrip(':')
server = socket.socket(socket.AF_UNIX)
server.bind(path)
server.listen(1)
with open(os.environ['XVFB_PID_FILE'], 'w') as handle:
    handle.write(str(os.getpid()))
def stop(*args):
    server.close()
    os.unlink(path)
    if created:
        try: os.rmdir(directory)
        except OSError: pass
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
while True: time.sleep(1)
''')

    def test_reuses_existing_display_and_forwards_arguments(self):
        self.env['DISPLAY'] = ':42'
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        capture = json.loads((self.root / 'capture.json').read_text())
        self.assertEqual(capture['display'], ':42')
        self.assertIn('--write', capture['args'])
        self.assertIn('--headed-browser', capture['args'])

    def test_native_xvfb_starts_and_cleans_up_after_scraper_failure(self):
        self.fake_xvfb()
        self.env['RUNNER_EXIT_CODE'] = '7'
        result = self.run_script()
        self.assertEqual(result.returncode, 7, result.stderr)
        capture = json.loads((self.root / 'capture.json').read_text())
        self.assertTrue(capture['auth_exists'])
        self.assertFalse(Path(capture['auth']).exists())
        pid = int((self.root / 'xvfb.pid').read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_native_xvfb_startup_failure_never_runs_scraper(self):
        self.program('Xvfb', '#!/bin/sh\necho "X server dependency failure" >&2\nexit 1\n')
        result = self.run_script()
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('Xvfb failed to start', result.stderr)
        self.assertFalse((self.root / 'capture.json').exists())
        self.assertIn('dependency failure', (self.root / 'state/xvfb.log').read_text())


if __name__ == '__main__':
    unittest.main()

"""Linux-local PID/start/boot fencing; unknown identities are never reclaimed."""
import hashlib
import os
from pathlib import Path


def _host():
    return hashlib.sha256(Path('/etc/machine-id').read_bytes().strip()).hexdigest()[:16]


def _started(pid):
    # comm may contain spaces/parentheses. Fields after its final ')' start
    # at field 3; starttime is field 22, hence offset 19.
    return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]


def local_process_owner(fallback='liangjian-runtime'):
    try:
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        return f'process:{_host()}:{boot}:{os.getpid()}:{_started(os.getpid())}'
    except (OSError, IndexError, ValueError):
        return fallback


def owner_is_dead(owner):
    parts = str(owner).split(':')
    if len(parts) != 5 or parts[0] != 'process' or not parts[3].isdigit() or not parts[4].isdigit():
        return False
    try:
        if parts[1] != _host():
            return False
        if parts[2] != Path('/proc/sys/kernel/random/boot_id').read_text().strip():
            return True
        try:
            return _started(int(parts[3])) != parts[4]
        except FileNotFoundError:
            return True
    except (OSError, IndexError, ValueError):
        return False

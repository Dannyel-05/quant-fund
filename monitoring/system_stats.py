"""
System resource stats without psutil.
Uses Linux /proc filesystem and standard library only.
"""
import shutil
import time
from pathlib import Path
from typing import Tuple


def get_ram_mb() -> Tuple[int, int, float]:
    """Return (used_mb, total_mb, pct)."""
    try:
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    info[parts[0].rstrip(":")] = int(parts[1])
        total_kb = info.get("MemTotal", 2097152)
        avail_kb = info.get("MemAvailable", total_kb // 2)
        used_kb  = total_kb - avail_kb
        total_mb = total_kb // 1024
        used_mb  = used_kb  // 1024
        pct      = used_kb / total_kb * 100.0
        return used_mb, total_mb, pct
    except Exception:
        return 0, 2048, 0.0


def get_cpu_pct(sample_sec: float = 0.5) -> float:
    """
    CPU utilisation % averaged over *sample_sec* seconds.
    Uses /proc/loadavg (1-min load average / n_cpus * 100).
    For a 1-vCPU server, load_avg ≈ CPU%.
    """
    try:
        with open("/proc/loadavg") as f:
            load = float(f.read().split()[0])
        # Count logical CPUs
        cpus = 1
        try:
            with open("/proc/cpuinfo") as f:
                cpus = max(1, f.read().count("processor\t:"))
        except Exception:
            pass
        return min(100.0, load / cpus * 100.0)
    except Exception:
        return 0.0


def get_disk_gb(path: str = "/") -> Tuple[float, float, float]:
    """Return (used_gb, total_gb, pct)."""
    try:
        total, used, free = shutil.disk_usage(path)
        total_gb = total / 1e9
        used_gb  = used  / 1e9
        pct      = used / total * 100.0
        return used_gb, total_gb, pct
    except Exception:
        return 0.0, 70.0, 0.0


def is_pm2_running() -> bool:
    """Return True if a pm2 daemon process is found."""
    try:
        import subprocess
        result = subprocess.run(
            ["pgrep", "-x", "PM2"],
            capture_output=True, timeout=3,
        )
        if result.returncode == 0:
            return True
        # Fallback: check pm2 process name variations
        result2 = subprocess.run(
            ["pgrep", "-f", "pm2"],
            capture_output=True, timeout=3,
        )
        return result2.returncode == 0
    except Exception:
        return False


def get_log_last_write(log_path: str) -> float:
    """Return seconds since log file was last written to (or 999999 if missing)."""
    try:
        p = Path(log_path)
        if not p.exists():
            return 999999.0
        return time.time() - p.stat().st_mtime
    except Exception:
        return 999999.0

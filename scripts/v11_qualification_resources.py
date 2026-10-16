"""Best-effort process counters, reported as cumulative CPU seconds and RSS bytes."""

import ctypes
import os
from pathlib import Path


def process_counters(pid):
    if os.name != "nt":
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().split()
            return dict(
                pid=pid,
                cpu_seconds=(int(fields[13]) + int(fields[14])) / os.sysconf("SC_CLK_TCK"),
                rss_bytes=int(fields[23]) * os.sysconf("SC_PAGE_SIZE"),
            )
        except (OSError, ValueError, IndexError):
            return dict(pid=pid, unavailable=True)
    from ctypes import wintypes

    class Memory(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("faults", wintypes.DWORD),
            *[
                (name, ctypes.c_size_t)
                for name in (
                    "peak_rss",
                    "rss",
                    "peak_paged_pool",
                    "paged_pool",
                    "peak_nonpaged_pool",
                    "nonpaged_pool",
                    "pagefile",
                    "peak_pagefile",
                )
            ],
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE, *([ctypes.POINTER(wintypes.FILETIME)] * 4)]
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Memory), wintypes.DWORD]
    handle = kernel.OpenProcess(0x410, False, pid)
    if not handle:
        return dict(pid=pid, unavailable=True)
    try:
        counters = Memory()
        counters.cb = ctypes.sizeof(counters)
        times = [wintypes.FILETIME() for _ in range(4)]
        if not psapi.GetProcessMemoryInfo(
            handle, ctypes.byref(counters), counters.cb
        ) or not kernel.GetProcessTimes(handle, *[ctypes.byref(t) for t in times]):
            return dict(pid=pid, unavailable=True)
        cpu = sum((t.dwHighDateTime << 32) + t.dwLowDateTime for t in times[2:]) / 10000000
        return dict(pid=pid, cpu_seconds=cpu, rss_bytes=counters.rss)
    finally:
        kernel.CloseHandle(handle)

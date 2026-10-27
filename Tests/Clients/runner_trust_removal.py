import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import ssl
import subprocess
import sys
import tempfile
import time


OUTPUT_LIMIT = 64 * 1024
REPORT_LIMIT = 31 * 1024
SECOND = 1_000_000_000


class BSDInfo(ctypes.Structure):
    # macOS SDK sys/proc_info.h:60-81; MAXCOMLEN is 16 in sys/param.h.
    _fields_ = [(name, ctypes.c_uint32) for name in (
        "flags", "status", "xstatus", "pid", "ppid", "uid", "gid", "ruid", "rgid", "svuid", "svgid", "rfu"
    )] + [("comm", ctypes.c_char * 16), ("name", ctypes.c_char * 32)] + [
        (name, ctypes.c_uint32) for name in ("nfiles", "pgid", "pjobc", "tdev", "tpgid")
    ] + [("nice", ctypes.c_int32), ("start_seconds", ctypes.c_uint64), ("start_microseconds", ctypes.c_uint64)]


def now():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def process_identity(pid):
    library = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
    library.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
    library.proc_pidinfo.restype = ctypes.c_int
    library.proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
    library.proc_pidpath.restype = ctypes.c_int
    info = BSDInfo()
    if library.proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info)) != ctypes.sizeof(info):
        raise OSError(ctypes.get_errno(), "proc_pidinfo failed")
    path = ctypes.create_string_buffer(4096)
    if library.proc_pidpath(pid, path, len(path)) <= 0:
        raise OSError(ctypes.get_errno(), "proc_pidpath failed")
    return {"pid": info.pid, "ppid": info.ppid, "uid": info.uid, "status": info.status,
            "startSeconds": info.start_seconds, "startMicroseconds": info.start_microseconds,
            "executable": path.value.decode("utf-8")}


def same_process(before, after):
    keys = ("pid", "ppid", "uid", "startSeconds", "startMicroseconds", "executable")
    return all(before[key] == after[key] for key in keys)


def sample_limits():
    resource.setrlimit(resource.RLIMIT_FSIZE, (OUTPUT_LIMIT, OUTPUT_LIMIT))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


class OwnedChild:
    def __init__(self, arguments, sampler=False):
        self.process = subprocess.Popen(arguments, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, start_new_session=True,
                                        preexec_fn=sample_limits if sampler else None)
        self.reaped = False
        self.output = bytearray()
        self.output_truncated = False
        self.signals = []
        os.set_blocking(self.process.stdout.fileno(), False)

    def exited(self):
        if self.reaped:
            return True
        return os.waitid(os.P_PID, self.process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None

    def drain(self):
        for _ in range(4):
            try:
                chunk = os.read(self.process.stdout.fileno(), 4096)
            except BlockingIOError:
                break
            if not chunk:
                break
            available = 2048 - len(self.output)
            self.output.extend(chunk[:available])
            self.output_truncated |= len(chunk) > available

    def send(self, number):
        if self.reaped or number in self.signals:
            return
        if os.getpgid(self.process.pid) != self.process.pid or self.process.pid == os.getpgrp():
            raise RuntimeError("owned child process group changed")
        os.killpg(self.process.pid, number)
        self.signals.append(number)

    def reap(self):
        if self.reaped:
            return
        pid, status = os.waitpid(self.process.pid, os.WNOHANG)
        if pid:
            self.process.returncode = os.waitstatus_to_exitcode(status)
            self.reaped = True
            self.drain()

    def facts(self):
        return {"pid": self.process.pid, "reaped": self.reaped, "exitCode": self.process.returncode,
                "signals": self.signals, "output": self.output.decode("utf-8", errors="replace"),
                "outputTruncated": self.output_truncated}


def error_fact(phase, error):
    return {"phase": phase, "type": type(error).__name__, "errno": getattr(error, "errno", None)}


def trusted_identity(child, previous=None):
    identity = process_identity(child.process.pid)
    if identity["executable"] != "/usr/bin/security" or identity["uid"] != 0 or identity["ppid"] != os.getpid():
        raise RuntimeError("security child identity mismatch")
    if previous is not None and not same_process(previous, identity):
        raise RuntimeError("security child start identity changed")
    return identity


def retain_stack(path, report):
    if not path.exists():
        return
    with path.open("rb") as source:
        data = source.read(OUTPUT_LIMIT)
    report["stackFileAtLimit"] = path.stat().st_size >= OUTPUT_LIMIT
    headers = ("Process:", "Path:", "Parent Process:", "User ID:", "Date/Time:", "Launch Time:", "OS Version:")
    selected = []
    calltree = False
    for line in data.decode("utf-8", errors="replace").splitlines():
        if line.startswith("Call graph:"):
            calltree = True
        if line.startswith(("Total number in stack", "Sort by top of stack", "Binary Images:")):
            calltree = False
        if calltree or line.startswith(headers):
            selected.append(line)
    report["stack"] = "\n".join(selected)


def supervise(certificate, start, directory, report):
    security = None
    sampler = None
    identity = None
    sampled = False
    cancelled = False
    collector_failed = False
    stack_allowed = False
    progress_sent = False
    deadline = start + 30 * SECOND
    sample_path = directory / "sample.txt"
    try:
        if now() >= deadline:
            raise RuntimeError("shared deadline expired before removal launch")
        security = OwnedChild(["/usr/bin/security", "remove-trusted-cert", "-d", str(certificate)])
        try:
            identity = trusted_identity(security)
            report["securityLaunchIdentity"] = identity
        except (OSError, RuntimeError, UnicodeError) as error:
            report["identityFailure"] = error_fact("launch-identity", error)
        while now() < deadline:
            instant = now()
            security.drain()
            security_exited = security.exited()
            if not sampled and instant >= start + 25 * SECOND:
                sampled = True
                if not security_exited and identity is not None and instant < start + 27 * SECOND:
                    try:
                        report["securitySampleIdentity"] = trusted_identity(security, identity)
                        sampler = OwnedChild(["/usr/bin/sample", str(security.process.pid), "1", "10",
                                              "-file", str(sample_path)], sampler=True)
                        report["sampleStartedNanoseconds"] = now()
                    except (OSError, RuntimeError, UnicodeError, subprocess.SubprocessError) as error:
                        report["sampleFailure"] = error_fact("sample-start", error)
            if sampler is not None and not sampler.reaped:
                if instant >= start + 28 * SECOND:
                    collector_failed = True
                sampler.drain()
                if sampler.exited():
                    if not security_exited:
                        try:
                            report["securityAfterSampleIdentity"] = trusted_identity(security, identity)
                            stack_allowed = True
                        except (OSError, RuntimeError, UnicodeError) as error:
                            report["sampleFailure"] = error_fact("sample-identity-changed", error)
                    sampler.reap()
                    report["sampleFinishedNanoseconds"] = now()
                elif security_exited or instant >= start + 27 * SECOND:
                    sampler.send(signal.SIGKILL)
            if sampled and not progress_sent and (sampler is None or sampler.reaped or instant >= start + 28 * SECOND):
                if stack_allowed:
                    retain_stack(sample_path, report)
                else:
                    report["stackDiscarded"] = sample_path.exists()
                report.update(phase="diagnostic", status="observing", observedNanoseconds=now(),
                              security=security.facts(), sampler=sampler.facts() if sampler else {"launched": False})
                emit(report)
                report["stackInProgressRecord"] = "stack" in report
                report.pop("stack", None)
                progress_sent = True
            if security_exited and (sampler is None or sampler.reaped):
                security.reap()
                if security.reaped:
                    break
            if instant >= start + 29 * SECOND and not security_exited:
                cancelled = True
                security.send(signal.SIGTERM)
            if instant >= start + 29 * SECOND + SECOND // 4 and not security_exited:
                security.send(signal.SIGKILL)
            time.sleep(min(0.01, max(0, (deadline - now()) / SECOND)))
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        report["supervisorFailure"] = error_fact("supervise", error)
        cancelled = True
    finally:
        cleanup_errors = {}
        for child in (sampler, security):
            if child is not None and not child.reaped:
                try:
                    child.send(signal.SIGKILL)
                except (OSError, RuntimeError) as error:
                    cleanup_errors[f"kill-{child.process.pid}"] = error_fact("kill-owned-group", error)
        while now() < deadline and any(child is not None and not child.reaped for child in (sampler, security)):
            for child in (sampler, security):
                if child is not None:
                    try:
                        child.drain()
                        if child is sampler or sampler is None or sampler.reaped:
                            child.reap()
                    except OSError as error:
                        cleanup_errors[f"reap-{child.process.pid}"] = error_fact("reap-owned-child", error)
            time.sleep(min(0.01, max(0, (deadline - now()) / SECOND)))
        report["security"] = security.facts() if security is not None else {"launched": False}
        report["sampler"] = sampler.facts() if sampler is not None else {"launched": False}
        report["cleanupErrors"] = cleanup_errors
        for child in (sampler, security):
            if child is not None:
                child.process.stdout.close()
    if stack_allowed and not progress_sent:
        retain_stack(sample_path, report)
    elif not stack_allowed:
        report["stackDiscarded"] = sample_path.exists()
    report["collectorContainmentFailure"] = collector_failed
    unreaped = any(child is not None and not child.reaped for child in (sampler, security))
    report["status"] = "cleanupFailure" if unreaped or collector_failed or cleanup_errors else (
        "deadline" if cancelled else "removed" if security is not None and security.process.returncode == 0 else "failed")


def emit(report):
    report["outputTruncated"] = False
    encoded = json.dumps(report, ensure_ascii=True).encode("ascii")
    while len(encoded) + 1 > REPORT_LIMIT and report.get("stack"):
        report["stack"] = report["stack"][:len(report["stack"]) // 2]
        report["outputTruncated"] = True
        encoded = json.dumps(report, ensure_ascii=True).encode("ascii")
    if len(encoded) + 1 > REPORT_LIMIT:
        raise RuntimeError("bounded diagnostic metadata exceeded output limit")
    sys.stdout.buffer.write(encoded + b"\n")
    sys.stdout.buffer.flush()


def main(arguments):
    report = {"schema": "SwiftMITM.runnerTrustRemoval.v1", "status": "failed",
              "startedNanoseconds": arguments.started_nanoseconds, "diagnosticMayPerturbTiming": True}
    try:
        if os.geteuid() != 0:
            raise RuntimeError("existing admin authority required")
        if not all(hasattr(os, name) for name in ("waitid", "P_PID", "WEXITED", "WNOHANG", "WNOWAIT")):
            raise RuntimeError("nonreaping child observation unavailable")
        elapsed = now() - arguments.started_nanoseconds
        if elapsed < 0 or elapsed >= 30 * SECOND:
            raise RuntimeError("shared deadline expired or invalid")
        with arguments.certificate.open("rb") as source:
            pem = source.read(OUTPUT_LIMIT + 1)
        if len(pem) > OUTPUT_LIMIT:
            raise ValueError("certificate input exceeds bound")
        der = ssl.PEM_cert_to_DER_cert(pem.decode("ascii"))
        if hashlib.sha256(der).hexdigest() != arguments.sha256:
            raise ValueError("fixture root fingerprint mismatch")
        os.umask(0o077)
        with tempfile.TemporaryDirectory(prefix="swiftmitm-trust-removal-") as temporary:
            directory = Path(temporary)
            certificate = directory / "root.der"
            certificate.write_bytes(der)
            supervise(certificate, arguments.started_nanoseconds, directory, report)
    except (OSError, RuntimeError, ValueError, UnicodeError) as error:
        report["failure"] = error_fact("supervisor-setup-or-report", error)
        report["status"] = "failed"
    report["finishedNanoseconds"] = now()
    report["phase"] = "completed"
    emit(report)
    return 0 if report["status"] == "removed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--started-nanoseconds", type=int, required=True)
    sys.exit(main(parser.parse_args()))

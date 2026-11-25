"""Install or upgrade Quant CTA as a macOS launchd service."""

import argparse
import json
import os
from pathlib import Path
import plistlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


SOURCE = Path(__file__).resolve().parent
LABEL = "net.olven.quant"
SYSTEM_PLIST = Path("/Library/LaunchDaemons") / f"{LABEL}.plist"
PORT = 8766
SUDO = "/usr/bin/sudo"
LAUNCHCTL = "/bin/launchctl"
SYSTEM_PATH = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
SYSTEM_CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
REQUIRED_FILES = ("app.py", "collector.mjs", "index.html", "ledger.py")


def checked(argv, **kwargs):
    subprocess.run([str(value) for value in argv], check=True, **kwargs)


def find_command(name, path):
    result = shutil.which(name, path=path)
    if result is None:
        raise RuntimeError(f"Install {name} and add it to PATH before running the installer")
    return Path(result).absolute()


def is_loaded(prefix, job):
    return subprocess.run(
        [*prefix, LAUNCHCTL, "print", job], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, check=False,
    ).returncode == 0


def daemon_plist(user, release, data, python, env, config):
    arguments = [str(python), str(release / "app.py"), "--serve"]
    if config.is_file():
        arguments += ["--config", str(config)]
    contents = {
        "Label": LABEL,
        "WorkingDirectory": str(release),
        "ProgramArguments": arguments,
        "EnvironmentVariables": {
            "HOME": env["HOME"],
            "PATH": env["PATH"],
            "NODE_BINARY": env["NODE_BINARY"],
            "PYTHONUNBUFFERED": "1",
        },
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "StandardOutPath": str(data / "service.out.log"),
        "StandardErrorPath": str(data / "service.err.log"),
    }
    if user is not None:
        contents["UserName"] = user
    return plistlib.dumps(contents)


def install_plist(source, destination, prefix):
    ownership = ["-o", "root", "-g", "wheel"] if prefix else []
    checked([*prefix, "/usr/bin/install", *ownership, "-m", "644", source, destination])


def verify_http():
    url = f"http://127.0.0.1:{PORT}/"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with opener.open(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.5)
    raise RuntimeError(f"The new service did not serve {url} within 30 seconds")


def stage_release(release, node, env):
    for item in SOURCE.iterdir():
        if (
            item.is_file()
            and not item.name.startswith(("test_", "config."))
            and item.name != Path(__file__).name
            and item.suffix in {".py", ".mjs", ".cjs", ".html", ".css", ".js", ".json"}
        ):
            shutil.copy2(item, release / item.name)
    manifest = release / "package.json"
    if not manifest.is_file():
        return
    package = json.loads(manifest.read_text(encoding="utf-8"))
    dependencies = {**package.get("dependencies", {}), **package.get("optionalDependencies", {})}
    if not dependencies:
        return
    npm = find_command("npm", env["PATH"])
    command = "ci" if (release / "package-lock.json").is_file() else "install"
    checked([npm, command, "--omit=dev", "--no-fund", "--no-audit"], cwd=release, env=env)
    if "playwright" in dependencies and not os.access(SYSTEM_CHROME, os.X_OK):
        playwright = release / "node_modules" / "playwright" / "cli.js"
        browser_env = dict(env, PLAYWRIGHT_SKIP_BROWSER_GC="1")
        checked([node, playwright, "install", "--only-shell", "chromium"], cwd=release, env=browser_env)


def config_path(explicit, data, plist):
    if explicit:
        return explicit.expanduser().resolve()
    if plist.is_file():
        options = plistlib.loads(plist.read_bytes()).get("ProgramArguments", [])
        if "--config" in options:
            return Path(options[options.index("--config") + 1])
    return data / "config.json"


def replace_service(release, config, user, data, python, env, plist, domain):
    prefix = [SUDO, "-n"] if domain == "system" else []
    job = f"{domain}/{LABEL}"
    previous = plist.read_bytes() if plist.is_file() else None
    loaded = is_loaded(prefix, job)
    if loaded and previous is None:
        raise RuntimeError(f"{job} is loaded but has no plist at {plist}")
    if not loaded:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", PORT))

    with tempfile.TemporaryDirectory(prefix="quant-launchd-") as temporary:
        next_plist = Path(temporary) / f"{LABEL}.plist"
        next_plist.write_bytes(daemon_plist(user, release, data, python, env, config))
        if loaded:
            checked([*prefix, LAUNCHCTL, "bootout", job])

        try:
            install_plist(next_plist, plist, prefix)
            checked([*prefix, LAUNCHCTL, "bootstrap", domain, plist])
            verify_http()
        except (OSError, RuntimeError, subprocess.CalledProcessError):
            if is_loaded(prefix, job):
                checked([*prefix, LAUNCHCTL, "bootout", job])
            if previous is not None:
                next_plist.write_bytes(previous)
                install_plist(next_plist, plist, prefix)
                if loaded:
                    checked([*prefix, LAUNCHCTL, "bootstrap", domain, plist])
            elif plist.exists():
                if prefix:
                    checked([*prefix, "/bin/rm", "-f", plist])
                else:
                    plist.unlink()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="existing JSON config outside the release directory")
    parser.add_argument("--user-agent", action="store_true", help="install without sudo for the current GUI login")
    args = parser.parse_args()
    if sys.platform != "darwin" or os.geteuid() == 0:
        parser.error("Run this on macOS as the account that will own the service, without sudo")
    if sys.version_info < (3, 9):
        parser.error("Python 3.9 or later is required")

    import pwd

    account = pwd.getpwuid(os.getuid())
    home = Path(account.pw_dir)
    data = home / "Library" / "Application Support" / "QuantCTA"
    user_plist = home / "Library" / "LaunchAgents" / f"{LABEL}.plist"
    plist = user_plist if args.user_agent else SYSTEM_PLIST
    domain = f"gui/{account.pw_uid}" if args.user_agent else "system"
    missing = [name for name in REQUIRED_FILES if not (SOURCE / name).is_file()]
    if missing:
        parser.error(f"Source files missing: {', '.join(missing)}")

    try:
        other_plist = SYSTEM_PLIST if args.user_agent else user_plist
        if other_plist.is_file():
            raise RuntimeError(f"Remove the existing launchd installation at {other_plist} before switching modes")
        if args.user_agent and not is_loaded([], domain):
            raise RuntimeError(f"Log into the Mac desktop as {account.pw_name} before installing a LaunchAgent")
        config = config_path(args.config, data, plist)
        if (args.config or config != data / "config.json") and not config.is_file():
            parser.error(f"Config file does not exist: {config}")
        paths = [entry for entry in os.environ.get("PATH", "").split(os.pathsep) if Path(entry).is_absolute()]
        paths.extend(SYSTEM_PATH.split(os.pathsep))
        path = os.pathsep.join(paths)
        node = find_command("node", path)
        python = Path(sys.executable).absolute()
        env = os.environ.copy()
        env.update(HOME=str(home), PATH=os.pathsep.join(dict.fromkeys(
            [str(node.parent), str(python.parent), *paths]
        )), NODE_BINARY=str(node))
        # Use the same per-user browser cache for dependency setup and launchd.
        env.pop("PLAYWRIGHT_BROWSERS_PATH", None)
        releases = data / "releases"
        data.mkdir(mode=0o700, parents=True, exist_ok=True)
        releases.mkdir(mode=0o700, exist_ok=True)
        release = Path(tempfile.mkdtemp(prefix="app-", dir=releases))
        try:
            stage_release(release, node, env)
            if args.user_agent:
                plist.parent.mkdir(parents=True, exist_ok=True)
            else:
                checked([SUDO, "-v"])
        except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError):
            shutil.rmtree(release)
            raise
        user = None if args.user_agent else account.pw_name
        replace_service(release, config, user, data, python, env, plist, domain)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Installation failed: {error}", file=sys.stderr)
        return 1

    print(f"{LABEL} is running at http://127.0.0.1:{PORT}/")
    print(f"Persistent data and logs: {data}")
    print(f"Application release: {release}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

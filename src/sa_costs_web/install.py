from __future__ import annotations

import argparse
import getpass
import grp
import os
import pwd
import secrets
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class WebInstallConfig:
    runtime_dir: Path
    env_path: Path
    db_path: Path
    bridge_url: str
    bind_host: str
    bind_port: int
    secret_key: str
    log_level: str
    http_timeout: float
    service_mode: str
    service_name: str
    service_path: Path | None
    service_user: str | None
    service_group: str | None
    enable_now: bool


def run_init(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sa_web init",
        description="Asistente interactivo para configurar e instalar la aplicacion web.",
    )
    parser.parse_args(argv)

    install_config = prompt_install_config()
    validate_install_config(install_config)
    try:
        write_runtime_files(install_config)
    except PermissionError as exc:
        raise RuntimeError(permission_help(install_config)) from exc
    if install_config.service_mode != "none" and install_config.enable_now:
        enable_service(install_config)
    print_summary(install_config)
    return 0


def prompt_install_config() -> WebInstallConfig:
    current_user = pwd.getpwuid(os.getuid()).pw_name
    current_group = grp.getgrgid(os.getgid()).gr_name
    runtime_dir = Path(
        prompt_text(
            "Directorio de trabajo de la web",
            str(Path.cwd()),
        )
    ).expanduser()
    default_env_path = runtime_dir / "solarcost-web.env"
    default_db_path = runtime_dir / "data" / "energy_costs.sqlite3"
    bridge_url = prompt_text("URL del bridge", "http://127.0.0.1:8765").rstrip("/")
    bind_host = prompt_text("Host para publicar la web", "0.0.0.0")
    bind_port = prompt_int("Puerto para publicar la web", 8890)
    secret_key = prompt_secret_with_default(
        "Secret key de Flask (enter para generar automaticamente)",
        secrets.token_urlsafe(48),
    )
    log_level = prompt_text("Nivel de log", "INFO").upper()
    http_timeout = prompt_float("Timeout HTTP hacia el bridge", 10.0)
    env_path = Path(prompt_text("Archivo de entorno", str(default_env_path))).expanduser()
    db_path = Path(prompt_text("Ruta de la base SQLite", str(default_db_path))).expanduser()

    service_mode = prompt_choice(
        "Modo de servicio",
        choices=("system", "user", "none"),
        default="system" if is_root() else "user",
    )
    service_name = prompt_text("Nombre del servicio", "solarcost-web.service")
    service_user: str | None = None
    service_group: str | None = None
    service_path: Path | None = None
    enable_now = False

    if service_mode == "system":
        service_path = Path(prompt_text("Ruta del unit file", f"/etc/systemd/system/{service_name}")).expanduser()
        service_user = prompt_text("Usuario del servicio", current_user)
        service_group = prompt_text("Grupo del servicio", current_group)
        enable_now = prompt_yes_no("Habilitar e iniciar el servicio ahora", True)
    elif service_mode == "user":
        service_path = Path(
            prompt_text(
                "Ruta del unit file",
                str(Path.home() / ".config" / "systemd" / "user" / service_name),
            )
        ).expanduser()
        enable_now = prompt_yes_no("Habilitar e iniciar el servicio de usuario ahora", True)

    return WebInstallConfig(
        runtime_dir=runtime_dir,
        env_path=env_path,
        db_path=db_path,
        bridge_url=bridge_url,
        bind_host=bind_host,
        bind_port=bind_port,
        secret_key=secret_key,
        log_level=log_level,
        http_timeout=http_timeout,
        service_mode=service_mode,
        service_name=service_name,
        service_path=service_path,
        service_user=service_user,
        service_group=service_group,
        enable_now=enable_now,
    )


def write_runtime_files(install_config: WebInstallConfig) -> None:
    install_config.runtime_dir.mkdir(parents=True, exist_ok=True)
    install_config.db_path.parent.mkdir(parents=True, exist_ok=True)
    install_config.env_path.parent.mkdir(parents=True, exist_ok=True)

    write_text_file(
        install_config.env_path,
        build_env_file(install_config),
    )
    if install_config.service_mode != "none" and install_config.service_path is not None:
        install_config.service_path.parent.mkdir(parents=True, exist_ok=True)
        write_text_file(
            install_config.service_path,
            build_service_file(install_config, Path(sys.executable)),
        )


def validate_install_config(install_config: WebInstallConfig) -> None:
    if install_config.service_mode == "system" and not is_root():
        raise RuntimeError(permission_help(install_config))


def build_env_file(install_config: WebInstallConfig) -> str:
    lines = [
        "# SolarCost Web",
        env_line("SA_COSTS_BRIDGE_URL", install_config.bridge_url),
        env_line("SA_COSTS_BIND_HOST", install_config.bind_host),
        env_line("SA_COSTS_BIND_PORT", str(install_config.bind_port)),
        env_line("SA_COSTS_DB_PATH", str(install_config.db_path)),
        env_line("SA_COSTS_SECRET_KEY", install_config.secret_key),
        env_line("SA_COSTS_LOG_LEVEL", install_config.log_level),
        env_line("SA_COSTS_HTTP_TIMEOUT", str(install_config.http_timeout)),
        "",
    ]
    return "\n".join(lines)


def build_service_file(install_config: WebInstallConfig, python_executable: Path) -> str:
    wanted_by = "multi-user.target" if install_config.service_mode == "system" else "default.target"
    user_lines: list[str] = []
    if install_config.service_mode == "system" and install_config.service_user and install_config.service_group:
        user_lines = [
            f"User={install_config.service_user}",
            f"Group={install_config.service_group}",
        ]

    service_lines = [
        "[Unit]",
        "Description=SolarCost Web",
        "Wants=network-online.target",
        "After=network-online.target",
        "",
        "[Service]",
        "Type=simple",
        *user_lines,
        f"WorkingDirectory={install_config.runtime_dir}",
        f"EnvironmentFile={install_config.env_path}",
        f"ExecStart={python_executable} -m sa_costs_web run",
        "Restart=always",
        "RestartSec=5",
        "",
        "[Install]",
        f"WantedBy={wanted_by}",
        "",
    ]
    return "\n".join(service_lines)


def enable_service(install_config: WebInstallConfig) -> None:
    if install_config.service_mode == "none":
        return
    if install_config.service_path is None:
        raise RuntimeError("No se encontro la ruta del servicio para habilitar.")
    if shutil.which("systemctl") is None:
        raise RuntimeError("No se encontro systemctl. El servicio fue generado, pero debes habilitarlo manualmente.")

    try:
        if install_config.service_mode == "system":
            if not is_root():
                raise RuntimeError("Para instalar un servicio de sistema debes ejecutar el init con sudo o elegir modo user.")
            subprocess.run(["systemctl", "daemon-reload"], check=True)
            subprocess.run(["systemctl", "enable", "--now", install_config.service_name], check=True)
            return

        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "--user", "enable", "--now", install_config.service_name], check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"No se pudo habilitar el servicio automaticamente: {exc}") from exc


def print_summary(install_config: WebInstallConfig) -> None:
    print("")
    print("Aplicacion web configurada.")
    print(f"- Directorio: {install_config.runtime_dir}")
    print(f"- Env file: {install_config.env_path}")
    print(f"- Base de datos: {install_config.db_path}")
    if install_config.service_mode != "none" and install_config.service_path is not None:
        print(f"- Servicio: {install_config.service_path}")
        if install_config.service_mode == "user":
            print("- Si quieres que el servicio de usuario arranque al boot, revisa `loginctl enable-linger`.")
    print("- El usuario administrador inicial se crea desde la primera pantalla web al entrar.")


def permission_help(install_config: WebInstallConfig) -> str:
    executable = Path(sys.argv[0]).expanduser()
    service_path = install_config.service_path or Path("/etc/systemd/system/solarcost-web.service")
    return (
        "Para instalar un servicio de sistema debes ejecutar el asistente con permisos de root. "
        "Si el paquete esta dentro de un .venv, `sudo sa_web init` normalmente no funciona "
        "porque sudo no encuentra el binario del entorno virtual. "
        f"Usa alguno de estos comandos:\n"
        f"- sudo \"$(command -v sa_web)\" init\n"
        f"- sudo {executable} init\n"
        "Si prefieres no usar sudo, elige `user` como modo de servicio. "
        f"El unit file de sistema se intentaba escribir en {service_path}."
    )


def env_line(key: str, value: str) -> str:
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'{key}="{escaped}"'


def write_text_file(path: Path, content: str) -> None:
    if path.exists() and not prompt_yes_no(f"{path} ya existe. Sobrescribir", False):
        raise RuntimeError(f"Operacion cancelada. No se sobrescribio {path}.")
    path.write_text(content, encoding="utf-8")


def prompt_text(label: str, default: str | None = None) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        print("Este valor es obligatorio.")


def prompt_secret_with_default(label: str, default: str) -> str:
    value = getpass.getpass(f"{label}: ").strip()
    return value or default


def prompt_int(label: str, default: int) -> int:
    while True:
        raw = input(f"{label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            print("Debes ingresar un numero entero.")


def prompt_float(label: str, default: float) -> float:
    while True:
        raw = input(f"{label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            print("Debes ingresar un numero valido.")


def prompt_yes_no(label: str, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        raw = input(f"{label} [{suffix}]: ").strip().lower()
        if not raw:
            return default
        if raw in {"y", "yes", "s", "si", "sí"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Responde si o no.")


def prompt_choice(label: str, *, choices: tuple[str, ...], default: str) -> str:
    choice_text = "/".join(choices)
    while True:
        raw = input(f"{label} ({choice_text}) [{default}]: ").strip().lower()
        if not raw:
            return default
        if raw in choices:
            return raw
        print(f"Debes elegir una de estas opciones: {choice_text}.")


def is_root() -> bool:
    geteuid = getattr(os, "geteuid", None)
    return bool(geteuid and geteuid() == 0)

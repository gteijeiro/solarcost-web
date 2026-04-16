from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


PACKAGE_NAME = "solarcost-web"


@dataclass(slots=True)
class WebUninstallConfig:
    runtime_dir: Path
    env_path: Path
    db_path: Path
    service_mode: str
    service_name: str
    service_path: Path | None
    remove_service: bool
    remove_env_file: bool
    remove_db_file: bool
    remove_runtime_dir: bool
    uninstall_package: bool


def run_uninstall(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sa_web uninstall",
        description="Asistente interactivo para desinstalar la aplicacion web.",
    )
    parser.parse_args(argv)

    uninstall_config = prompt_uninstall_config()
    validate_uninstall_config(uninstall_config)
    execute_uninstall(uninstall_config)
    print_summary(uninstall_config)
    return 0


def prompt_uninstall_config() -> WebUninstallConfig:
    runtime_dir = Path(
        prompt_text(
            "Directorio de trabajo de la web",
            str(Path.cwd()),
        )
    ).expanduser()
    default_env_path = runtime_dir / "solarcost-web.env"
    default_db_path = runtime_dir / "data" / "energy_costs.sqlite3"

    service_mode = prompt_choice(
        "Modo de servicio",
        choices=("system", "user", "none"),
        default="system" if is_root() else "user",
    )
    service_name = prompt_text("Nombre del servicio", "solarcost-web.service")
    service_path: Path | None = None
    if service_mode == "system":
        service_path = Path(prompt_text("Ruta del unit file", f"/etc/systemd/system/{service_name}")).expanduser()
    elif service_mode == "user":
        service_path = Path(
            prompt_text(
                "Ruta del unit file",
                str(Path.home() / ".config" / "systemd" / "user" / service_name),
            )
        ).expanduser()

    remove_service = prompt_yes_no("Detener, deshabilitar y borrar el servicio", True)
    remove_env_file = prompt_yes_no("Borrar el archivo solarcost-web.env", False)
    remove_db_file = prompt_yes_no("Borrar la base SQLite", False)
    remove_runtime_dir = prompt_yes_no("Borrar todo el directorio de trabajo", False)
    uninstall_package = prompt_yes_no("Ejecutar pip uninstall del paquete", True)

    return WebUninstallConfig(
        runtime_dir=runtime_dir,
        env_path=Path(prompt_text("Archivo de entorno", str(default_env_path))).expanduser(),
        db_path=Path(prompt_text("Ruta de la base SQLite", str(default_db_path))).expanduser(),
        service_mode=service_mode,
        service_name=service_name,
        service_path=service_path,
        remove_service=remove_service,
        remove_env_file=remove_env_file,
        remove_db_file=remove_db_file,
        remove_runtime_dir=remove_runtime_dir,
        uninstall_package=uninstall_package,
    )


def validate_uninstall_config(uninstall_config: WebUninstallConfig) -> None:
    needs_root = uninstall_config.service_mode == "system" and uninstall_config.remove_service
    if needs_root and not is_root():
        raise RuntimeError(permission_help(uninstall_config))


def execute_uninstall(uninstall_config: WebUninstallConfig) -> None:
    if uninstall_config.remove_service:
        remove_service(uninstall_config)

    if uninstall_config.remove_env_file:
        unlink_if_exists(uninstall_config.env_path)

    if uninstall_config.remove_db_file:
        unlink_if_exists(uninstall_config.db_path)

    if uninstall_config.remove_runtime_dir and uninstall_config.runtime_dir.exists():
        shutil.rmtree(uninstall_config.runtime_dir)

    if uninstall_config.uninstall_package:
        subprocess.run([sys.executable, "-m", "pip", "uninstall", "-y", PACKAGE_NAME], check=True)


def remove_service(uninstall_config: WebUninstallConfig) -> None:
    if uninstall_config.service_mode == "none":
        return
    if shutil.which("systemctl") is None:
        raise RuntimeError("No se encontro systemctl. Debes borrar el servicio manualmente.")

    if uninstall_config.service_mode == "system":
        subprocess.run(["systemctl", "stop", uninstall_config.service_name], check=False)
        subprocess.run(["systemctl", "disable", uninstall_config.service_name], check=False)
        if uninstall_config.service_path is not None:
            unlink_if_exists(uninstall_config.service_path)
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        return

    subprocess.run(["systemctl", "--user", "stop", uninstall_config.service_name], check=False)
    subprocess.run(["systemctl", "--user", "disable", uninstall_config.service_name], check=False)
    if uninstall_config.service_path is not None:
        unlink_if_exists(uninstall_config.service_path)
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)


def print_summary(uninstall_config: WebUninstallConfig) -> None:
    print("")
    print("Desinstalacion de la web completada.")
    if uninstall_config.remove_service:
        print(f"- Servicio removido: {uninstall_config.service_name}")
    if uninstall_config.remove_env_file:
        print(f"- Env eliminado: {uninstall_config.env_path}")
    if uninstall_config.remove_db_file:
        print(f"- Base eliminada: {uninstall_config.db_path}")
    if uninstall_config.remove_runtime_dir:
        print(f"- Directorio eliminado: {uninstall_config.runtime_dir}")
    if uninstall_config.uninstall_package:
        print(f"- Paquete desinstalado: {PACKAGE_NAME}")


def permission_help(uninstall_config: WebUninstallConfig) -> str:
    executable = Path(sys.argv[0]).expanduser()
    service_path = uninstall_config.service_path or Path("/etc/systemd/system/solarcost-web.service")
    return (
        "Para desinstalar un servicio de sistema debes ejecutar el asistente con permisos de root. "
        "Si el paquete esta dentro de un .venv, `sudo sa_web uninstall` normalmente no funciona "
        "porque sudo no encuentra el binario del entorno virtual. "
        f"Usa alguno de estos comandos:\n"
        f"- sudo \"$(command -v sa_web)\" uninstall\n"
        f"- sudo {executable} uninstall\n"
        "Si prefieres no usar sudo, deja `system` y el servicio intactos o elige `user` si era un servicio de usuario. "
        f"El unit file de sistema esperado es {service_path}."
    )


def unlink_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def prompt_text(label: str, default: str | None = None) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        print("Este valor es obligatorio.")


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

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from functools import wraps
from html import escape
from pathlib import Path
from typing import Any, Callable

from flask import (
    Flask,
    Response,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from markupsafe import Markup
from werkzeug.security import check_password_hash, generate_password_hash

from .calculator import (
    VARIABLE_ALIASES,
    BridgeData,
    build_period_ranges,
    calculate_period_summary,
    fetch_bridge_data,
    normalize_reference_key,
)
from .config import WebConfig
from .db import CostsRepository

REFERENCE_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CHARGE_SECTIONS = {"service", "tax"}
MONTH_ABBR_ES = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")
ROLE_LABELS = {"admin": "Administrador", "viewer": "Visualizador"}
SERIES_COLORS = (
    ("#2563eb", "rgba(37, 99, 235, 0.18)"),
    ("#ef4444", "rgba(239, 68, 68, 0.18)"),
    ("#14b8a6", "rgba(20, 184, 166, 0.18)"),
    ("#facc15", "rgba(250, 204, 21, 0.18)"),
    ("#22c55e", "rgba(34, 197, 94, 0.18)"),
    ("#f97316", "rgba(249, 115, 22, 0.18)"),
)


@dataclass(slots=True)
class DashboardData:
    bridge_data: BridgeData | None
    bridge_error: str | None
    summaries: list[dict[str, Any]]


@dataclass(slots=True)
class SeedResult:
    source: str
    copied_count: int
    source_period_name: str | None = None


@dataclass(slots=True)
class ConsumptionComparisonData:
    max_kwh: float
    items: list[dict[str, Any]]


@dataclass(slots=True)
class ToggleChartData:
    title: str
    subtitle: str
    legend: list[dict[str, str]]
    config: dict[str, Any]


def localize_number_text(text: str) -> str:
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def trim_decimal_text(text: str) -> str:
    return text.rstrip("0").rstrip(".")


def format_money_value(value: Any) -> str:
    amount = float(value or 0.0)
    return f"${localize_number_text(f'{amount:,.2f}')}"


def format_kwh_value(value: Any) -> str:
    amount = float(value or 0.0)
    return localize_number_text(trim_decimal_text(f"{amount:,.3f}"))


def format_percent_value(value: Any) -> str:
    if value is None:
        return "n/d"
    amount = float(value)
    return f"{localize_number_text(f'{amount:,.2f}')}%"


def format_chart_tick(value: float, kind: str) -> str:
    if kind == "money_rate":
        return f"${localize_number_text(trim_decimal_text(f'{value:,.2f}'))}"
    if abs(value) >= 1000:
        compact = f"{localize_number_text(trim_decimal_text(f'{value / 1000:.1f}'))}k"
    else:
        compact = localize_number_text(trim_decimal_text(f"{value:.0f}"))
    return f"${compact}" if kind == "money" else compact


def format_period_axis_label(period: dict[str, Any]) -> str:
    raw_date = str(period.get("effective_start") or period.get("starts_on") or "")
    try:
        start_date = date.fromisoformat(raw_date)
    except ValueError:
        return raw_date
    return f"{MONTH_ABBR_ES[start_date.month - 1]} {start_date.year}"


def format_day_axis_label(raw_date: str) -> str:
    try:
        parsed = date.fromisoformat(str(raw_date))
    except ValueError:
        return str(raw_date)
    return parsed.strftime("%d/%m")


def format_datetime_value(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "sin datos"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone()
    return parsed.strftime("%d/%m/%Y %H:%M")


def create_app(config: WebConfig) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = config.secret_key
    app.logger.setLevel(getattr(logging, config.log_level, logging.INFO))
    app.extensions["repo"] = CostsRepository(config.db_path)
    app.extensions["web_config"] = config

    @app.template_filter("money")
    def money_filter(value: Any) -> Markup:
        return Markup(f'<span class="val-money">{format_money_value(value)}</span>')

    @app.template_filter("kwh")
    def kwh_filter(value: Any) -> Markup:
        return Markup(f'<span class="val-energy">{format_kwh_value(value)}</span>')

    @app.template_filter("percent")
    def percent_filter(value: Any) -> str:
        return format_percent_value(value)

    @app.template_filter("datetime_local")
    def datetime_local_filter(value: Any) -> str:
        return format_datetime_value(value)

    @app.context_processor
    def inject_globals() -> dict[str, Any]:
        current_user = get_current_user()
        return {
            "current_user": current_user,
            "can_manage": is_admin_user(current_user),
            "role_label": ROLE_LABELS.get(str((current_user or {}).get("role") or ""), "Usuario"),
            "has_users": get_repo().user_count() > 0,
            "bridge_url": get_web_config().bridge_url,
        }

    def render_settings_page(*, import_preview: dict[str, Any] | None = None) -> str:
        repo = get_repo()
        return render_template(
            "settings.html",
            template_bands=repo.list_tariff_bands(scope="default"),
            default_fixed=repo.list_charge_rules(scope="default", kind="fixed"),
            default_taxes=repo.list_charge_rules(scope="default", kind="tax"),
            import_preview=import_preview,
        )

    @app.before_request
    def require_setup_when_empty() -> Any:
        endpoint = request.endpoint or ""
        if endpoint == "static":
            return None
        if get_repo().user_count() == 0 and endpoint != "setup":
            return redirect(url_for("setup"))
        current_user = get_current_user()
        if current_user is not None and not bool(current_user.get("enabled", 1)):
            session.clear()
            flash("Tu usuario esta deshabilitado.", "error")
            return redirect(url_for("login"))
        return None

    @app.route("/setup", methods=["GET", "POST"])
    def setup() -> Any:
        if get_repo().user_count() > 0:
            return redirect(url_for("login"))

        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            password_confirm = request.form.get("password_confirm", "")

            if not username:
                flash("El usuario es obligatorio.", "error")
            elif len(password) < 6:
                flash("La contrasena debe tener al menos 6 caracteres.", "error")
            elif password != password_confirm:
                flash("Las contrasenas no coinciden.", "error")
            else:
                try:
                    user_id = get_repo().create_user(username, generate_password_hash(password), role="admin")
                except Exception as exc:  # noqa: BLE001
                    flash(f"No se pudo crear el usuario: {exc}", "error")
                else:
                    session["user_id"] = user_id
                    flash("Usuario administrador creado.", "success")
                    return redirect(url_for("dashboard"))

        return render_template("setup.html")

    @app.route("/login", methods=["GET", "POST"])
    def login() -> Any:
        if get_repo().user_count() == 0:
            return redirect(url_for("setup"))
        if get_current_user() is not None:
            return redirect(url_for("dashboard"))

        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            user = get_repo().get_user_by_username(username)
            if user is None or not bool(user.get("enabled", 1)) or not check_password_hash(user["password_hash"], password):
                flash("Usuario o contrasena incorrectos.", "error")
            else:
                session["user_id"] = user["id"]
                flash("Sesion iniciada.", "success")
                return redirect(url_for("dashboard"))

        return render_template("login.html")

    @app.route("/logout", methods=["POST"])
    def logout() -> Any:
        session.clear()
        flash("Sesion cerrada.", "success")
        return redirect(url_for("login"))

    @app.route("/manifest.webmanifest")
    def manifest_file() -> Any:
        manifest_path = Path(str(current_app.static_folder)) / "manifest.webmanifest"
        response = Response(manifest_path.read_text(encoding="utf-8"), mimetype="application/manifest+json")
        response.headers["Cache-Control"] = "no-cache"
        return response

    @app.route("/sw.js")
    def service_worker() -> Any:
        sw_path = Path(str(current_app.static_folder)) / "sw.js"
        response = Response(sw_path.read_text(encoding="utf-8"), mimetype="application/javascript")
        response.headers["Service-Worker-Allowed"] = "/"
        response.headers["Cache-Control"] = "no-cache"
        return response

    @app.route("/")
    @login_required
    def dashboard() -> Any:
        periods = get_repo().list_billing_periods()
        dashboard_data = build_dashboard_data(periods)
        current_summary = dashboard_data.summaries[0] if dashboard_data.summaries else None
        comparison_data = build_consumption_comparison_data(dashboard_data.summaries)
        consumption_chart = build_consumption_chart(comparison_data)
        costs_chart = build_costs_chart(dashboard_data.summaries)
        tariff_price_chart = build_tariff_price_chart(dashboard_data.summaries)
        fixed_charge_chart = build_fixed_charge_chart(dashboard_data.summaries)
        yearly_change = build_yearly_change_summary(dashboard_data.summaries)
        return render_template(
            "dashboard.html",
            periods=periods,
            summaries=dashboard_data.summaries,
            current_summary=current_summary,
            consumption_chart=consumption_chart,
            costs_chart=costs_chart,
            tariff_price_chart=tariff_price_chart,
            fixed_charge_chart=fixed_charge_chart,
            yearly_change=yearly_change,
            bridge_status=dashboard_data.bridge_data.status if dashboard_data.bridge_data else None,
            bridge_error=dashboard_data.bridge_error,
        )

    @app.route("/account")
    @login_required
    def account() -> Any:
        repo = get_repo()
        return render_template(
            "account.html",
            users=repo.list_users() if is_admin_user(get_current_user()) else [],
        )

    @app.route("/account/password", methods=["POST"])
    @login_required
    def change_password() -> Any:
        user = get_current_user()
        if user is None:
            return redirect(url_for("login"))

        current_password = request.form.get("current_password", "")
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not check_password_hash(str(user["password_hash"]), current_password):
            flash("La contrasena actual no es correcta.", "error")
        elif len(new_password) < 6:
            flash("La nueva contrasena debe tener al menos 6 caracteres.", "error")
        elif new_password != confirm_password:
            flash("Las nuevas contrasenas no coinciden.", "error")
        else:
            get_repo().update_user_password(int(user["id"]), generate_password_hash(new_password))
            flash("Contrasena actualizada.", "success")
        return redirect(url_for("account"))

    @app.route("/account/users/save", methods=["POST"])
    @admin_required
    def save_user() -> Any:
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        password_confirm = request.form.get("password_confirm", "")
        role = request.form.get("role", "").strip()

        if role not in ROLE_LABELS:
            flash("Rol invalido.", "error")
        elif not username:
            flash("El usuario es obligatorio.", "error")
        elif len(password) < 6:
            flash("La contrasena debe tener al menos 6 caracteres.", "error")
        elif password != password_confirm:
            flash("Las contrasenas no coinciden.", "error")
        else:
            try:
                get_repo().create_user(username, generate_password_hash(password), role=role)
            except Exception as exc:  # noqa: BLE001
                flash(f"No se pudo crear el usuario: {exc}", "error")
            else:
                flash("Usuario creado.", "success")
        return redirect(url_for("account"))

    @app.route("/account/users/<int:user_id>/toggle-enabled", methods=["POST"])
    @admin_required
    def toggle_user_enabled(user_id: int) -> Any:
        current_user = get_current_user()
        if current_user is None:
            return redirect(url_for("login"))
        if int(current_user["id"]) == user_id:
            flash("No puedes deshabilitar tu propio usuario.", "error")
            return redirect(url_for("account"))

        target_user = get_repo().get_user_by_id(user_id)
        if target_user is None:
            flash("Usuario inexistente.", "error")
            return redirect(url_for("account"))

        enabled = request.form.get("enabled") == "1"
        get_repo().update_user_enabled(user_id, enabled)
        flash(
            "Usuario habilitado." if enabled else "Usuario deshabilitado.",
            "success",
        )
        return redirect(url_for("account"))

    @app.route("/settings")
    @admin_required
    def settings() -> Any:
        return render_settings_page()

    @app.route("/settings/export")
    @admin_required
    def export_settings() -> Any:
        payload = get_repo().export_configuration()
        response = Response(
            json.dumps(payload, ensure_ascii=False, indent=2),
            mimetype="application/json",
        )
        response.headers["Content-Disposition"] = (
            f'attachment; filename="solarcost-web-config-{date.today().isoformat()}.json"'
        )
        return response

    @app.route("/settings/import/preview", methods=["POST"])
    @admin_required
    def preview_import_settings() -> Any:
        uploaded_file = request.files.get("config_file")
        if uploaded_file is None or not str(uploaded_file.filename or "").strip():
            flash("Selecciona un archivo JSON para importar.", "error")
            return redirect(url_for("settings"))

        try:
            payload = json.loads(uploaded_file.read().decode("utf-8"))
            prepared_payload = get_repo().prepare_configuration_import(payload)
        except UnicodeDecodeError:
            flash("El archivo importado debe estar codificado en UTF-8.", "error")
            return redirect(url_for("settings"))
        except json.JSONDecodeError as exc:
            flash(f"El archivo JSON no es valido: {exc.msg}.", "error")
            return redirect(url_for("settings"))
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo leer la configuracion importada: {exc}", "error")
            return redirect(url_for("settings"))

        return render_settings_page(
            import_preview=build_import_preview_data(prepared_payload),
        )

    @app.route("/settings/import/apply", methods=["POST"])
    @admin_required
    def apply_import_settings() -> Any:
        payload_json = str(request.form.get("payload_json") or "").strip()
        if not payload_json:
            flash("La previsualizacion de importacion expiro. Vuelve a subir el archivo.", "error")
            return redirect(url_for("settings"))

        try:
            payload = json.loads(payload_json)
            prepared_payload = get_repo().prepare_configuration_import(payload)
        except json.JSONDecodeError as exc:
            flash(f"El archivo JSON no es valido: {exc.msg}.", "error")
            return redirect(url_for("settings"))
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo validar la configuracion importada: {exc}", "error")
            return redirect(url_for("settings"))

        include_default_bands = request.form.get("include_default_bands") == "on"
        include_default_fixed = request.form.get("include_default_fixed") == "on"
        include_default_taxes = request.form.get("include_default_taxes") == "on"
        selected_period_starts_on = {
            str(value).strip()
            for value in request.form.getlist("selected_period_starts_on")
            if str(value).strip()
        }

        try:
            result = get_repo().import_configuration(
                prepared_payload,
                include_default_bands=include_default_bands,
                include_default_fixed=include_default_fixed,
                include_default_taxes=include_default_taxes,
                selected_period_starts_on=selected_period_starts_on,
            )
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo importar la configuracion: {exc}", "error")
            return render_settings_page(
                import_preview=build_import_preview_data(prepared_payload),
            )

        flash(build_import_result_message(result), "success")
        return redirect(url_for("settings"))

    @app.route("/settings/bands/save", methods=["POST"])
    @admin_required
    def save_default_band() -> Any:
        try:
            handle_band_save(scope="default", billing_period_id=None)
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo guardar la franja: {exc}", "error")
        else:
            flash("Franja guardada.", "success")
        return redirect(url_for("settings"))

    @app.route("/settings/bands/<int:band_id>/delete", methods=["POST"])
    @admin_required
    def delete_default_band(band_id: int) -> Any:
        get_repo().delete_tariff_band(band_id)
        flash("Franja eliminada.", "success")
        return redirect(url_for("settings"))

    @app.route("/settings/charges/save", methods=["POST"])
    @admin_required
    def save_default_charge() -> Any:
        try:
            handle_charge_save(scope="default", billing_period_id=None)
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo guardar la regla: {exc}", "error")
        else:
            flash("Regla guardada.", "success")
        return redirect(url_for("settings"))

    @app.route("/settings/charges/<int:rule_id>/delete", methods=["POST"])
    @admin_required
    def delete_default_charge(rule_id: int) -> Any:
        get_repo().delete_charge_rule(rule_id)
        flash("Regla eliminada.", "success")
        return redirect(url_for("settings"))

    @app.route("/periods")
    @admin_required
    def periods() -> Any:
        repo = get_repo()
        periods = repo.list_billing_periods()
        dashboard_data = build_dashboard_data(periods) if periods else DashboardData(None, None, [])
        period_summaries = {item["period"]["id"]: item for item in dashboard_data.summaries}
        return render_template(
            "periods.html",
            periods=periods,
            period_summaries=period_summaries,
            bridge_error=dashboard_data.bridge_error,
        )

    @app.route("/periods/save", methods=["POST"])
    @admin_required
    def save_period() -> Any:
        repo = get_repo()
        period_id = parse_optional_int(request.form.get("period_id"))
        name = request.form.get("name", "").strip()
        starts_on = request.form.get("starts_on", "").strip()
        notes = request.form.get("notes", "").strip()
        utility_measured_kwh = parse_optional_float(request.form.get("utility_measured_kwh"))
        has_inverter_data_issue = request.form.get("has_inverter_data_issue") == "on"
        existing_period = repo.get_billing_period(period_id) if period_id is not None else None
        requested_billing_source = request.form.get("billing_source")

        if not name:
            flash("El nombre del periodo es obligatorio.", "error")
            return redirect(url_for("periods"))
        if not starts_on:
            flash("La fecha de inicio es obligatoria.", "error")
            return redirect(url_for("periods"))
        if utility_measured_kwh is not None and utility_measured_kwh < 0:
            flash("El consumo medido por la compania no puede ser negativo.", "error")
            if period_id is None:
                return redirect(url_for("periods"))
            return redirect(url_for("period_detail", period_id=period_id))

        try:
            billing_source = resolve_billing_source(
                requested_billing_source,
                utility_measured_kwh=utility_measured_kwh,
                existing_period=existing_period,
            )
            saved_id = repo.save_billing_period(
                period_id=period_id,
                name=name,
                starts_on=starts_on,
                utility_measured_kwh=utility_measured_kwh,
                has_inverter_data_issue=has_inverter_data_issue,
                billing_source=billing_source,
                notes=notes,
            )
            tariff_seed_result = ensure_period_tariff_bands(saved_id, starts_on) if period_id is None else None
            fixed_seed_result = ensure_period_fixed_charges(saved_id, starts_on) if period_id is None else None
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo guardar el periodo: {exc}", "error")
            return redirect(url_for("periods"))

        if tariff_seed_result is not None:
            flash_tariff_seed_result(tariff_seed_result)
        if fixed_seed_result is not None:
            flash_fixed_charge_seed_result(fixed_seed_result)
        flash("Periodo guardado.", "success")
        return redirect(url_for("period_detail", period_id=saved_id))

    @app.route("/periods/<int:period_id>/billing-source", methods=["POST"])
    @admin_required
    def save_period_billing_source(period_id: int) -> Any:
        repo = get_repo()
        period = ensure_period_exists(period_id)
        requested_billing_source = str(request.form.get("billing_source") or "").strip()
        utility_measured_kwh = float(period["utility_measured_kwh"]) if period.get("utility_measured_kwh") is not None else None
        billing_source = resolve_billing_source(
            requested_billing_source,
            utility_measured_kwh=utility_measured_kwh,
            existing_period=period,
        )
        if billing_source == "utility" and utility_measured_kwh is None:
            flash("No puedes usar la compania para calcular hasta cargar su lectura.", "error")
            return redirect(url_for("period_detail", period_id=period_id))

        repo.update_billing_source(period_id, billing_source)
        flash(
            "La fuente usada para calcular la factura fue actualizada.",
            "success",
        )
        return redirect(url_for("period_detail", period_id=period_id))

    @app.route("/periods/<int:period_id>/delete", methods=["POST"])
    @admin_required
    def delete_period(period_id: int) -> Any:
        get_repo().delete_billing_period(period_id)
        flash("Periodo eliminado.", "success")
        return redirect(url_for("periods"))

    @app.route("/periods/<int:period_id>")
    @login_required
    def period_detail(period_id: int) -> Any:
        repo = get_repo()
        period = repo.get_billing_period(period_id)
        if period is None:
            abort(404)

        all_periods = repo.list_billing_periods()
        dashboard_data = build_dashboard_data(all_periods)
        summary = next(
            (item for item in dashboard_data.summaries if item["period"]["id"] == period_id),
            None,
        )
        daily_cost_chart = build_period_daily_cost_chart(summary)
        daily_energy_chart = build_period_daily_energy_chart(summary)

        return render_template(
            "period_detail.html",
            period=period,
            summary=summary,
            period_comparison=build_period_consumption_comparison(summary),
            daily_cost_chart=daily_cost_chart,
            daily_energy_chart=daily_energy_chart,
            bridge_status=dashboard_data.bridge_data.status if dashboard_data.bridge_data else None,
            bridge_error=dashboard_data.bridge_error,
            period_bands=repo.list_tariff_bands(scope="period", billing_period_id=period_id),
            override_fixed=repo.list_charge_rules(scope="period", kind="fixed", billing_period_id=period_id),
            override_taxes=repo.list_charge_rules(scope="period", kind="tax", billing_period_id=period_id),
            effective_bands=repo.get_effective_tariff_bands(period_id),
            effective_fixed=repo.get_effective_fixed_charges(period_id),
            effective_taxes=repo.get_effective_tax_rules(period_id),
        )

    @app.route("/periods/<int:period_id>/bands/save", methods=["POST"])
    @admin_required
    def save_period_band(period_id: int) -> Any:
        ensure_period_exists(period_id)
        try:
            handle_band_save(scope="period", billing_period_id=period_id)
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo guardar la franja del periodo: {exc}", "error")
        else:
            flash("Franja del periodo guardada.", "success")
        return redirect(url_for("period_detail", period_id=period_id))

    @app.route("/periods/<int:period_id>/bands/<int:band_id>/delete", methods=["POST"])
    @admin_required
    def delete_period_band(period_id: int, band_id: int) -> Any:
        ensure_period_exists(period_id)
        get_repo().delete_tariff_band(band_id)
        flash("Franja del periodo eliminada.", "success")
        return redirect(url_for("period_detail", period_id=period_id))

    @app.route("/periods/<int:period_id>/bands/seed", methods=["POST"])
    @admin_required
    def seed_period_bands(period_id: int) -> Any:
        period = ensure_period_exists(period_id)
        seed_result = ensure_period_tariff_bands(period_id, period["starts_on"])
        flash_tariff_seed_result(seed_result)
        return redirect(url_for("period_detail", period_id=period_id))

    @app.route("/periods/<int:period_id>/charges/fixed/seed", methods=["POST"])
    @admin_required
    def seed_period_fixed_charges(period_id: int) -> Any:
        period = ensure_period_exists(period_id)
        seed_result = ensure_period_fixed_charges(period_id, period["starts_on"])
        flash_fixed_charge_seed_result(seed_result)
        return redirect(url_for("period_detail", period_id=period_id))

    @app.route("/periods/<int:period_id>/charges/save", methods=["POST"])
    @admin_required
    def save_period_charge(period_id: int) -> Any:
        ensure_period_exists(period_id)
        try:
            handle_charge_save(scope="period", billing_period_id=period_id)
        except Exception as exc:  # noqa: BLE001
            flash(f"No se pudo guardar la regla del periodo: {exc}", "error")
        else:
            flash("Regla del periodo guardada.", "success")
        return redirect(url_for("period_detail", period_id=period_id))

    @app.route("/periods/<int:period_id>/charges/<int:rule_id>/delete", methods=["POST"])
    @admin_required
    def delete_period_charge(period_id: int, rule_id: int) -> Any:
        ensure_period_exists(period_id)
        get_repo().delete_charge_rule(rule_id)
        flash("Regla del periodo eliminada.", "success")
        return redirect(url_for("period_detail", period_id=period_id))

    return app


def login_required(view: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if get_current_user() is None:
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


def admin_required(view: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        user = get_current_user()
        if user is None:
            return redirect(url_for("login"))
        if not is_admin_user(user):
            flash("Tu usuario solo tiene acceso de visualizacion.", "error")
            return redirect(url_for("dashboard"))
        return view(*args, **kwargs)

    return wrapped


def get_repo() -> CostsRepository:
    return current_app.extensions["repo"]


def get_web_config() -> WebConfig:
    return current_app.extensions["web_config"]


def get_current_user() -> dict[str, Any] | None:
    user_id = session.get("user_id")
    if not isinstance(user_id, int):
        return None
    return get_repo().get_user_by_id(user_id)


def is_admin_user(user: dict[str, Any] | None) -> bool:
    return str((user or {}).get("role") or "admin") == "admin"


def ensure_period_exists(period_id: int) -> dict[str, Any]:
    period = get_repo().get_billing_period(period_id)
    if period is None:
        abort(404)
    return period


def ensure_period_tariff_bands(period_id: int, starts_on: str) -> SeedResult:
    repo = get_repo()
    existing = repo.list_tariff_bands(scope="period", billing_period_id=period_id)
    if existing:
        return SeedResult(source="existing", copied_count=len(existing))

    source_period = repo.find_latest_period_with_tariff_bands_before(
        starts_on,
        exclude_period_id=period_id,
    )
    if source_period is not None:
        source_bands = repo.list_tariff_bands(scope="period", billing_period_id=source_period["id"])
        copied_count = repo.copy_tariff_bands_to_period(
            billing_period_id=period_id,
            source_bands=source_bands,
        )
        return SeedResult(
            source="previous",
            copied_count=copied_count,
            source_period_name=source_period["name"],
        )

    template_bands = repo.list_tariff_bands(scope="default")
    copied_count = repo.copy_tariff_bands_to_period(
        billing_period_id=period_id,
        source_bands=template_bands,
    )
    if copied_count:
        return SeedResult(source="template", copied_count=copied_count)
    return SeedResult(source="empty", copied_count=0)


def ensure_period_fixed_charges(period_id: int, starts_on: str) -> SeedResult:
    repo = get_repo()
    existing = repo.list_charge_rules(scope="period", kind="fixed", billing_period_id=period_id)
    if existing:
        return SeedResult(source="existing", copied_count=len(existing))

    source_period = repo.find_latest_period_with_charge_rules_before(
        starts_on,
        kind="fixed",
        exclude_period_id=period_id,
    )
    if source_period is not None:
        source_rules = repo.list_charge_rules(scope="period", kind="fixed", billing_period_id=source_period["id"])
        copied_count = repo.copy_charge_rules_to_period(
            billing_period_id=period_id,
            source_rules=source_rules,
            kind="fixed",
        )
        return SeedResult(
            source="previous",
            copied_count=copied_count,
            source_period_name=source_period["name"],
        )

    template_rules = repo.list_charge_rules(scope="default", kind="fixed")
    copied_count = repo.copy_charge_rules_to_period(
        billing_period_id=period_id,
        source_rules=template_rules,
        kind="fixed",
    )
    if copied_count:
        return SeedResult(source="template", copied_count=copied_count)
    return SeedResult(source="empty", copied_count=0)


def flash_tariff_seed_result(result: SeedResult) -> None:
    if result.source == "existing":
        flash("Este periodo ya tiene sus propios precios por franja.", "success")
        return
    if result.source == "previous":
        flash(
            f"Se copiaron {result.copied_count} franjas desde {result.source_period_name}.",
            "success",
        )
        return
    if result.source == "template":
        flash(
            f"Se copiaron {result.copied_count} franjas desde la plantilla de tarifas.",
            "success",
        )
        return
    flash(
        "El periodo se guardo, pero no hay una plantilla ni otro mes con tarifas para copiar.",
        "error",
    )


def flash_fixed_charge_seed_result(result: SeedResult) -> None:
    if result.source == "existing":
        flash("Este periodo ya tiene sus propios cargos fijos.", "success")
        return
    if result.source == "previous":
        flash(
            f"Se copiaron {result.copied_count} cargos fijos desde {result.source_period_name}.",
            "success",
        )
        return
    if result.source == "template":
        flash(
            f"Se copiaron {result.copied_count} cargos fijos desde la plantilla base.",
            "success",
        )
        return
    flash(
        "El periodo se guardo, pero no hay una plantilla ni otro mes con cargos fijos para copiar.",
        "error",
    )


def build_import_preview_data(prepared_payload: dict[str, Any]) -> dict[str, Any]:
    data = prepared_payload.get("data") if isinstance(prepared_payload, dict) else {}
    defaults = data.get("defaults") if isinstance(data, dict) else {}
    periods = data.get("periods") if isinstance(data, dict) else []
    local_periods = get_repo().list_billing_periods(ascending=True)
    existing_starts_on = {str(period.get("starts_on") or "") for period in local_periods}

    return {
        "exported_at": str(prepared_payload.get("exported_at") or ""),
        "payload_json": json.dumps(prepared_payload, ensure_ascii=False),
        "sections": [
            {
                "field": "include_default_bands",
                "title": "Plantilla de tarifa por consumo",
                "description": "Reemplaza las franjas base actuales por las del archivo.",
                "count": len(defaults.get("tariff_bands") or []),
                "checked": bool(defaults.get("tariff_bands")),
            },
            {
                "field": "include_default_fixed",
                "title": "Plantilla de importes fijos",
                "description": "Reemplaza los cargos fijos base actuales por los del archivo.",
                "count": len(defaults.get("fixed_charges") or []),
                "checked": bool(defaults.get("fixed_charges")),
            },
            {
                "field": "include_default_taxes",
                "title": "Conceptos calculados",
                "description": "Reemplaza las reglas por formula base actuales por las del archivo.",
                "count": len(defaults.get("tax_rules") or []),
                "checked": bool(defaults.get("tax_rules")),
            },
        ],
        "periods": [
            {
                "name": str(period.get("name") or ""),
                "starts_on": str(period.get("starts_on") or ""),
                "utility_measured_kwh": period.get("utility_measured_kwh"),
                "has_inverter_data_issue": bool(period.get("has_inverter_data_issue", False)),
                "billing_source": str(period.get("billing_source") or "inverter"),
                "band_count": len(period.get("tariff_bands") or []),
                "fixed_count": len(period.get("fixed_charges") or []),
                "tax_count": len(period.get("tax_rules") or []),
                "exists_locally": str(period.get("starts_on") or "") in existing_starts_on,
            }
            for period in periods
        ],
    }


def build_import_result_message(result: dict[str, int]) -> str:
    parts: list[str] = []
    if result.get("default_bands_replaced"):
        parts.append("plantilla de tarifas actualizada")
    if result.get("default_fixed_replaced"):
        parts.append("plantilla de cargos fijos actualizada")
    if result.get("default_taxes_replaced"):
        parts.append("conceptos calculados actualizados")
    if result.get("periods_created"):
        parts.append(f"{result['periods_created']} periodos creados")
    if result.get("periods_updated"):
        parts.append(f"{result['periods_updated']} periodos actualizados")
    if not parts:
        return "No se selecciono ningun elemento para importar."
    return "Importacion aplicada: " + ", ".join(parts) + "."


def build_dashboard_data(periods: list[dict[str, Any]]) -> DashboardData:
    bridge_data: BridgeData | None = None
    bridge_error: str | None = None
    summaries: list[dict[str, Any]] = []

    if not periods:
        return DashboardData(bridge_data=None, bridge_error=None, summaries=[])

    try:
        bridge_data = fetch_bridge_data(get_web_config().bridge_url, get_web_config().http_timeout)
    except Exception as exc:  # noqa: BLE001
        bridge_error = str(exc)
        current_app.logger.warning("no se pudo leer el bridge: %s", exc)
        return DashboardData(bridge_data=None, bridge_error=bridge_error, summaries=[])

    ranges = build_period_ranges(periods)
    repo = get_repo()
    for period in ranges:
        summaries.append(
            calculate_period_summary(
                period,
                bridge_data.points,
                tariff_bands=repo.get_effective_tariff_bands(period["id"]),
                fixed_charges=repo.get_effective_fixed_charges(period["id"]),
                tax_rules=repo.get_effective_tax_rules(period["id"]),
            )
        )

    return DashboardData(bridge_data=bridge_data, bridge_error=bridge_error, summaries=summaries)


def build_consumption_comparison_data(summaries: list[dict[str, Any]]) -> ConsumptionComparisonData:
    if not summaries:
        return ConsumptionComparisonData(max_kwh=0.0, items=[])

    ordered = sorted(summaries, key=lambda item: item["period"]["effective_start"])
    max_kwh = max(
        max(
            float(summary.get("inverter_consumption_kwh") or 0.0),
            float(summary.get("utility_consumption_kwh") or 0.0),
            float(summary.get("inverter_load_kwh") or 0.0),
            float(summary.get("solar_pv_kwh") or 0.0),
        )
        for summary in ordered
    )
    if max_kwh <= 0:
        max_kwh = 1.0

    items: list[dict[str, Any]] = []
    for summary in ordered:
        inverter_kwh = float(summary.get("inverter_consumption_kwh") or 0.0)
        utility_value = summary.get("utility_consumption_kwh")
        utility_kwh = float(utility_value) if utility_value is not None else None
        difference_kwh = summary.get("consumption_difference_kwh")
        items.append(
            {
                "period_id": summary["period"]["id"],
                "name": summary["period"]["name"],
                "axis_label": format_period_axis_label(summary["period"]),
                "full_label": (
                    f"{summary['period']['name']} ({summary['period']['effective_start']} a {summary['period']['effective_end']})"
                ),
                "inverter_kwh": inverter_kwh,
                "utility_kwh": utility_kwh,
                "load_kwh": float(summary.get("inverter_load_kwh") or 0.0),
                "solar_pv_kwh": float(summary.get("solar_pv_kwh") or 0.0),
                "billing_kwh": float(summary.get("billing_consumption_kwh") or 0.0),
                "difference_kwh": difference_kwh,
                "difference_percent": summary.get("consumption_difference_percent"),
                "source": summary.get("consumption_source"),
                "inverter_pct": round((inverter_kwh / max_kwh) * 100, 4),
                "utility_pct": round(((utility_kwh or 0.0) / max_kwh) * 100, 4) if utility_kwh is not None else None,
            }
        )

    return ConsumptionComparisonData(max_kwh=max_kwh, items=items)


def build_period_consumption_comparison(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if summary is None:
        return None

    inverter_kwh = float(summary.get("inverter_consumption_kwh") or 0.0)
    utility_value = summary.get("utility_consumption_kwh")
    utility_kwh = float(utility_value) if utility_value is not None else None
    max_kwh = max(inverter_kwh, utility_kwh or 0.0, 1.0)
    return {
        "inverter_kwh": inverter_kwh,
        "utility_kwh": utility_kwh,
        "billing_kwh": float(summary.get("billing_consumption_kwh") or 0.0),
        "difference_kwh": summary.get("consumption_difference_kwh"),
        "difference_percent": summary.get("consumption_difference_percent"),
        "source": summary.get("consumption_source"),
        "inverter_pct": round((inverter_kwh / max_kwh) * 100, 4),
        "utility_pct": round(((utility_kwh or 0.0) / max_kwh) * 100, 4) if utility_kwh is not None else None,
    }


def build_consumption_chart(comparison_data: ConsumptionComparisonData) -> ToggleChartData | None:
    if not comparison_data.items:
        return None

    return build_toggle_chart(
        title="Consumo por periodo",
        subtitle="Compara red, compania, consumo total load y generacion solar PV en cada periodo.",
        labels=[str(item["axis_label"]) for item in comparison_data.items],
        full_labels=[str(item["full_label"]) for item in comparison_data.items],
        datasets=[
            {
                "label": "Load",
                "color": "#2563eb",
                "fill": "rgba(37, 99, 235, 0.18)",
                "values": [float(item["load_kwh"]) for item in comparison_data.items],
            },
            {
                "label": "Red",
                "color": "#ef4444",
                "fill": "rgba(239, 68, 68, 0.18)",
                "values": [float(item["inverter_kwh"]) for item in comparison_data.items],
            },
            {
                "label": "Compania",
                "color": "#14b8a6",
                "fill": "rgba(20, 184, 166, 0.18)",
                "values": [item["utility_kwh"] for item in comparison_data.items],
            },
            {
                "label": "Solar PV",
                "color": "#facc15",
                "fill": "rgba(250, 204, 21, 0.18)",
                "values": [float(item["solar_pv_kwh"]) for item in comparison_data.items],
            },
        ],
        value_kind="kwh",
    )


def build_period_daily_cost_chart(summary: dict[str, Any] | None) -> ToggleChartData | None:
    if summary is None:
        return None

    rows = summary.get("daily_energy_cost_breakdown") or []
    if not rows:
        return None

    return build_toggle_chart(
        title="Costo diario de energia",
        subtitle=str(summary.get("daily_cost_note") or "Visualiza el costo diario calculado de la energia."),
        labels=[format_day_axis_label(str(item["date"])) for item in rows],
        full_labels=[str(item["date"]) for item in rows],
        datasets=[
            {
                "label": "Costo energia",
                "color": "#16a34a",
                "fill": "rgba(22, 163, 74, 0.18)",
                "values": [float(item.get("energy_cost") or 0.0) for item in rows],
            }
        ],
        value_kind="money",
    )


def build_period_daily_energy_chart(summary: dict[str, Any] | None) -> ToggleChartData | None:
    if summary is None:
        return None

    rows = summary.get("daily_energy_cost_breakdown") or []
    if not rows:
        return None

    return build_toggle_chart(
        title="Red y Solar PV por dia",
        subtitle="Compara el consumo diario de red del inversor y la generacion solar del periodo.",
        labels=[format_day_axis_label(str(item["date"])) for item in rows],
        full_labels=[str(item["date"]) for item in rows],
        datasets=[
            {
                "label": "Red inversor",
                "color": "#ef4444",
                "fill": "rgba(239, 68, 68, 0.18)",
                "values": [float(item.get("inverter_grid_kwh") or 0.0) for item in rows],
            },
            {
                "label": "Solar PV",
                "color": "#facc15",
                "fill": "rgba(250, 204, 21, 0.18)",
                "values": [float(item.get("solar_pv_kwh") or 0.0) for item in rows],
            },
        ],
        value_kind="kwh",
    )


def build_costs_chart(summaries: list[dict[str, Any]]) -> ToggleChartData | None:
    if not summaries:
        return None

    ordered = sorted(summaries, key=lambda item: item["period"]["effective_start"])
    return build_toggle_chart(
        title="Costos por periodo",
        subtitle="Visualiza energia electrica, total del servicio y total final de la factura.",
        labels=[format_period_axis_label(summary["period"]) for summary in ordered],
        full_labels=[
            f"{summary['period']['name']} ({summary['period']['effective_start']} a {summary['period']['effective_end']})"
            for summary in ordered
        ],
        datasets=[
            {
                "label": "Total factura",
                "color": "#2563eb",
                "fill": "rgba(37, 99, 235, 0.18)",
                "values": [float(summary.get("total") or 0.0) for summary in ordered],
            },
            {
                "label": "Total servicio",
                "color": "#facc15",
                "fill": "rgba(250, 204, 21, 0.18)",
                "values": [float(summary.get("service_total") or 0.0) for summary in ordered],
            },
            {
                "label": "Energia electrica",
                "color": "#ef4444",
                "fill": "rgba(239, 68, 68, 0.18)",
                "values": [float(summary.get("energy_cost") or 0.0) for summary in ordered],
            },
        ],
        value_kind="money",
    )


def build_tariff_price_chart(summaries: list[dict[str, Any]]) -> ToggleChartData | None:
    if not summaries:
        return None

    ordered = sorted(summaries, key=lambda item: item["period"]["effective_start"])
    band_catalog: dict[str, dict[str, Any]] = {}
    for summary in ordered:
        for item in summary.get("energy_breakdown", []):
            key = tariff_band_key(item)
            band_catalog.setdefault(
                key,
                {
                    "key": key,
                    "label": str(item.get("label") or "Franja"),
                    "order": (
                        float(item.get("from_kwh") or 0.0),
                        float(item["to_kwh"]) if item.get("to_kwh") is not None else float("inf"),
                    ),
                },
            )

    if not band_catalog:
        return None

    datasets: list[dict[str, Any]] = []
    band_lookup_by_period = [
        {tariff_band_key(item): item for item in summary.get("energy_breakdown", [])}
        for summary in ordered
    ]
    for index, band in enumerate(sorted(band_catalog.values(), key=lambda item: item["order"])):
        color, fill = pick_series_colors(index)
        datasets.append(
            {
                "label": band["label"],
                "color": color,
                "fill": fill,
                "values": [
                    (
                        float(period_lookup[band["key"]]["price_per_kwh"])
                        if band["key"] in period_lookup
                        else None
                    )
                    for period_lookup in band_lookup_by_period
                ],
            }
        )

    return build_toggle_chart(
        title="Precio por franja",
        subtitle="Sigue el valor del kWh configurado en cada tramo de consumo para cada periodo.",
        labels=[format_period_axis_label(summary["period"]) for summary in ordered],
        full_labels=[
            f"{summary['period']['name']} ({summary['period']['effective_start']} a {summary['period']['effective_end']})"
            for summary in ordered
        ],
        datasets=datasets,
        value_kind="money_rate",
        minimum_padding_ratio=0.5,
    )


def build_fixed_charge_chart(summaries: list[dict[str, Any]]) -> ToggleChartData | None:
    if not summaries:
        return None

    ordered = sorted(summaries, key=lambda item: item["period"]["effective_start"])
    charge_catalog: dict[str, dict[str, Any]] = {}
    for summary in ordered:
        for item in summary.get("fixed_breakdown", []):
            if not item.get("show_on_dashboard"):
                continue
            key = fixed_charge_key(item)
            charge_catalog.setdefault(
                key,
                {
                    "key": key,
                    "label": str(item.get("name") or "Cargo fijo"),
                    "order": (int(item.get("position") or 0), str(item.get("name") or "")),
                },
            )

    if not charge_catalog:
        return None

    period_lookup = [
        {
            fixed_charge_key(item): item
            for item in summary.get("fixed_breakdown", [])
            if item.get("show_on_dashboard")
        }
        for summary in ordered
    ]
    datasets: list[dict[str, Any]] = []
    for index, charge in enumerate(sorted(charge_catalog.values(), key=lambda item: item["order"])):
        color, fill = pick_series_colors(index)
        datasets.append(
            {
                "label": charge["label"],
                "color": color,
                "fill": fill,
                "values": [
                    (
                        float(period_items[charge["key"]]["amount"])
                        if charge["key"] in period_items
                        else None
                    )
                    for period_items in period_lookup
                ],
            }
        )

    return build_toggle_chart(
        title="Cargos fijos visibles",
        subtitle="Muestra los importes fijos marcados para comparar en el dashboard.",
        labels=[format_period_axis_label(summary["period"]) for summary in ordered],
        full_labels=[
            f"{summary['period']['name']} ({summary['period']['effective_start']} a {summary['period']['effective_end']})"
            for summary in ordered
        ],
        datasets=datasets,
        value_kind="money",
        minimum_padding_ratio=0.5,
    )


def build_yearly_change_summary(summaries: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not summaries:
        return None

    ordered = sorted(summaries, key=lambda item: item["period"]["effective_start"])
    latest = ordered[-1]
    latest_start = date.fromisoformat(str(latest["period"]["effective_start"]))
    candidates = [
        summary
        for summary in ordered
        if 0 <= (latest_start - date.fromisoformat(str(summary["period"]["effective_start"]))).days <= 365
    ]
    if len(candidates) < 2:
        return None

    baseline = candidates[0]
    current_energy_rate = calculate_average_tariff_rate(latest)
    baseline_energy_rate = calculate_average_tariff_rate(baseline)
    current_fixed_total = round(sum(float(item.get("amount") or 0.0) for item in latest.get("fixed_breakdown", [])), 6)
    baseline_fixed_total = round(sum(float(item.get("amount") or 0.0) for item in baseline.get("fixed_breakdown", [])), 6)

    return {
        "current_period_name": latest["period"]["name"],
        "baseline_period_name": baseline["period"]["name"],
        "energy_rate_current": current_energy_rate,
        "energy_rate_baseline": baseline_energy_rate,
        "energy_rate_change_percent": calculate_change_percent(current_energy_rate, baseline_energy_rate),
        "fixed_total_current": current_fixed_total,
        "fixed_total_baseline": baseline_fixed_total,
        "fixed_total_change_percent": calculate_change_percent(current_fixed_total, baseline_fixed_total),
    }


def build_toggle_chart(
    *,
    title: str,
    subtitle: str,
    labels: list[str],
    full_labels: list[str],
    datasets: list[dict[str, Any]],
    value_kind: str,
    minimum_padding_ratio: float | None = None,
) -> ToggleChartData | None:
    if not labels:
        return None

    normalized_datasets: list[dict[str, Any]] = []
    max_value = 0.0
    observed_values: list[float] = []
    for dataset in datasets:
        values: list[float | None] = []
        for value in dataset["values"]:
            if value is None:
                values.append(None)
                continue
            numeric_value = float(value)
            values.append(numeric_value)
            max_value = max(max_value, numeric_value)
            observed_values.append(numeric_value)
        normalized_datasets.append({**dataset, "values": values})

    min_value = calculate_chart_minimum(observed_values, minimum_padding_ratio)
    if max_value <= 0:
        max_value = 1.0
    if max_value <= min_value:
        max_value = min_value + (abs(min_value) * 0.5 or 1.0)

    return ToggleChartData(
        title=title,
        subtitle=subtitle,
        legend=[{"label": str(dataset["label"]), "color": str(dataset["color"])} for dataset in normalized_datasets],
        config={
            "labels": labels,
            "full_labels": full_labels,
            "datasets": normalized_datasets,
            "min_value": min_value,
            "max_value": max_value,
            "value_kind": value_kind,
            "default_mode": "area",
        },
    )


def pick_series_colors(index: int) -> tuple[str, str]:
    return SERIES_COLORS[index % len(SERIES_COLORS)]


def tariff_band_key(item: dict[str, Any]) -> str:
    end_value = "open" if item.get("to_kwh") is None else f"{float(item['to_kwh']):.6f}"
    return "|".join(
        [
            normalize_reference_key(str(item.get("label") or "")),
            f"{float(item.get('from_kwh') or 0.0):.6f}",
            end_value,
        ]
    )


def fixed_charge_key(item: dict[str, Any]) -> str:
    return "|".join(
        [
            normalize_reference_key(str(item.get("alias") or item.get("name") or "cargo_fijo")),
            str(item.get("section") or "service"),
        ]
    )


def calculate_average_tariff_rate(summary: dict[str, Any]) -> float:
    prices = [float(item.get("price_per_kwh") or 0.0) for item in summary.get("energy_breakdown", [])]
    if not prices:
        return 0.0
    return round(sum(prices) / len(prices), 6)


def calculate_change_percent(current_value: float, baseline_value: float) -> float | None:
    if baseline_value <= 0:
        return None
    return round(((current_value - baseline_value) / baseline_value) * 100, 4)


def calculate_chart_minimum(observed_values: list[float], minimum_padding_ratio: float | None) -> float:
    if minimum_padding_ratio is None or not observed_values:
        return 0.0
    positive_values = [value for value in observed_values if value > 0]
    if not positive_values:
        return 0.0
    minimum_value = min(positive_values)
    padded = minimum_value - (minimum_value * minimum_padding_ratio)
    return round(max(0.0, padded), 6)


def render_bar_chart_svg(
    *,
    labels: list[str],
    full_labels: list[str],
    datasets: list[dict[str, Any]],
    min_value: float,
    max_value: float,
    value_kind: str,
) -> Markup:
    width = 1100
    height = 600
    left = 72
    right = 24
    top = 28
    bottom = 82
    plot_width = width - left - right
    plot_height = height - top - bottom
    baseline_y = top + plot_height
    scale_span = max(max_value - min_value, 1e-9)
    group_count = max(len(labels), 1)
    group_width = plot_width / group_count
    visible_datasets = [dataset for dataset in datasets if any(value is not None for value in dataset["values"])]
    series_count = max(len(visible_datasets), 1)
    usable_group_width = min(group_width * 0.76, 80.0)
    bar_width = max(min(usable_group_width / series_count, 26.0), 8.0)
    series_offset = (usable_group_width - (bar_width * series_count)) / 2

    y_axis_markup: list[str] = []
    for step in range(6):
        ratio = step / 5
        y = baseline_y - (plot_height * ratio)
        value = min_value + (scale_span * ratio)
        y_axis_markup.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{width - right}" y2="{y:.2f}" stroke="rgba(31,42,46,0.10)" stroke-width="1" />'
        )
        y_axis_markup.append(
            f'<text x="{left - 12}" y="{y + 4:.2f}" text-anchor="end" fill="#59676b" font-size="11">{escape(format_chart_tick(value, value_kind))}</text>'
        )

    x_axis_markup: list[str] = []
    bars_markup: list[str] = []
    for index, label in enumerate(labels):
        group_left = left + (group_width * index) + ((group_width - usable_group_width) / 2)
        label_x = left + (group_width * index) + (group_width / 2)
        x_axis_markup.append(
            f'<text x="{label_x:.2f}" y="{height - 18}" text-anchor="middle" fill="#59676b" font-size="11">{escape(label)}</text>'
        )
        for dataset_index, dataset in enumerate(visible_datasets):
            value = dataset["values"][index]
            if value is None:
                continue
            normalized_value = max(0.0, min(1.0, (float(value) - min_value) / scale_span))
            bar_height = plot_height * normalized_value
            x = group_left + series_offset + (dataset_index * bar_width)
            y = baseline_y - bar_height
            tooltip = escape(
                f"{dataset['label']} - {full_labels[index]}: {format_chart_detail_value(float(value), value_kind)}"
            )
            bars_markup.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width - 2:.2f}" height="{bar_height:.2f}" rx="8" fill="{dataset["color"]}"><title>{tooltip}</title></rect>'
            )

    svg = (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape("Grafico de barras")}">'
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="22" fill="#ffffff" />'
        f'{"".join(y_axis_markup)}'
        f'<line x1="{left}" y1="{baseline_y:.2f}" x2="{width - right}" y2="{baseline_y:.2f}" stroke="rgba(31,42,46,0.16)" stroke-width="1.5" />'
        f'{"".join(bars_markup)}'
        f'{"".join(x_axis_markup)}'
        "</svg>"
    )
    return Markup(svg)


def render_area_chart_svg(
    *,
    labels: list[str],
    full_labels: list[str],
    datasets: list[dict[str, Any]],
    min_value: float,
    max_value: float,
    value_kind: str,
) -> Markup:
    width = 1100
    height = 600
    left = 72
    right = 24
    top = 28
    bottom = 82
    plot_width = width - left - right
    plot_height = height - top - bottom
    baseline_y = top + plot_height
    scale_span = max(max_value - min_value, 1e-9)
    x_positions = build_line_x_positions(len(labels), left, plot_width)

    grid_markup: list[str] = []
    for step in range(6):
        ratio = step / 5
        y = baseline_y - (plot_height * ratio)
        value = min_value + (scale_span * ratio)
        grid_markup.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{width - right}" y2="{y:.2f}" stroke="rgba(255,255,255,0.10)" stroke-width="1" />'
        )
        grid_markup.append(
            f'<text x="{left - 12}" y="{y + 4:.2f}" text-anchor="end" fill="rgba(255,255,255,0.72)" font-size="11">{escape(format_chart_tick(value, value_kind))}</text>'
        )

    x_axis_markup: list[str] = []
    for index, label in enumerate(labels):
        x_axis_markup.append(
            f'<text x="{x_positions[index]:.2f}" y="{height - 18}" text-anchor="middle" fill="rgba(255,255,255,0.72)" font-size="11">{escape(label)}</text>'
        )

    series_markup: list[str] = []
    for dataset in datasets:
        segments = build_series_segments(
            x_positions=x_positions,
            values=dataset["values"],
            baseline_y=baseline_y,
            top=top,
            plot_height=plot_height,
            min_value=min_value,
            max_value=max_value,
        )
        for segment in segments:
            if len(segment) == 1:
                x, y, value, label_index = segment[0]
                tooltip = escape(
                    f"{dataset['label']} - {full_labels[label_index]}: {format_chart_detail_value(value, value_kind)}"
                )
                series_markup.append(
                    f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4.5" fill="{dataset["color"]}"><title>{tooltip}</title></circle>'
                )
                continue

            area_path = build_area_path(segment, baseline_y)
            line_path = build_line_path(segment)
            series_markup.append(
                f'<path d="{area_path}" fill="{dataset["fill"]}" stroke="none" />'
            )
            series_markup.append(
                f'<path d="{line_path}" fill="none" stroke="{dataset["color"]}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round" />'
            )
            for x, y, value, label_index in segment:
                tooltip = escape(
                    f"{dataset['label']} - {full_labels[label_index]}: {format_chart_detail_value(value, value_kind)}"
                )
                series_markup.append(
                    f'<circle cx="{x:.2f}" cy="{y:.2f}" r="3.4" fill="{dataset["color"]}"><title>{tooltip}</title></circle>'
                )

    svg = (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape("Grafico de area")}">'
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="22" fill="#1d1f22" />'
        f'{"".join(grid_markup)}'
        f'<line x1="{left}" y1="{baseline_y:.2f}" x2="{width - right}" y2="{baseline_y:.2f}" stroke="rgba(255,255,255,0.12)" stroke-width="1.5" />'
        f'{"".join(series_markup)}'
        f'{"".join(x_axis_markup)}'
        "</svg>"
    )
    return Markup(svg)


def build_line_x_positions(count: int, left: float, plot_width: float) -> list[float]:
    if count <= 1:
        return [left + (plot_width / 2)]
    step = plot_width / (count - 1)
    return [left + (step * index) for index in range(count)]


def build_series_segments(
    *,
    x_positions: list[float],
    values: list[float | None],
    baseline_y: float,
    top: float,
    plot_height: float,
    min_value: float,
    max_value: float,
) -> list[list[tuple[float, float, float, int]]]:
    segments: list[list[tuple[float, float, float, int]]] = []
    current_segment: list[tuple[float, float, float, int]] = []
    scale_span = max(max_value - min_value, 1e-9)

    for index, value in enumerate(values):
        if value is None:
            if current_segment:
                segments.append(current_segment)
                current_segment = []
            continue

        normalized_value = max(0.0, min(1.0, (float(value) - min_value) / scale_span))
        y = baseline_y - (plot_height * normalized_value)
        y = max(top, y)
        current_segment.append((x_positions[index], y, float(value), index))

    if current_segment:
        segments.append(current_segment)
    return segments


def build_line_path(segment: list[tuple[float, float, float, int]]) -> str:
    x0, y0, _, _ = segment[0]
    commands = [f"M {x0:.2f} {y0:.2f}"]
    for x, y, _, _ in segment[1:]:
        commands.append(f"L {x:.2f} {y:.2f}")
    return " ".join(commands)


def build_area_path(segment: list[tuple[float, float, float, int]], baseline_y: float) -> str:
    x0, y0, _, _ = segment[0]
    commands = [f"M {x0:.2f} {baseline_y:.2f}", f"L {x0:.2f} {y0:.2f}"]
    for x, y, _, _ in segment[1:]:
        commands.append(f"L {x:.2f} {y:.2f}")
    x_last, _, _, _ = segment[-1]
    commands.append(f"L {x_last:.2f} {baseline_y:.2f}")
    commands.append("Z")
    return " ".join(commands)


def format_chart_detail_value(value: float, value_kind: str) -> str:
    if value_kind == "money":
        return format_money_value(value)
    if value_kind == "money_rate":
        return f"{format_money_value(value)}/kWh"
    return f"{format_kwh_value(value)} kWh"


def handle_band_save(*, scope: str, billing_period_id: int | None) -> None:
    repo = get_repo()
    band_id = parse_optional_int(request.form.get("band_id"))
    position = parse_int(request.form.get("position"), default=1)
    label = request.form.get("label", "").strip()
    from_kwh = parse_float(request.form.get("from_kwh"), default=0.0)
    to_kwh = parse_optional_float(request.form.get("to_kwh"))
    price_per_kwh = parse_float(request.form.get("price_per_kwh"), default=0.0)

    if to_kwh is not None and to_kwh <= from_kwh:
        raise ValueError("El valor hasta kWh debe ser mayor que desde kWh.")

    repo.save_tariff_band(
        band_id=band_id,
        scope=scope,
        billing_period_id=billing_period_id,
        position=position,
        label=label,
        from_kwh=from_kwh,
        to_kwh=to_kwh,
        price_per_kwh=price_per_kwh,
    )


def handle_charge_save(*, scope: str, billing_period_id: int | None) -> None:
    repo = get_repo()
    rule_id = parse_optional_int(request.form.get("rule_id"))
    kind = request.form.get("kind", "").strip()
    section = request.form.get("section", "").strip() or ("service" if kind == "fixed" else "tax")
    name = request.form.get("name", "").strip()
    alias = request.form.get("alias", "").strip()
    position = parse_int(request.form.get("position"), default=1)
    enabled = request.form.get("enabled") == "on"
    show_on_dashboard = request.form.get("show_on_dashboard") == "on"

    if kind not in {"tax", "fixed"}:
        raise ValueError("Tipo de regla invalido.")
    if section not in CHARGE_SECTIONS:
        raise ValueError("Seccion invalida.")
    if not name:
        raise ValueError("El nombre es obligatorio.")
    if alias and not REFERENCE_ALIAS_RE.fullmatch(alias):
        raise ValueError("El alias solo puede tener letras, numeros y guion bajo, y debe empezar con una letra o _.")
    if alias and normalize_reference_key(alias) in set(VARIABLE_ALIASES.values()):
        raise ValueError("Ese alias esta reservado por el sistema. Usa otro nombre.")

    if kind == "tax":
        expression = request.form.get("expression", "").strip()
        amount = None
        show_on_dashboard = False
        if not expression:
            raise ValueError("La expresion del impuesto es obligatoria.")
    else:
        expression = None
        amount = parse_float(request.form.get("amount"), default=0.0)

    repo.save_charge_rule(
        rule_id=rule_id,
        scope=scope,
        billing_period_id=billing_period_id,
        position=position,
        kind=kind,
        section=section,
        name=name,
        alias=alias or None,
        expression=expression,
        amount=amount,
        show_on_dashboard=show_on_dashboard,
        enabled=enabled,
    )


def parse_float(value: str | None, *, default: float = 0.0) -> float:
    if value is None or not value.strip():
        return default
    return float(value.replace(",", "."))


def parse_optional_float(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    return float(value.replace(",", "."))


def parse_int(value: str | None, *, default: int = 0) -> int:
    if value is None or not value.strip():
        return default
    return int(value)


def parse_optional_int(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    return int(value)


def resolve_billing_source(
    requested_billing_source: str | None,
    *,
    utility_measured_kwh: float | None,
    existing_period: dict[str, Any] | None,
) -> str:
    source = (requested_billing_source or "").strip()
    if source not in {"utility", "inverter"}:
        source = str(existing_period.get("billing_source") or "").strip() if existing_period is not None else ""
    if source not in {"utility", "inverter"}:
        source = "utility" if utility_measured_kwh is not None else "inverter"
    if source == "utility" and utility_measured_kwh is None:
        return "inverter"
    return source

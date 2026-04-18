from __future__ import annotations

import ast
import json
import math
import re
import unicodedata
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from .sections import (
    DEFAULT_SERVICE_SECTION_CODE,
    DEFAULT_TAX_SECTION_CODE,
    SYSTEM_SECTIONS,
)


VARIABLE_ALIASES = {
    "total_factura": "total_factura",
    "total de la factura": "total_factura",
    "factura": "total_factura",
    "costo_energia": "costo_energia",
    "costo de la energia": "costo_energia",
    "costo de la luz": "costo_energia",
    "energia": "costo_energia",
    "energia_electrica": "energia_electrica",
    "energia electrica": "energia_electrica",
    "cargos_fijos": "cargos_fijos",
    "cargos fijos": "cargos_fijos",
    "cargos_fijos_servicio": "cargos_fijos_servicio",
    "cargos fijos servicio": "cargos_fijos_servicio",
    "cargos_fijos_impuestos": "cargos_fijos_impuestos",
    "cargos fijos impuestos": "cargos_fijos_impuestos",
    "subtotal": "subtotal",
    "impuestos_acumulados": "impuestos_acumulados",
    "impuestos acumulados": "impuestos_acumulados",
    "conceptos_calculados_acumulados": "conceptos_calculados_acumulados",
    "conceptos calculados acumulados": "conceptos_calculados_acumulados",
    "servicio_energia": "servicio_energia",
    "servicio de energia": "servicio_energia",
    "total_servicio_energia": "total_servicio_energia",
    "total servicio de energia": "total_servicio_energia",
    "costo_total_energia": "total_servicio_energia",
    "costo total de la energia": "total_servicio_energia",
    "iva_otros_conceptos": "iva_otros_conceptos",
    "iva y otros conceptos": "iva_otros_conceptos",
    "otros_conceptos": "iva_otros_conceptos",
    "otros conceptos": "iva_otros_conceptos",
    "consumo_kwh": "consumo_kwh",
    "consumo": "consumo_kwh",
    "consumo_inversor_kwh": "consumo_inversor_kwh",
    "consumo inversor kwh": "consumo_inversor_kwh",
    "consumo_inversor": "consumo_inversor_kwh",
    "inversor_kwh": "consumo_inversor_kwh",
    "consumo_compania_kwh": "consumo_compania_kwh",
    "consumo compania kwh": "consumo_compania_kwh",
    "consumo_compania": "consumo_compania_kwh",
    "medido_compania_kwh": "consumo_compania_kwh",
    "medido compania kwh": "consumo_compania_kwh",
}

PERCENT_EXPRESSION_RE = re.compile(r"^\s*([0-9]+(?:[.,][0-9]+)?)\s*%\s*d(?:e|el)\s+(.+?)\s*$", re.IGNORECASE)
NUMBER_RE = re.compile(r"^\s*[0-9]+(?:[.,][0-9]+)?\s*$")


@dataclass(slots=True)
class BridgeData:
    status: dict[str, Any]
    points: list[dict[str, Any]]


def fetch_bridge_data(bridge_url: str, timeout: float) -> BridgeData:
    status = _fetch_json(f"{bridge_url}/health", timeout)
    points_payload = _fetch_json(f"{bridge_url}/totals/daily/points", timeout)
    return BridgeData(status=status, points=normalize_points_payload(points_payload))


def _fetch_json(url: str, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "solarcost-web/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def normalize_points_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get("points"), list):
        raw_points = payload["points"]
    else:
        raw_points = []
        for period in payload.get("periods", []):
            period_points = period.get("points", [])
            if isinstance(period_points, list):
                raw_points.extend(period_points)

    unique: dict[tuple[Any, Any], dict[str, Any]] = {}
    for point in raw_points:
        if not isinstance(point, dict):
            continue
        signature = (point.get("timestamp"), point.get("iso"))
        unique[signature] = point

    return sorted(unique.values(), key=lambda item: (item.get("timestamp") or 0, item.get("iso") or ""))


def build_period_ranges(periods: list[dict[str, Any]], *, today: date | None = None) -> list[dict[str, Any]]:
    current_day = today or date.today()
    ordered = sorted(periods, key=lambda item: item["starts_on"])
    ranges: list[dict[str, Any]] = []

    for index, period in enumerate(ordered):
        start = date.fromisoformat(period["starts_on"])
        if index + 1 < len(ordered):
            end = date.fromisoformat(ordered[index + 1]["starts_on"]) - timedelta(days=1)
        else:
            end = current_day
        if end < start:
            end = start
        ranges.append(
            {
                **period,
                "effective_start": start.isoformat(),
                "effective_end": end.isoformat(),
                "is_open": index == len(ordered) - 1,
            }
        )

    return list(reversed(ranges))


def normalize_sections(sections: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    section_map: dict[str, dict[str, Any]] = {}
    for system_section in SYSTEM_SECTIONS:
        section_map[system_section["code"]] = {
            **system_section,
            "id": None,
            "enabled": bool(system_section.get("enabled", True)),
        }

    for raw_section in sections or []:
        code = str(raw_section.get("code") or "").strip()
        if not code:
            continue
        existing = section_map.get(code, {})
        section_map[code] = {
            **existing,
            **raw_section,
            "code": code,
            "name": str(raw_section.get("name") or existing.get("name") or code),
            "position": int(raw_section.get("position") or existing.get("position") or 0),
            "is_system": bool(raw_section.get("is_system", existing.get("is_system", False))),
            "enabled": bool(raw_section.get("enabled", existing.get("enabled", True))),
        }

    return sorted(
        section_map.values(),
        key=lambda item: (int(item.get("position") or 0), str(item.get("name") or ""), str(item.get("code") or "")),
    )


def section_enabled(sections_by_code: dict[str, dict[str, Any]], code: str) -> bool:
    section = sections_by_code.get(code)
    if section is None:
        return True
    return bool(section.get("enabled", True))


def calculate_period_summary(
    period: dict[str, Any],
    points: list[dict[str, Any]],
    *,
    sections: list[dict[str, Any]],
    tariff_bands: list[dict[str, Any]],
    fixed_charges: list[dict[str, Any]],
    tax_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    start_date = date.fromisoformat(period["effective_start"])
    end_date = date.fromisoformat(period["effective_end"])
    period_points = [point for point in points if _point_date(point) is not None and start_date <= _point_date(point) <= end_date]
    observed_dates = {point_date for point in period_points if (point_date := _point_date(point)) is not None}
    coverage_end_date = end_date - timedelta(days=1) if period.get("is_open") else end_date
    coverage_dates = (
        list(_iter_day_range(start_date, coverage_end_date))
        if coverage_end_date >= start_date
        else []
    )
    missing_dates = [day.isoformat() for day in coverage_dates if day not in observed_dates]
    coverage_expected_day_count = len(coverage_dates)
    coverage_observed_day_count = len([day for day in observed_dates if day <= coverage_end_date])
    has_missing_days = bool(missing_dates)
    has_manual_inverter_data_issue = bool(period.get("has_inverter_data_issue"))
    has_inverter_issue = has_manual_inverter_data_issue or has_missing_days
    issue_parts: list[str] = []
    if has_manual_inverter_data_issue:
        issue_parts.append("Marcado manualmente")
    if has_missing_days:
        missing_day_label = "dia" if len(missing_dates) == 1 else "dias"
        issue_parts.append(f"Faltan {len(missing_dates)} {missing_day_label} sin medicion")
    inverter_issue_summary = " | ".join(issue_parts) if issue_parts else "Cobertura completa"

    inverter_consumption_kwh = round(sum(_point_grid_kwh(point) for point in period_points), 6)
    inverter_load_kwh = round(sum(_point_load_kwh(point) for point in period_points), 6)
    solar_pv_kwh = round(sum(_point_solar_pv_kwh(point) for point in period_points), 6)
    utility_consumption_kwh = parse_optional_number(period.get("utility_measured_kwh"))
    requested_billing_source = str(period.get("billing_source") or "").strip() or (
        "utility" if utility_consumption_kwh is not None else "inverter"
    )
    if requested_billing_source == "utility" and utility_consumption_kwh is not None:
        consumption_source = "utility"
    else:
        consumption_source = "inverter"
    consumption_difference_kwh = None
    consumption_difference_percent = None
    if utility_consumption_kwh is not None:
        consumption_difference_kwh = round(utility_consumption_kwh - inverter_consumption_kwh, 6)
        if inverter_consumption_kwh:
            consumption_difference_percent = round((consumption_difference_kwh / inverter_consumption_kwh) * 100, 6)

    normalized_sections = normalize_sections(sections)
    inverter_variant = calculate_cost_variant(
        billed_consumption_kwh=inverter_consumption_kwh,
        inverter_consumption_kwh=inverter_consumption_kwh,
        utility_consumption_kwh=utility_consumption_kwh,
        sections=normalized_sections,
        tariff_bands=tariff_bands,
        fixed_charges=fixed_charges,
        tax_rules=tax_rules,
    )
    utility_variant = (
        calculate_cost_variant(
            billed_consumption_kwh=round(utility_consumption_kwh, 6),
            inverter_consumption_kwh=inverter_consumption_kwh,
            utility_consumption_kwh=utility_consumption_kwh,
            sections=normalized_sections,
            tariff_bands=tariff_bands,
            fixed_charges=fixed_charges,
            tax_rules=tax_rules,
        )
        if utility_consumption_kwh is not None
        else None
    )
    load_variant = calculate_cost_variant(
        billed_consumption_kwh=inverter_load_kwh,
        inverter_consumption_kwh=inverter_consumption_kwh,
        utility_consumption_kwh=utility_consumption_kwh,
        sections=normalized_sections,
        tariff_bands=tariff_bands,
        fixed_charges=fixed_charges,
        tax_rules=tax_rules,
    )
    active_variant = utility_variant if consumption_source == "utility" and utility_variant is not None else inverter_variant
    alternate_variant = (
        inverter_variant if consumption_source == "utility" else utility_variant
    )
    billed_consumption_kwh = float(active_variant["consumption_kwh"])
    solar_savings_total = round(float(load_variant["total"]) - float(active_variant["total"]), 6)
    daily_energy_cost_breakdown, daily_cost_note = build_daily_energy_cost_breakdown(
        start_date=start_date,
        end_date=end_date,
        period_points=period_points,
        tariff_bands=tariff_bands,
        billed_consumption_kwh=billed_consumption_kwh,
        inverter_consumption_kwh=inverter_consumption_kwh,
        utility_consumption_kwh=utility_consumption_kwh,
        consumption_source=consumption_source,
        expected_energy_cost=float(active_variant["energy_cost"]),
    )

    return {
        "period": period,
        "consumption_kwh": billed_consumption_kwh,
        "inverter_consumption_kwh": inverter_consumption_kwh,
        "inverter_load_kwh": inverter_load_kwh,
        "solar_pv_kwh": solar_pv_kwh,
        "utility_consumption_kwh": utility_consumption_kwh,
        "billing_consumption_kwh": billed_consumption_kwh,
        "consumption_source": consumption_source,
        "requested_billing_source": requested_billing_source,
        "solar_savings_total": solar_savings_total,
        "consumption_difference_kwh": consumption_difference_kwh,
        "consumption_difference_percent": consumption_difference_percent,
        "billing_variants": {
            "inverter": {**inverter_variant, "source": "inverter", "label": "Red"},
            "utility": ({**utility_variant, "source": "utility", "label": "Compania"} if utility_variant is not None else None),
        },
        "selected_variant": {**active_variant, "source": consumption_source, "label": ("Compania" if consumption_source == "utility" else "Red")},
        "load_variant": {**load_variant, "source": "load", "label": "Carga total"},
        "alternate_variant": (
            {
                **alternate_variant,
                "source": ("inverter" if consumption_source == "utility" else "utility"),
                "label": ("Red" if consumption_source == "utility" else "Compania"),
            }
            if alternate_variant is not None
            else None
        ),
        "has_manual_inverter_data_issue": has_manual_inverter_data_issue,
        "has_missing_days": has_missing_days,
        "has_inverter_issue": has_inverter_issue,
        "missing_day_count": len(missing_dates),
        "missing_dates": missing_dates,
        "missing_dates_preview": missing_dates[:6],
        "coverage_expected_day_count": coverage_expected_day_count,
        "coverage_observed_day_count": coverage_observed_day_count,
        "inverter_issue_summary": inverter_issue_summary,
        "daily_points": period_points,
        "daily_energy_cost_breakdown": daily_energy_cost_breakdown,
        "daily_cost_note": daily_cost_note,
        "energy_breakdown": active_variant["energy_breakdown"],
        "fixed_breakdown": active_variant["fixed_breakdown"],
        "tax_breakdown": active_variant["formula_breakdown"],
        "formula_breakdown": active_variant["formula_breakdown"],
        "service_breakdown": active_variant["service_breakdown"],
        "other_concepts_breakdown": active_variant["other_concepts_breakdown"],
        "section_breakdowns": active_variant["section_breakdowns"],
        "section_totals": active_variant["section_totals"],
        "energy_cost": active_variant["energy_cost"],
        "fixed_total": active_variant["fixed_total"],
        "tax_total": active_variant["other_concepts_total"],
        "formula_total": active_variant["formula_total"],
        "service_total": active_variant["service_total"],
        "other_concepts_total": active_variant["other_concepts_total"],
        "extra_sections_total": active_variant["extra_sections_total"],
        "service_fixed_total": active_variant["service_fixed_total"],
        "other_fixed_total": active_variant["other_fixed_total"],
        "service_formula_total": active_variant["service_formula_total"],
        "other_formula_total": active_variant["other_formula_total"],
        "subtotal": active_variant["subtotal"],
        "total": active_variant["total"],
        "config_source": {
            "bands_scope": describe_scope_label("period" if _has_period_scope(tariff_bands) else "default"),
            "fixed_scope": describe_scope_label("period" if _has_period_scope(fixed_charges) else "default"),
            "tax_scope": describe_scope_label("period" if _has_period_scope(tax_rules) else "default"),
        },
    }


def build_daily_energy_cost_breakdown(
    *,
    start_date: date,
    end_date: date,
    period_points: list[dict[str, Any]],
    tariff_bands: list[dict[str, Any]],
    billed_consumption_kwh: float,
    inverter_consumption_kwh: float,
    utility_consumption_kwh: float | None,
    consumption_source: str,
    expected_energy_cost: float,
) -> tuple[list[dict[str, Any]], str]:
    days = _iter_day_range(start_date, end_date)
    if not days:
        return [], "Sin dias para calcular."

    points_by_date = {
        point_date: point
        for point in period_points
        if (point_date := _point_date(point)) is not None
    }

    daily_grid_values = [float(_point_grid_kwh(points_by_date.get(day, {}))) for day in days]
    day_count = len(days)

    if consumption_source == "utility" and utility_consumption_kwh is not None:
        if inverter_consumption_kwh > 0:
            billed_daily_values = [
                float(utility_consumption_kwh) * (daily_grid_kwh / float(inverter_consumption_kwh))
                for daily_grid_kwh in daily_grid_values
            ]
            note = "El consumo diario usado para calcular se prorratea segun el perfil diario del inversor."
        else:
            equal_share = float(utility_consumption_kwh) / float(day_count)
            billed_daily_values = [equal_share for _ in days]
            note = "El consumo diario usado para calcular se reparte en partes iguales porque no hay consumo diario del inversor."
    else:
        billed_daily_values = daily_grid_values
        note = "El costo diario usa el consumo de red informado por el inversor para cada dia."

    rows: list[dict[str, Any]] = []
    billed_so_far = 0.0

    for index, day in enumerate(days):
        point = points_by_date.get(day)
        if index == day_count - 1:
            billed_kwh = float(billed_consumption_kwh) - billed_so_far
        else:
            billed_kwh = billed_daily_values[index]
        billed_kwh = max(0.0, billed_kwh)

        previous_energy_cost = calculate_energy_cost(billed_so_far, tariff_bands)[1]
        billed_so_far += billed_kwh
        cumulative_energy_cost = calculate_energy_cost(billed_so_far, tariff_bands)[1]
        energy_cost = cumulative_energy_cost - previous_energy_cost
        rows.append(
            {
                "date": day.isoformat(),
                "has_measurement": point is not None,
                "inverter_grid_kwh": round(_point_grid_kwh(point or {}), 6),
                "billed_kwh": round(billed_kwh, 6),
                "load_kwh": round(_point_load_kwh(point or {}), 6),
                "solar_pv_kwh": round(_point_solar_pv_kwh(point or {}), 6),
                "energy_cost": round(energy_cost, 6),
                "cumulative_energy_cost": round(cumulative_energy_cost, 6),
            }
        )

    if expected_energy_cost <= 0:
        cumulative_cost = 0.0
        for row in rows:
            row["energy_cost"] = 0.0
            row["cumulative_energy_cost"] = cumulative_cost
        return rows, note

    if rows:
        energy_delta = round(float(expected_energy_cost) - float(rows[-1]["cumulative_energy_cost"]), 6)
        if energy_delta:
            rows[-1]["energy_cost"] = round(float(rows[-1]["energy_cost"]) + energy_delta, 6)
            rows[-1]["cumulative_energy_cost"] = round(float(expected_energy_cost), 6)

    return rows, note


def calculate_cost_variant(
    *,
    billed_consumption_kwh: float,
    inverter_consumption_kwh: float,
    utility_consumption_kwh: float | None,
    sections: list[dict[str, Any]],
    tariff_bands: list[dict[str, Any]],
    fixed_charges: list[dict[str, Any]],
    tax_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    section_list = normalize_sections(sections)
    sections_by_code = {
        str(section.get("code") or ""): section
        for section in section_list
    }
    raw_energy_breakdown, raw_energy_cost = calculate_energy_cost(billed_consumption_kwh, tariff_bands)
    energy_section_enabled = section_enabled(sections_by_code, DEFAULT_SERVICE_SECTION_CODE)
    energy_breakdown = [
        {
            **item,
            "subtotal": round(float(item.get("subtotal") or 0.0), 6) if energy_section_enabled else 0.0,
        }
        for item in raw_energy_breakdown
    ]
    energy_cost = round(raw_energy_cost, 6) if energy_section_enabled else 0.0
    fixed_breakdown, fixed_total = calculate_fixed_charges(fixed_charges, sections_by_code=sections_by_code)
    formula_breakdown, formulas_total = calculate_taxes(
        tax_rules,
        consumo_kwh=billed_consumption_kwh,
        consumo_inversor_kwh=inverter_consumption_kwh,
        consumo_compania_kwh=utility_consumption_kwh,
        costo_energia=energy_cost,
        cargos_fijos=fixed_total,
        fixed_charges=fixed_breakdown,
        sections_by_code=sections_by_code,
    )

    subtotal = round(energy_cost + fixed_total, 6)
    service_fixed_total = sum_breakdown_amounts(fixed_breakdown, section=DEFAULT_SERVICE_SECTION_CODE)
    other_fixed_total = sum_breakdown_amounts(fixed_breakdown, section=DEFAULT_TAX_SECTION_CODE)
    service_formula_total = sum_breakdown_amounts(formula_breakdown, section=DEFAULT_SERVICE_SECTION_CODE)
    other_formula_total = sum_breakdown_amounts(formula_breakdown, section=DEFAULT_TAX_SECTION_CODE)
    service_total = round(energy_cost + service_fixed_total + service_formula_total, 6)
    other_concepts_total = round(other_fixed_total + other_formula_total, 6)
    section_breakdowns = build_section_breakdowns(
        sections=section_list,
        energy_cost=energy_cost,
        fixed_breakdown=fixed_breakdown,
        formula_breakdown=formula_breakdown,
    )
    section_totals = [
        {
            "code": str(section["code"]),
            "name": str(section["name"]),
            "enabled": bool(section.get("enabled", True)),
            "is_system": bool(section.get("is_system", False)),
            "items": breakdown["items"],
            "total": breakdown["total"],
        }
        for section, breakdown in section_breakdowns
    ]
    total = round(sum(float(item["total"]) for item in section_totals), 6)
    extra_sections_total = round(
        total - service_total - other_concepts_total,
        6,
    )
    section_breakdown_map = {
        str(section["code"]): breakdown["items"]
        for section, breakdown in section_breakdowns
    }

    return {
        "consumption_kwh": billed_consumption_kwh,
        "energy_breakdown": energy_breakdown,
        "fixed_breakdown": fixed_breakdown,
        "formula_breakdown": formula_breakdown,
        "service_breakdown": section_breakdown_map.get(DEFAULT_SERVICE_SECTION_CODE, []),
        "other_concepts_breakdown": section_breakdown_map.get(DEFAULT_TAX_SECTION_CODE, []),
        "section_breakdowns": section_totals,
        "section_totals": section_totals,
        "energy_cost": energy_cost,
        "fixed_total": fixed_total,
        "formula_total": formulas_total,
        "service_total": service_total,
        "other_concepts_total": other_concepts_total,
        "extra_sections_total": extra_sections_total,
        "service_fixed_total": service_fixed_total,
        "other_fixed_total": other_fixed_total,
        "service_formula_total": service_formula_total,
        "other_formula_total": other_formula_total,
        "subtotal": subtotal,
        "total": total,
    }


def _has_period_scope(items: list[dict[str, Any]]) -> bool:
    return any(item.get("scope") == "period" for item in items)


def describe_scope_label(scope: str) -> str:
    return "Periodo" if scope == "period" else "Plantilla"


def calculate_energy_cost(consumption_kwh: float, tariff_bands: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], float]:
    breakdown: list[dict[str, Any]] = []
    total = 0.0

    for band in sorted(tariff_bands, key=lambda item: (float(item["from_kwh"]), item["position"], item["id"])):
        start = float(band["from_kwh"])
        end = float(band["to_kwh"]) if band.get("to_kwh") is not None else None
        if consumption_kwh <= start:
            used_kwh = 0.0
        else:
            upper = consumption_kwh if end is None else min(consumption_kwh, end)
            used_kwh = max(0.0, upper - start)
        subtotal = round(used_kwh * float(band["price_per_kwh"]), 6)
        total += subtotal
        breakdown.append(
            {
                "id": band["id"],
                "label": band.get("label") or _default_band_label(start, end),
                "from_kwh": start,
                "to_kwh": end,
                "used_kwh": round(used_kwh, 6),
                "price_per_kwh": float(band["price_per_kwh"]),
                "subtotal": subtotal,
            }
        )

    return breakdown, round(total, 6)


def _default_band_label(start: float, end: float | None) -> str:
    if end is None:
        return f"Desde {start:g} kWh"
    return f"{start:g} a {end:g} kWh"


def calculate_fixed_charges(
    fixed_charges: list[dict[str, Any]],
    *,
    sections_by_code: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], float]:
    breakdown: list[dict[str, Any]] = []
    total = 0.0

    for charge in fixed_charges:
        if not charge.get("enabled", 1):
            continue
        configured_amount = round(float(charge.get("amount") or 0.0), 6)
        section_code = charge_section(charge, default=DEFAULT_SERVICE_SECTION_CODE)
        amount = configured_amount if section_enabled(sections_by_code, section_code) else 0.0
        total += amount
        breakdown.append(
            {
                "id": charge["id"],
                "kind": "fixed",
                "name": charge["name"],
                "alias": charge.get("alias"),
                "section": section_code,
                "position": int(charge.get("position") or 0),
                "show_on_dashboard": bool(charge.get("show_on_dashboard", 0)),
                "configured_value": configured_amount,
                "amount": amount,
            }
        )

    return breakdown, round(total, 6)


def calculate_taxes(
    tax_rules: list[dict[str, Any]],
    *,
    consumo_kwh: float,
    consumo_inversor_kwh: float,
    consumo_compania_kwh: float | None,
    costo_energia: float,
    cargos_fijos: float,
    fixed_charges: list[dict[str, Any]] | None = None,
    sections_by_code: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], float]:
    subtotal = round(costo_energia + cargos_fijos, 6)
    formulas_total = 0.0
    breakdown: list[dict[str, Any]] = []
    reference_context = build_named_reference_context(fixed_charges or [])
    section_states = sections_by_code or {}
    section_totals = {
        str(code): 0.0
        for code in section_states
    }
    section_totals.setdefault(DEFAULT_SERVICE_SECTION_CODE, 0.0)
    section_totals.setdefault(DEFAULT_TAX_SECTION_CODE, 0.0)
    section_totals[DEFAULT_SERVICE_SECTION_CODE] = round(costo_energia, 6)
    for item in fixed_charges or []:
        section_code = charge_section(item, default=DEFAULT_SERVICE_SECTION_CODE)
        section_totals[section_code] = round(
            float(section_totals.get(section_code, 0.0)) + float(item.get("amount") or 0.0),
            6,
        )
    fixed_service_total = sum_breakdown_amounts(fixed_charges or [], section=DEFAULT_SERVICE_SECTION_CODE)
    fixed_other_total = sum_breakdown_amounts(fixed_charges or [], section=DEFAULT_TAX_SECTION_CODE)

    for rule in tax_rules:
        if not rule.get("enabled", 1):
            continue
        section = charge_section(rule, default=DEFAULT_TAX_SECTION_CODE)
        current_service_total = round(float(section_totals.get(DEFAULT_SERVICE_SECTION_CODE, 0.0)), 6)
        current_other_total = round(float(section_totals.get(DEFAULT_TAX_SECTION_CODE, 0.0)), 6)

        context = {
            "consumo_kwh": consumo_kwh,
            "consumo_inversor_kwh": consumo_inversor_kwh,
            "consumo_compania_kwh": consumo_compania_kwh or 0.0,
            "costo_energia": costo_energia,
            "energia_electrica": costo_energia,
            "cargos_fijos": cargos_fijos,
            "cargos_fijos_servicio": fixed_service_total,
            "cargos_fijos_impuestos": fixed_other_total,
            "subtotal": subtotal,
            "impuestos_acumulados": formulas_total,
            "conceptos_calculados_acumulados": formulas_total,
            "servicio_energia": current_service_total,
            "total_servicio_energia": current_service_total,
            "iva_otros_conceptos": current_other_total,
            "otros_conceptos": current_other_total,
            "total_factura": round(sum(float(value) for value in section_totals.values()), 6),
        }
        context.update(reference_context)
        amount = evaluate_tax_expression(str(rule.get("expression") or ""), context)
        if not section_enabled(section_states, section):
            amount = 0.0
        formulas_total = round(formulas_total + amount, 6)
        section_totals[section] = round(float(section_totals.get(section, 0.0)) + amount, 6)
        breakdown.append(
            {
                "id": rule["id"],
                "kind": "formula",
                "name": rule["name"],
                "alias": rule.get("alias"),
                "section": section,
                "position": int(rule.get("position") or 0),
                "expression": rule.get("expression") or "",
                "configured_value": rule.get("expression") or "",
                "amount": round(amount, 6),
                "base_snapshot": context,
            }
        )
        register_named_amount(reference_context, rule.get("alias"), rule.get("name"), amount)

    return breakdown, round(formulas_total, 6)


def parse_optional_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def charge_section(item: dict[str, Any], *, default: str) -> str:
    section = item.get("section")
    if isinstance(section, str) and section.strip():
        return str(section).strip()
    kind = str(item.get("kind") or "")
    if kind == "fixed":
        return DEFAULT_SERVICE_SECTION_CODE
    if kind in {"tax", "formula"}:
        return DEFAULT_TAX_SECTION_CODE
    expression = item.get("expression")
    if isinstance(expression, str) and expression.strip():
        return DEFAULT_TAX_SECTION_CODE
    return default


def sum_breakdown_amounts(items: list[dict[str, Any]], *, section: str | None = None) -> float:
    total = 0.0
    for item in items:
        if section is not None and charge_section(item, default=DEFAULT_SERVICE_SECTION_CODE) != section:
            continue
        total += float(item.get("amount") or 0.0)
    return round(total, 6)


def build_section_breakdowns(
    *,
    sections: list[dict[str, Any]],
    energy_cost: float,
    fixed_breakdown: list[dict[str, Any]],
    formula_breakdown: list[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {
        str(section.get("code") or ""): []
        for section in sections
    }
    grouped.setdefault(DEFAULT_SERVICE_SECTION_CODE, [])
    energy_item = {
        "kind": "energy",
        "name": "Energia electrica",
        "alias": "energia_electrica",
        "section": DEFAULT_SERVICE_SECTION_CODE,
        "position": 0,
        "configured_value": "Franjas por consumo",
        "amount": round(energy_cost, 6),
    }
    grouped[DEFAULT_SERVICE_SECTION_CODE].append(energy_item)
    for item in sorted(
        fixed_breakdown,
        key=lambda value: (int(value.get("position") or 0), str(value.get("name") or "")),
    ):
        grouped.setdefault(charge_section(item, default=DEFAULT_SERVICE_SECTION_CODE), []).append(item)
    for item in sorted(
        formula_breakdown,
        key=lambda value: (int(value.get("position") or 0), str(value.get("name") or "")),
    ):
        grouped.setdefault(charge_section(item, default=DEFAULT_TAX_SECTION_CODE), []).append(item)

    section_breakdowns: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for section in sections:
        code = str(section.get("code") or "")
        items = grouped.get(code, [])
        if not items and not bool(section.get("is_system", False)):
            continue
        section_breakdowns.append(
            (
                section,
                {
                    "items": items,
                    "total": round(sum(float(item.get("amount") or 0.0) for item in items), 6),
                },
            )
        )
    return section_breakdowns


def build_service_breakdown(
    *,
    energy_cost: float,
    fixed_breakdown: list[dict[str, Any]],
    formula_breakdown: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = [
        {
            "kind": "energy",
            "name": "Energia electrica",
            "alias": "energia_electrica",
            "section": DEFAULT_SERVICE_SECTION_CODE,
            "position": 0,
            "configured_value": "Franjas por consumo",
            "amount": round(energy_cost, 6),
        }
    ]
    items.extend(
        item
        for item in sorted(fixed_breakdown, key=lambda value: (int(value.get("position") or 0), str(value.get("name") or "")))
        if charge_section(item, default=DEFAULT_SERVICE_SECTION_CODE) == DEFAULT_SERVICE_SECTION_CODE
    )
    items.extend(
        item
        for item in sorted(formula_breakdown, key=lambda value: (int(value.get("position") or 0), str(value.get("name") or "")))
        if charge_section(item, default=DEFAULT_TAX_SECTION_CODE) == DEFAULT_SERVICE_SECTION_CODE
    )
    return items


def build_other_concepts_breakdown(
    *,
    fixed_breakdown: list[dict[str, Any]],
    formula_breakdown: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    items.extend(
        item
        for item in sorted(fixed_breakdown, key=lambda value: (int(value.get("position") or 0), str(value.get("name") or "")))
        if charge_section(item, default=DEFAULT_SERVICE_SECTION_CODE) == DEFAULT_TAX_SECTION_CODE
    )
    items.extend(
        item
        for item in sorted(formula_breakdown, key=lambda value: (int(value.get("position") or 0), str(value.get("name") or "")))
        if charge_section(item, default=DEFAULT_TAX_SECTION_CODE) == DEFAULT_TAX_SECTION_CODE
    )
    return items


def evaluate_tax_expression(expression: str, context: dict[str, float]) -> float:
    raw_expression = expression.strip()
    if not raw_expression:
        return 0.0

    if NUMBER_RE.fullmatch(raw_expression):
        return round(float(raw_expression.replace(",", ".")), 6)

    percent_match = PERCENT_EXPRESSION_RE.fullmatch(raw_expression)
    if percent_match:
        factor = float(percent_match.group(1).replace(",", ".")) / 100.0
        variable_name = resolve_variable_name(percent_match.group(2))
        return round(factor * float(context.get(variable_name, 0.0)), 6)

    return round(safe_eval_expression(raw_expression, context), 6)


def resolve_variable_name(label: str) -> str:
    normalized = normalize_text(label).replace("_", " ")
    if normalized in VARIABLE_ALIASES:
        return VARIABLE_ALIASES[normalized]
    compact = normalize_reference_key(label)
    if compact in VARIABLE_ALIASES:
        return VARIABLE_ALIASES[compact]
    return compact


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().strip().split())


def normalize_reference_key(value: str) -> str:
    normalized = normalize_text(value)
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized.replace(" ", "_"))
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return "_"
    if normalized[0].isdigit():
        return f"item_{normalized}"
    return normalized


def build_named_reference_context(items: list[dict[str, Any]]) -> dict[str, float]:
    context: dict[str, float] = {}
    for item in items:
        amount = float(item.get("amount") or 0.0)
        register_named_amount(context, item.get("alias"), item.get("name"), amount)
    return context


def register_named_amount(
    context: dict[str, float],
    alias: object,
    name: object,
    amount: float,
) -> None:
    reserved_keys = set(VARIABLE_ALIASES.values())
    keys_to_register: list[str] = []

    if isinstance(alias, str) and alias.strip():
        alias_key = normalize_reference_key(alias)
        if alias_key not in reserved_keys:
            keys_to_register.append(alias_key)

    if isinstance(name, str) and name.strip():
        name_key = normalize_reference_key(name)
        if name_key not in reserved_keys:
            keys_to_register.append(name_key)

    for key in dict.fromkeys(keys_to_register):
        context[key] = round(context.get(key, 0.0) + amount, 6)


def safe_eval_expression(expression: str, context: dict[str, float]) -> float:
    prepared = expression.replace(",", ".")
    tree = ast.parse(prepared, mode="eval")

    def eval_node(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return eval_node(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return float(node.value)
        if isinstance(node, ast.BinOp):
            left = eval_node(node.left)
            right = eval_node(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            raise ValueError("operador no permitido")
        if isinstance(node, ast.UnaryOp):
            operand = eval_node(node.operand)
            if isinstance(node.op, ast.UAdd):
                return operand
            if isinstance(node.op, ast.USub):
                return -operand
            raise ValueError("operador unario no permitido")
        if isinstance(node, ast.Name):
            variable_name = resolve_variable_name(node.id)
            return float(context.get(variable_name, 0.0))
        raise ValueError("expresion no permitida")

    result = eval_node(tree)
    if not math.isfinite(result):
        raise ValueError("resultado no finito")
    return result


def _point_date(point: dict[str, Any]) -> date | None:
    iso_value = point.get("iso")
    if not isinstance(iso_value, str) or "T" not in iso_value:
        return None
    return datetime.fromisoformat(iso_value).date()


def _point_grid_kwh(point: dict[str, Any]) -> float:
    value = point.get("grid_kwh")
    if value is None:
        value = point.get("grid_used_kwh")
    if value is None:
        value = _convert_wh_to_kwh(point.get("grid_wh"))
    if value is None:
        return 0.0
    return float(value)


def _point_load_kwh(point: dict[str, Any]) -> float:
    value = point.get("load_kwh")
    if value is None:
        value = _convert_wh_to_kwh(point.get("load_wh"))
    if value is None:
        return 0.0
    return float(value)


def _point_solar_pv_kwh(point: dict[str, Any]) -> float:
    value = point.get("solar_pv_kwh")
    if value is None:
        value = _convert_wh_to_kwh(point.get("solar_pv_wh"))
    if value is None:
        return 0.0
    return float(value)


def _convert_wh_to_kwh(value: object) -> float | None:
    if value is None:
        return None
    return float(value) / 1000.0


def _iter_day_range(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)
    return days

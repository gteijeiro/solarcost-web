from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from .sections import (
    DEFAULT_SERVICE_SECTION_CODE,
    DEFAULT_TAX_SECTION_CODE,
    SYSTEM_SECTIONS,
    get_system_section_name,
    is_system_section_code,
    normalize_section_code,
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class CostsRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    role TEXT NOT NULL DEFAULT 'admin' CHECK(role IN ('admin', 'viewer')),
                    enabled INTEGER NOT NULL DEFAULT 1,
                    language TEXT NOT NULL DEFAULT 'es' CHECK(language IN ('es', 'en')),
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS billing_periods (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    starts_on TEXT NOT NULL UNIQUE,
                    utility_measured_kwh REAL,
                    has_inverter_data_issue INTEGER NOT NULL DEFAULT 0,
                    billing_source TEXT NOT NULL DEFAULT 'inverter' CHECK(billing_source IN ('inverter', 'utility')),
                    notes TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tariff_bands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL CHECK(scope IN ('default', 'period')),
                    billing_period_id INTEGER,
                    position INTEGER NOT NULL DEFAULT 0,
                    label TEXT NOT NULL DEFAULT '',
                    from_kwh REAL NOT NULL,
                    to_kwh REAL,
                    price_per_kwh REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (billing_period_id) REFERENCES billing_periods(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS cost_sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    position INTEGER NOT NULL DEFAULT 0,
                    is_system INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS charge_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL CHECK(scope IN ('default', 'period')),
                    billing_period_id INTEGER,
                    position INTEGER NOT NULL DEFAULT 0,
                    kind TEXT NOT NULL CHECK(kind IN ('tax', 'fixed')),
                    section TEXT NOT NULL DEFAULT 'tax',
                    name TEXT NOT NULL,
                    alias TEXT,
                    expression TEXT,
                    amount REAL,
                    show_on_dashboard INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (billing_period_id) REFERENCES billing_periods(id) ON DELETE CASCADE,
                    FOREIGN KEY (section) REFERENCES cost_sections(code)
                );
                """
            )
            self._ensure_system_sections(conn)
            user_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(users)").fetchall()}
            if "role" not in user_columns:
                conn.execute(
                    "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'admin'"
                )
            if "enabled" not in user_columns:
                conn.execute(
                    "ALTER TABLE users ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1"
                )
            if "language" not in user_columns:
                conn.execute(
                    "ALTER TABLE users ADD COLUMN language TEXT NOT NULL DEFAULT 'es'"
                )
            conn.execute(
                """
                UPDATE users
                SET role = 'admin'
                WHERE role IS NULL OR TRIM(role) = ''
                """
            )
            conn.execute(
                """
                UPDATE users
                SET enabled = 1
                WHERE enabled IS NULL
                """
            )
            conn.execute(
                """
                UPDATE users
                SET language = 'es'
                WHERE language IS NULL OR TRIM(language) = ''
                """
            )
            billing_period_columns = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(billing_periods)").fetchall()
            }
            if "utility_measured_kwh" not in billing_period_columns:
                conn.execute("ALTER TABLE billing_periods ADD COLUMN utility_measured_kwh REAL")
            if "has_inverter_data_issue" not in billing_period_columns:
                conn.execute(
                    "ALTER TABLE billing_periods ADD COLUMN has_inverter_data_issue INTEGER NOT NULL DEFAULT 0"
                )
            if "billing_source" not in billing_period_columns:
                conn.execute(
                    "ALTER TABLE billing_periods ADD COLUMN billing_source TEXT NOT NULL DEFAULT 'inverter'"
                )
            conn.execute(
                """
                UPDATE billing_periods
                SET billing_source = CASE
                    WHEN utility_measured_kwh IS NOT NULL THEN 'utility'
                    ELSE 'inverter'
                END
                WHERE billing_source IS NULL OR TRIM(billing_source) = ''
                """
            )
            columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(charge_rules)").fetchall()}
            if "section" not in columns:
                conn.execute("ALTER TABLE charge_rules ADD COLUMN section TEXT")
            conn.execute(
                """
                UPDATE charge_rules
                SET section = CASE
                    WHEN kind = 'fixed' THEN ?
                    ELSE ?
                END
                WHERE section IS NULL OR TRIM(section) = ''
                """,
                (DEFAULT_SERVICE_SECTION_CODE, DEFAULT_TAX_SECTION_CODE),
            )
            if "alias" not in columns:
                conn.execute("ALTER TABLE charge_rules ADD COLUMN alias TEXT")
            if "show_on_dashboard" not in columns:
                conn.execute(
                    "ALTER TABLE charge_rules ADD COLUMN show_on_dashboard INTEGER NOT NULL DEFAULT 0"
                )
            self._migrate_charge_rules_table(conn)
            self._ensure_rule_sections_are_valid(conn)

    def _ensure_system_sections(self, conn: sqlite3.Connection) -> None:
        now = utc_now()
        for section in SYSTEM_SECTIONS:
            existing = conn.execute(
                "SELECT id FROM cost_sections WHERE code = ?",
                (section["code"],),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO cost_sections (code, name, position, is_system, enabled, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        section["code"],
                        section["name"],
                        int(section["position"]),
                        1,
                        1 if section["enabled"] else 0,
                        now,
                        now,
                    ),
                )
                continue
            conn.execute(
                """
                UPDATE cost_sections
                SET name = ?, position = ?, is_system = 1
                WHERE code = ?
                """,
                (
                    section["name"],
                    int(section["position"]),
                    section["code"],
                ),
            )

    def _migrate_charge_rules_table(self, conn: sqlite3.Connection) -> None:
        create_sql_row = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'charge_rules'
            """
        ).fetchone()
        create_sql = str(create_sql_row["sql"] or "") if create_sql_row is not None else ""
        if "CHECK(section IN ('service', 'tax'))" not in create_sql:
            return

        conn.execute("ALTER TABLE charge_rules RENAME TO charge_rules_legacy")
        conn.execute(
            """
            CREATE TABLE charge_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL CHECK(scope IN ('default', 'period')),
                billing_period_id INTEGER,
                position INTEGER NOT NULL DEFAULT 0,
                kind TEXT NOT NULL CHECK(kind IN ('tax', 'fixed')),
                section TEXT NOT NULL DEFAULT 'tax',
                name TEXT NOT NULL,
                alias TEXT,
                expression TEXT,
                amount REAL,
                show_on_dashboard INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (billing_period_id) REFERENCES billing_periods(id) ON DELETE CASCADE,
                FOREIGN KEY (section) REFERENCES cost_sections(code)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO charge_rules (
                id, scope, billing_period_id, position, kind, section, name, alias, expression, amount, show_on_dashboard, enabled, created_at, updated_at
            )
            SELECT
                id,
                scope,
                billing_period_id,
                position,
                kind,
                CASE
                    WHEN section IS NULL OR TRIM(section) = '' THEN CASE WHEN kind = 'fixed' THEN ? ELSE ? END
                    ELSE section
                END,
                name,
                alias,
                expression,
                amount,
                show_on_dashboard,
                enabled,
                created_at,
                updated_at
            FROM charge_rules_legacy
            """,
            (DEFAULT_SERVICE_SECTION_CODE, DEFAULT_TAX_SECTION_CODE),
        )
        conn.execute("DROP TABLE charge_rules_legacy")

    def _ensure_rule_sections_are_valid(self, conn: sqlite3.Connection) -> None:
        section_codes = {
            str(row["code"])
            for row in conn.execute("SELECT code FROM cost_sections").fetchall()
        }
        fallback_service = DEFAULT_SERVICE_SECTION_CODE
        fallback_tax = DEFAULT_TAX_SECTION_CODE
        for row in conn.execute("SELECT id, kind, section FROM charge_rules").fetchall():
            section = str(row["section"] or "").strip()
            if section in section_codes:
                continue
            conn.execute(
                """
                UPDATE charge_rules
                SET section = ?
                WHERE id = ?
                """,
                (
                    fallback_service if str(row["kind"]) == "fixed" else fallback_tax,
                    int(row["id"]),
                ),
            )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None

    def user_count(self) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return int(row["total"]) if row is not None else 0

    def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        role: str = "admin",
        language: str = "es",
    ) -> int:
        now = utc_now()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO users (username, role, enabled, language, password_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (username, role, 1, language, password_hash, now),
            )
            return int(cursor.lastrowid)

    def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        return self._row_to_dict(row)

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def list_users(self) -> list[dict[str, Any]]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM users
                ORDER BY username ASC, id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def update_user_password(self, user_id: int, password_hash: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE users
                SET password_hash = ?
                WHERE id = ?
                """,
                (password_hash, user_id),
            )

    def update_user_enabled(self, user_id: int, enabled: bool) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE users
                SET enabled = ?
                WHERE id = ?
                """,
                (1 if enabled else 0, user_id),
            )

    def update_user_language(self, user_id: int, language: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE users
                SET language = ?
                WHERE id = ?
                """,
                (language, user_id),
            )

    def list_sections(self) -> list[dict[str, Any]]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM cost_sections
                ORDER BY position ASC, id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_section(self, section_id: int) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM cost_sections WHERE id = ?",
                (section_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def get_section_by_code(self, code: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM cost_sections WHERE code = ?",
                (code,),
            ).fetchone()
        return self._row_to_dict(row)

    def save_section(
        self,
        *,
        section_id: int | None,
        name: str,
        position: int,
        enabled: bool,
    ) -> int:
        now = utc_now()
        enabled_int = 1 if enabled else 0
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("El nombre de la seccion es obligatorio.")

        with self._connection() as conn:
            if section_id is None:
                base_code = normalize_section_code(normalized_name)
                code = base_code
                suffix = 2
                while conn.execute(
                    "SELECT 1 FROM cost_sections WHERE code = ?",
                    (code,),
                ).fetchone():
                    code = f"{base_code}_{suffix}"
                    suffix += 1
                cursor = conn.execute(
                    """
                    INSERT INTO cost_sections (code, name, position, is_system, enabled, created_at, updated_at)
                    VALUES (?, ?, ?, 0, ?, ?, ?)
                    """,
                    (code, normalized_name, position, enabled_int, now, now),
                )
                return int(cursor.lastrowid)

            existing = conn.execute(
                "SELECT code, is_system FROM cost_sections WHERE id = ?",
                (section_id,),
            ).fetchone()
            if existing is None:
                raise ValueError("La seccion no existe.")
            if bool(existing["is_system"]):
                normalized_name = str(get_system_section_name(str(existing["code"])) or normalized_name)
            conn.execute(
                """
                UPDATE cost_sections
                SET name = ?, position = ?, enabled = ?, updated_at = ?
                WHERE id = ?
                """,
                (normalized_name, position, enabled_int, now, section_id),
            )
            return section_id

    def delete_section(self, section_id: int) -> None:
        with self._connection() as conn:
            section = conn.execute(
                "SELECT code, is_system FROM cost_sections WHERE id = ?",
                (section_id,),
            ).fetchone()
            if section is None:
                raise ValueError("La seccion no existe.")
            if bool(section["is_system"]):
                raise ValueError("No puedes eliminar una seccion predeterminada.")
            usage = conn.execute(
                "SELECT COUNT(*) AS total FROM charge_rules WHERE section = ?",
                (str(section["code"]),),
            ).fetchone()
            if usage is not None and int(usage["total"]) > 0:
                raise ValueError("No puedes eliminar una seccion que ya esta en uso.")
            conn.execute("DELETE FROM cost_sections WHERE id = ?", (section_id,))

    def export_configuration(self) -> dict[str, Any]:
        periods = self.list_billing_periods(ascending=True)
        return {
            "format": "solarcost-web-config",
            "schema_version": 3,
            "exported_at": utc_now(),
            "data": {
                "sections": [self._serialize_section(item) for item in self.list_sections()],
                "defaults": {
                    "tariff_bands": [self._serialize_tariff_band(item) for item in self.list_tariff_bands(scope="default")],
                    "fixed_charges": [
                        self._serialize_charge_rule(item)
                        for item in self.list_charge_rules(scope="default", kind="fixed")
                    ],
                    "tax_rules": [
                        self._serialize_charge_rule(item)
                        for item in self.list_charge_rules(scope="default", kind="tax")
                    ],
                },
                "periods": [
                    {
                        "name": str(period.get("name") or ""),
                        "starts_on": str(period.get("starts_on") or ""),
                        "utility_measured_kwh": (
                            float(period["utility_measured_kwh"])
                            if period.get("utility_measured_kwh") is not None
                            else None
                        ),
                        "has_inverter_data_issue": bool(period.get("has_inverter_data_issue", 0)),
                        "billing_source": str(period.get("billing_source") or "inverter"),
                        "notes": str(period.get("notes") or ""),
                        "tariff_bands": [
                            self._serialize_tariff_band(item)
                            for item in self.list_tariff_bands(scope="period", billing_period_id=int(period["id"]))
                        ],
                        "fixed_charges": [
                            self._serialize_charge_rule(item)
                            for item in self.list_charge_rules(
                                scope="period",
                                kind="fixed",
                                billing_period_id=int(period["id"]),
                            )
                        ],
                        "tax_rules": [
                            self._serialize_charge_rule(item)
                            for item in self.list_charge_rules(
                                scope="period",
                                kind="tax",
                                billing_period_id=int(period["id"]),
                            )
                        ],
                    }
                    for period in periods
                ],
            },
        }

    def prepare_configuration_import(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("El archivo debe contener un objeto JSON valido.")
        if str(payload.get("format") or "") != "solarcost-web-config":
            raise ValueError("El archivo no corresponde a una exportacion valida de SolarCost Web.")
        schema_version = int(payload.get("schema_version") or 0)
        if schema_version not in {1, 2, 3}:
            raise ValueError("La version del archivo no es compatible con esta aplicacion.")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("El archivo no contiene datos de configuracion.")

        sections = self._normalize_import_sections(data.get("sections"))
        valid_section_codes = {str(section.get("code") or "") for section in sections}
        defaults = self._normalize_import_defaults(data.get("defaults"), valid_section_codes=valid_section_codes)
        periods = self._normalize_import_periods(data.get("periods"), valid_section_codes=valid_section_codes)

        return {
            "format": "solarcost-web-config",
            "schema_version": schema_version,
            "exported_at": str(payload.get("exported_at") or ""),
            "data": {
                "sections": sections,
                "defaults": defaults,
                "periods": periods,
            },
        }

    def import_configuration(
        self,
        prepared_payload: dict[str, Any],
        *,
        include_sections: bool,
        include_default_bands: bool,
        include_default_fixed: bool,
        include_default_taxes: bool,
        selected_period_starts_on: set[str],
    ) -> dict[str, int]:
        data = prepared_payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("La configuracion importada no es valida.")

        defaults = data.get("defaults")
        periods = data.get("periods")
        if not isinstance(defaults, dict) or not isinstance(periods, list):
            raise ValueError("La configuracion importada no es valida.")

        selected_period_keys = {str(item).strip() for item in selected_period_starts_on if str(item).strip()}
        if not any([include_sections, include_default_bands, include_default_fixed, include_default_taxes, selected_period_keys]):
            raise ValueError("Selecciona al menos una seccion o un periodo para importar.")

        result = {
            "sections_upserted": 0,
            "default_bands_replaced": 0,
            "default_fixed_replaced": 0,
            "default_taxes_replaced": 0,
            "periods_created": 0,
            "periods_updated": 0,
        }

        with self._connection() as conn:
            if include_sections:
                for section in data.get("sections", []):
                    if not isinstance(section, dict):
                        continue
                    self._upsert_section(conn, section)
                    result["sections_upserted"] += 1

            if include_default_bands:
                conn.execute("DELETE FROM tariff_bands WHERE scope = 'default'")
                for band in defaults.get("tariff_bands", []):
                    self._insert_tariff_band(conn, scope="default", billing_period_id=None, band=band)
                result["default_bands_replaced"] = len(defaults.get("tariff_bands", []))

            if include_default_fixed:
                conn.execute("DELETE FROM charge_rules WHERE scope = 'default' AND kind = 'fixed'")
                for rule in defaults.get("fixed_charges", []):
                    self._insert_charge_rule(
                        conn,
                        scope="default",
                        billing_period_id=None,
                        kind="fixed",
                        rule=rule,
                    )
                result["default_fixed_replaced"] = len(defaults.get("fixed_charges", []))

            if include_default_taxes:
                conn.execute("DELETE FROM charge_rules WHERE scope = 'default' AND kind = 'tax'")
                for rule in defaults.get("tax_rules", []):
                    self._insert_charge_rule(
                        conn,
                        scope="default",
                        billing_period_id=None,
                        kind="tax",
                        rule=rule,
                    )
                result["default_taxes_replaced"] = len(defaults.get("tax_rules", []))

            for period in periods:
                starts_on = str(period.get("starts_on") or "")
                if starts_on not in selected_period_keys:
                    continue

                existing_row = conn.execute(
                    "SELECT id FROM billing_periods WHERE starts_on = ?",
                    (starts_on,),
                ).fetchone()

                if existing_row is None:
                    cursor = conn.execute(
                        """
                        INSERT INTO billing_periods (
                            name, starts_on, utility_measured_kwh, has_inverter_data_issue, billing_source, notes, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            period["name"],
                            period["starts_on"],
                            period["utility_measured_kwh"],
                            1 if period["has_inverter_data_issue"] else 0,
                            period["billing_source"],
                            period["notes"],
                            utc_now(),
                            utc_now(),
                        ),
                    )
                    period_id = int(cursor.lastrowid)
                    result["periods_created"] += 1
                else:
                    period_id = int(existing_row["id"])
                    conn.execute(
                        """
                        UPDATE billing_periods
                        SET name = ?, utility_measured_kwh = ?, has_inverter_data_issue = ?, billing_source = ?, notes = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            period["name"],
                            period["utility_measured_kwh"],
                            1 if period["has_inverter_data_issue"] else 0,
                            period["billing_source"],
                            period["notes"],
                            utc_now(),
                            period_id,
                        ),
                    )
                    conn.execute(
                        "DELETE FROM tariff_bands WHERE scope = 'period' AND billing_period_id = ?",
                        (period_id,),
                    )
                    conn.execute(
                        "DELETE FROM charge_rules WHERE scope = 'period' AND billing_period_id = ?",
                        (period_id,),
                    )
                    result["periods_updated"] += 1

                for band in period["tariff_bands"]:
                    self._insert_tariff_band(
                        conn,
                        scope="period",
                        billing_period_id=period_id,
                        band=band,
                    )

                for rule in period["fixed_charges"]:
                    self._insert_charge_rule(
                        conn,
                        scope="period",
                        billing_period_id=period_id,
                        kind="fixed",
                        rule=rule,
                    )

                for rule in period["tax_rules"]:
                    self._insert_charge_rule(
                        conn,
                        scope="period",
                        billing_period_id=period_id,
                        kind="tax",
                        rule=rule,
                    )

        return result

    def list_billing_periods(self, *, ascending: bool = False) -> list[dict[str, Any]]:
        direction = "ASC" if ascending else "DESC"
        with self._connection() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM billing_periods
                ORDER BY starts_on {direction}, id {direction}
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_billing_period(self, period_id: int) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM billing_periods WHERE id = ?",
                (period_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def save_billing_period(
        self,
        *,
        period_id: int | None,
        name: str,
        starts_on: str,
        utility_measured_kwh: float | None,
        has_inverter_data_issue: bool,
        billing_source: str,
        notes: str,
    ) -> int:
        now = utc_now()
        has_inverter_data_issue_int = 1 if has_inverter_data_issue else 0
        with self._connection() as conn:
            if period_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO billing_periods (
                        name, starts_on, utility_measured_kwh, has_inverter_data_issue, billing_source, notes, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        starts_on,
                        utility_measured_kwh,
                        has_inverter_data_issue_int,
                        billing_source,
                        notes,
                        now,
                        now,
                    ),
                )
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE billing_periods
                SET name = ?, starts_on = ?, utility_measured_kwh = ?, has_inverter_data_issue = ?, billing_source = ?, notes = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name,
                    starts_on,
                    utility_measured_kwh,
                    has_inverter_data_issue_int,
                    billing_source,
                    notes,
                    now,
                    period_id,
                ),
            )
            return period_id

    def update_billing_source(self, period_id: int, billing_source: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE billing_periods
                SET billing_source = ?, updated_at = ?
                WHERE id = ?
                """,
                (billing_source, utc_now(), period_id),
            )

    def delete_billing_period(self, period_id: int) -> None:
        with self._connection() as conn:
            conn.execute("DELETE FROM billing_periods WHERE id = ?", (period_id,))

    def find_latest_period_with_tariff_bands_before(
        self,
        starts_on: str,
        *,
        exclude_period_id: int | None = None,
    ) -> dict[str, Any] | None:
        sql = """
            SELECT DISTINCT bp.*
            FROM billing_periods bp
            JOIN tariff_bands tb
              ON tb.scope = 'period'
             AND tb.billing_period_id = bp.id
            WHERE bp.starts_on < ?
        """
        params: list[Any] = [starts_on]
        if exclude_period_id is not None:
            sql += " AND bp.id != ?"
            params.append(exclude_period_id)
        sql += " ORDER BY bp.starts_on DESC, bp.id DESC LIMIT 1"

        with self._connection() as conn:
            row = conn.execute(sql, params).fetchone()
        return self._row_to_dict(row)

    def find_latest_period_with_charge_rules_before(
        self,
        starts_on: str,
        *,
        kind: str,
        exclude_period_id: int | None = None,
    ) -> dict[str, Any] | None:
        sql = """
            SELECT DISTINCT bp.*
            FROM billing_periods bp
            JOIN charge_rules cr
              ON cr.scope = 'period'
             AND cr.kind = ?
             AND cr.billing_period_id = bp.id
            WHERE bp.starts_on < ?
        """
        params: list[Any] = [kind, starts_on]
        if exclude_period_id is not None:
            sql += " AND bp.id != ?"
            params.append(exclude_period_id)
        sql += " ORDER BY bp.starts_on DESC, bp.id DESC LIMIT 1"

        with self._connection() as conn:
            row = conn.execute(sql, params).fetchone()
        return self._row_to_dict(row)

    def list_tariff_bands(self, *, scope: str, billing_period_id: int | None = None) -> list[dict[str, Any]]:
        with self._connection() as conn:
            if scope == "default":
                rows = conn.execute(
                    """
                    SELECT *
                    FROM tariff_bands
                    WHERE scope = 'default'
                    ORDER BY position ASC, from_kwh ASC, id ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM tariff_bands
                    WHERE scope = 'period' AND billing_period_id = ?
                    ORDER BY position ASC, from_kwh ASC, id ASC
                    """,
                    (billing_period_id,),
                ).fetchall()
        return [dict(row) for row in rows]

    def save_tariff_band(
        self,
        *,
        band_id: int | None,
        scope: str,
        billing_period_id: int | None,
        position: int,
        label: str,
        from_kwh: float,
        to_kwh: float | None,
        price_per_kwh: float,
    ) -> int:
        now = utc_now()
        with self._connection() as conn:
            if band_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO tariff_bands (
                        scope, billing_period_id, position, label, from_kwh, to_kwh, price_per_kwh, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (scope, billing_period_id, position, label, from_kwh, to_kwh, price_per_kwh, now, now),
                )
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE tariff_bands
                SET position = ?, label = ?, from_kwh = ?, to_kwh = ?, price_per_kwh = ?, updated_at = ?
                WHERE id = ?
                """,
                (position, label, from_kwh, to_kwh, price_per_kwh, now, band_id),
            )
            return band_id

    def delete_tariff_band(self, band_id: int) -> None:
        with self._connection() as conn:
            conn.execute("DELETE FROM tariff_bands WHERE id = ?", (band_id,))

    def copy_tariff_bands_to_period(self, *, billing_period_id: int, source_bands: list[dict[str, Any]]) -> int:
        if not source_bands:
            return 0

        now = utc_now()
        copied = 0
        with self._connection() as conn:
            for band in source_bands:
                conn.execute(
                    """
                    INSERT INTO tariff_bands (
                        scope, billing_period_id, position, label, from_kwh, to_kwh, price_per_kwh, created_at, updated_at
                    )
                    VALUES ('period', ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        billing_period_id,
                        int(band.get("position") or 0),
                        str(band.get("label") or ""),
                        float(band.get("from_kwh") or 0.0),
                        float(band["to_kwh"]) if band.get("to_kwh") is not None else None,
                        float(band.get("price_per_kwh") or 0.0),
                        now,
                        now,
                    ),
                )
                copied += 1
        return copied

    def copy_charge_rules_to_period(
        self,
        *,
        billing_period_id: int,
        source_rules: list[dict[str, Any]],
        kind: str,
    ) -> int:
        if not source_rules:
            return 0

        now = utc_now()
        copied = 0
        with self._connection() as conn:
            for rule in source_rules:
                conn.execute(
                    """
                    INSERT INTO charge_rules (
                        scope, billing_period_id, position, kind, section, name, alias, expression, amount, show_on_dashboard, enabled, created_at, updated_at
                    )
                    VALUES ('period', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        billing_period_id,
                        int(rule.get("position") or 0),
                        kind,
                        str(rule.get("section") or (DEFAULT_SERVICE_SECTION_CODE if kind == "fixed" else DEFAULT_TAX_SECTION_CODE)),
                        str(rule.get("name") or ""),
                        str(rule["alias"]) if rule.get("alias") is not None else None,
                        str(rule["expression"]) if rule.get("expression") is not None else None,
                        float(rule["amount"]) if rule.get("amount") is not None else None,
                        1 if rule.get("show_on_dashboard", 0) else 0,
                        1 if rule.get("enabled", 1) else 0,
                        now,
                        now,
                    ),
                )
                copied += 1
        return copied

    def list_charge_rules(
        self,
        *,
        scope: str,
        kind: str | None = None,
        billing_period_id: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["scope = ?"]
        params: list[Any] = [scope]

        if scope == "period":
            clauses.append("billing_period_id = ?")
            params.append(billing_period_id)
        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind)

        sql = f"""
            SELECT *
            FROM charge_rules
            WHERE {' AND '.join(clauses)}
            ORDER BY position ASC, id ASC
        """

        with self._connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def save_charge_rule(
        self,
        *,
        rule_id: int | None,
        scope: str,
        billing_period_id: int | None,
        position: int,
        kind: str,
        section: str,
        name: str,
        alias: str | None,
        expression: str | None,
        amount: float | None,
        show_on_dashboard: bool,
        enabled: bool,
    ) -> int:
        now = utc_now()
        enabled_int = 1 if enabled else 0
        show_on_dashboard_int = 1 if show_on_dashboard else 0
        with self._connection() as conn:
            if rule_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO charge_rules (
                        scope, billing_period_id, position, kind, section, name, alias, expression, amount, show_on_dashboard, enabled, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scope,
                        billing_period_id,
                        position,
                        kind,
                        section,
                        name,
                        alias,
                        expression,
                        amount,
                        show_on_dashboard_int,
                        enabled_int,
                        now,
                        now,
                    ),
                )
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE charge_rules
                SET position = ?, section = ?, name = ?, alias = ?, expression = ?, amount = ?, show_on_dashboard = ?, enabled = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    position,
                    section,
                    name,
                    alias,
                    expression,
                    amount,
                    show_on_dashboard_int,
                    enabled_int,
                    now,
                    rule_id,
                ),
            )
            return rule_id

    def delete_charge_rule(self, rule_id: int) -> None:
        with self._connection() as conn:
            conn.execute("DELETE FROM charge_rules WHERE id = ?", (rule_id,))

    def get_effective_tariff_bands(self, billing_period_id: int) -> list[dict[str, Any]]:
        period_bands = self.list_tariff_bands(scope="period", billing_period_id=billing_period_id)
        if period_bands:
            return period_bands
        return self.list_tariff_bands(scope="default")

    def get_effective_fixed_charges(self, billing_period_id: int) -> list[dict[str, Any]]:
        period_rules = self.list_charge_rules(scope="period", kind="fixed", billing_period_id=billing_period_id)
        if period_rules:
            return period_rules
        return self.list_charge_rules(scope="default", kind="fixed")

    def get_effective_tax_rules(self, billing_period_id: int) -> list[dict[str, Any]]:
        period_rules = self.list_charge_rules(scope="period", kind="tax", billing_period_id=billing_period_id)
        if period_rules:
            return period_rules
        return self.list_charge_rules(scope="default", kind="tax")

    @staticmethod
    def _serialize_tariff_band(band: dict[str, Any]) -> dict[str, Any]:
        return {
            "position": int(band.get("position") or 0),
            "label": str(band.get("label") or ""),
            "from_kwh": float(band.get("from_kwh") or 0.0),
            "to_kwh": float(band["to_kwh"]) if band.get("to_kwh") is not None else None,
            "price_per_kwh": float(band.get("price_per_kwh") or 0.0),
        }

    @staticmethod
    def _serialize_section(section: dict[str, Any]) -> dict[str, Any]:
        return {
            "code": str(section.get("code") or ""),
            "name": str(section.get("name") or ""),
            "position": int(section.get("position") or 0),
            "is_system": bool(section.get("is_system", 0)),
            "enabled": bool(section.get("enabled", 1)),
        }

    @staticmethod
    def _serialize_charge_rule(rule: dict[str, Any]) -> dict[str, Any]:
        return {
            "position": int(rule.get("position") or 0),
            "section": str(
                rule.get("section")
                or (DEFAULT_SERVICE_SECTION_CODE if rule.get("kind") == "fixed" else DEFAULT_TAX_SECTION_CODE)
            ),
            "name": str(rule.get("name") or ""),
            "alias": str(rule["alias"]) if rule.get("alias") not in (None, "") else None,
            "expression": str(rule["expression"]) if rule.get("expression") not in (None, "") else None,
            "amount": float(rule["amount"]) if rule.get("amount") is not None else None,
            "show_on_dashboard": bool(rule.get("show_on_dashboard", 0)),
            "enabled": bool(rule.get("enabled", 1)),
        }

    @staticmethod
    def _coerce_bool(value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"1", "true", "si", "sí", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default

    def _normalize_import_defaults(
        self,
        raw_defaults: Any,
        *,
        valid_section_codes: set[str],
    ) -> dict[str, list[dict[str, Any]]]:
        defaults = raw_defaults if isinstance(raw_defaults, dict) else {}
        return {
            "tariff_bands": self._normalize_import_tariff_bands(defaults.get("tariff_bands"), context="plantilla"),
            "fixed_charges": self._normalize_import_charge_rules(
                defaults.get("fixed_charges"),
                kind="fixed",
                context="plantilla",
                valid_section_codes=valid_section_codes,
            ),
            "tax_rules": self._normalize_import_charge_rules(
                defaults.get("tax_rules"),
                kind="tax",
                context="plantilla",
                valid_section_codes=valid_section_codes,
            ),
        }

    def _normalize_import_sections(self, raw_sections: Any) -> list[dict[str, Any]]:
        provided_sections = raw_sections if isinstance(raw_sections, list) else []
        sections: list[dict[str, Any]] = []
        seen_codes: set[str] = set()

        for raw_section in provided_sections:
            if not isinstance(raw_section, dict):
                raise ValueError("Cada seccion importada debe ser un objeto JSON.")
            raw_name = str(raw_section.get("name") or "").strip()
            raw_code = str(raw_section.get("code") or "").strip()
            code = raw_code or normalize_section_code(raw_name)
            if not raw_name and is_system_section_code(code):
                raw_name = str(get_system_section_name(code) or "")
            if not raw_name:
                raise ValueError("Hay una seccion importada sin nombre.")
            if code in seen_codes:
                raise ValueError(f"La seccion '{raw_name}' esta repetida en el archivo.")
            sections.append(
                {
                    "code": code,
                    "name": raw_name,
                    "position": int(raw_section.get("position") or 0),
                    "is_system": bool(raw_section.get("is_system", False) or is_system_section_code(code)),
                    "enabled": self._coerce_bool(raw_section.get("enabled"), default=True),
                }
            )
            seen_codes.add(code)

        for system_section in SYSTEM_SECTIONS:
            if system_section["code"] in seen_codes:
                continue
            sections.append(
                {
                    "code": system_section["code"],
                    "name": system_section["name"],
                    "position": int(system_section["position"]),
                    "is_system": True,
                    "enabled": bool(system_section["enabled"]),
                }
            )

        return sorted(sections, key=lambda item: (item["position"], item["name"], item["code"]))

    def _normalize_import_periods(
        self,
        raw_periods: Any,
        *,
        valid_section_codes: set[str],
    ) -> list[dict[str, Any]]:
        if raw_periods is None:
            return []
        if not isinstance(raw_periods, list):
            raise ValueError("La lista de periodos importados no es valida.")

        periods: list[dict[str, Any]] = []
        starts_on_seen: set[str] = set()
        for raw_period in raw_periods:
            if not isinstance(raw_period, dict):
                raise ValueError("Cada periodo importado debe ser un objeto JSON.")
            name = str(raw_period.get("name") or "").strip()
            starts_on = str(raw_period.get("starts_on") or "").strip()
            billing_source = str(raw_period.get("billing_source") or "inverter").strip()
            if not name:
                raise ValueError("Todos los periodos importados deben tener nombre.")
            if not starts_on:
                raise ValueError(f"El periodo '{name}' no tiene fecha de inicio.")
            if starts_on in starts_on_seen:
                raise ValueError(f"El periodo con inicio {starts_on} esta repetido.")
            if billing_source not in {"inverter", "utility"}:
                raise ValueError(f"El periodo '{name}' tiene una fuente de calculo invalida.")
            utility_measured_kwh = raw_period.get("utility_measured_kwh")
            periods.append(
                {
                    "name": name,
                    "starts_on": starts_on,
                    "utility_measured_kwh": float(utility_measured_kwh) if utility_measured_kwh is not None else None,
                    "has_inverter_data_issue": self._coerce_bool(
                        raw_period.get("has_inverter_data_issue"),
                        default=False,
                    ),
                    "billing_source": billing_source,
                    "notes": str(raw_period.get("notes") or ""),
                    "tariff_bands": self._normalize_import_tariff_bands(
                        raw_period.get("tariff_bands"),
                        context=f"periodo {name}",
                    ),
                    "fixed_charges": self._normalize_import_charge_rules(
                        raw_period.get("fixed_charges"),
                        kind="fixed",
                        context=f"periodo {name}",
                        valid_section_codes=valid_section_codes,
                    ),
                    "tax_rules": self._normalize_import_charge_rules(
                        raw_period.get("tax_rules"),
                        kind="tax",
                        context=f"periodo {name}",
                        valid_section_codes=valid_section_codes,
                    ),
                }
            )
            starts_on_seen.add(starts_on)
        return sorted(periods, key=lambda item: (item["starts_on"], item["name"]))

    def _normalize_import_tariff_bands(self, raw_bands: Any, *, context: str) -> list[dict[str, Any]]:
        if raw_bands is None:
            return []
        if not isinstance(raw_bands, list):
            raise ValueError(f"La lista de franjas para {context} no es valida.")

        bands: list[dict[str, Any]] = []
        for raw_band in raw_bands:
            if not isinstance(raw_band, dict):
                raise ValueError(f"Cada franja importada para {context} debe ser un objeto JSON.")
            if raw_band.get("from_kwh") is None:
                raise ValueError(f"Una franja de {context} no tiene 'from_kwh'.")
            if raw_band.get("price_per_kwh") is None:
                raise ValueError(f"Una franja de {context} no tiene 'price_per_kwh'.")
            bands.append(
                {
                    "position": int(raw_band.get("position") or 0),
                    "label": str(raw_band.get("label") or ""),
                    "from_kwh": float(raw_band["from_kwh"]),
                    "to_kwh": float(raw_band["to_kwh"]) if raw_band.get("to_kwh") is not None else None,
                    "price_per_kwh": float(raw_band["price_per_kwh"]),
                }
            )
        return sorted(bands, key=lambda item: (item["position"], item["from_kwh"], item["label"]))

    def _normalize_import_charge_rules(
        self,
        raw_rules: Any,
        *,
        kind: str,
        context: str,
        valid_section_codes: set[str],
    ) -> list[dict[str, Any]]:
        if raw_rules is None:
            return []
        if not isinstance(raw_rules, list):
            raise ValueError(f"La lista de reglas para {context} no es valida.")

        rules: list[dict[str, Any]] = []
        for raw_rule in raw_rules:
            if not isinstance(raw_rule, dict):
                raise ValueError(f"Cada regla importada para {context} debe ser un objeto JSON.")
            name = str(raw_rule.get("name") or "").strip()
            section = str(
                raw_rule.get("section")
                or (DEFAULT_SERVICE_SECTION_CODE if kind == "fixed" else DEFAULT_TAX_SECTION_CODE)
            ).strip()
            if not name:
                raise ValueError(f"Hay una regla de {context} sin nombre.")
            if section not in valid_section_codes and not is_system_section_code(section):
                raise ValueError(f"La regla '{name}' de {context} tiene una seccion invalida.")
            alias_value = raw_rule.get("alias")
            expression_value = raw_rule.get("expression")
            amount_value = raw_rule.get("amount")
            expression = str(expression_value).strip() if expression_value not in (None, "") else None
            amount = float(amount_value) if amount_value is not None else None
            if kind == "fixed" and amount is None:
                raise ValueError(f"La regla fija '{name}' de {context} no tiene importe.")
            if kind == "tax" and not expression:
                raise ValueError(f"La regla por formula '{name}' de {context} no tiene expresion.")
            rules.append(
                {
                    "position": int(raw_rule.get("position") or 0),
                    "section": section,
                    "name": name,
                    "alias": str(alias_value).strip() if alias_value not in (None, "") else None,
                    "expression": expression if kind == "tax" else None,
                    "amount": amount if kind == "fixed" else None,
                    "show_on_dashboard": self._coerce_bool(raw_rule.get("show_on_dashboard"), default=False),
                    "enabled": self._coerce_bool(raw_rule.get("enabled"), default=True),
                }
            )
        return sorted(rules, key=lambda item: (item["position"], item["name"]))

    @staticmethod
    def _insert_tariff_band(
        conn: sqlite3.Connection,
        *,
        scope: str,
        billing_period_id: int | None,
        band: dict[str, Any],
    ) -> None:
        conn.execute(
            """
            INSERT INTO tariff_bands (
                scope, billing_period_id, position, label, from_kwh, to_kwh, price_per_kwh, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scope,
                billing_period_id,
                int(band.get("position") or 0),
                str(band.get("label") or ""),
                float(band.get("from_kwh") or 0.0),
                float(band["to_kwh"]) if band.get("to_kwh") is not None else None,
                float(band.get("price_per_kwh") or 0.0),
                utc_now(),
                utc_now(),
            ),
        )

    @staticmethod
    def _insert_charge_rule(
        conn: sqlite3.Connection,
        *,
        scope: str,
        billing_period_id: int | None,
        kind: str,
        rule: dict[str, Any],
    ) -> None:
        conn.execute(
            """
            INSERT INTO charge_rules (
                scope, billing_period_id, position, kind, section, name, alias, expression, amount, show_on_dashboard, enabled, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scope,
                billing_period_id,
                int(rule.get("position") or 0),
                kind,
                str(rule.get("section") or (DEFAULT_SERVICE_SECTION_CODE if kind == "fixed" else DEFAULT_TAX_SECTION_CODE)),
                str(rule.get("name") or ""),
                str(rule["alias"]) if rule.get("alias") not in (None, "") else None,
                str(rule["expression"]) if rule.get("expression") not in (None, "") else None,
                float(rule["amount"]) if rule.get("amount") is not None else None,
                1 if rule.get("show_on_dashboard", False) else 0,
                1 if rule.get("enabled", True) else 0,
                utc_now(),
                utc_now(),
            ),
        )

    def _upsert_section(self, conn: sqlite3.Connection, section: dict[str, Any]) -> None:
        code = str(section.get("code") or "").strip()
        if not code:
            return
        now = utc_now()
        existing = conn.execute(
            "SELECT id, is_system FROM cost_sections WHERE code = ?",
            (code,),
        ).fetchone()
        name = str(section.get("name") or get_system_section_name(code) or code).strip()
        position = int(section.get("position") or 0)
        enabled = 1 if bool(section.get("enabled", True)) else 0
        is_system = 1 if is_system_section_code(code) or bool(section.get("is_system", False)) else 0

        if existing is None:
            conn.execute(
                """
                INSERT INTO cost_sections (code, name, position, is_system, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (code, name, position, is_system, enabled, now, now),
            )
            return

        if bool(existing["is_system"]):
            name = str(get_system_section_name(code) or name)
            is_system = 1
        conn.execute(
            """
            UPDATE cost_sections
            SET name = ?, position = ?, is_system = ?, enabled = ?, updated_at = ?
            WHERE code = ?
            """,
            (name, position, is_system, enabled, now, code),
        )

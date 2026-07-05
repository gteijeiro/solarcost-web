# SolarCost Web

Aplicacion web en Python para calcular el costo de la luz consumiendo la API del bridge de SolarAssistant.

## Funcionalidades

- login con usuario y contrasena,
- alta inicial del administrador,
- ABM de periodos de facturacion sin fecha de fin,
- plantilla de franjas de energia para nuevos meses,
- copia automatica de la tarifa del ultimo periodo con precios cargados al crear un mes nuevo,
- plantilla de cargos fijos para nuevos meses,
- copia automatica de los cargos fijos del ultimo periodo con importes cargados al crear un mes nuevo,
- ABM de cargos fijos,
- ABM de impuestos con expresiones como `21% de total_factura` o `5% de costo_energia`,
- referencias entre cargos e impuestos por alias en expresiones, tomando `0` cuando un item no existe en ese periodo,
- separacion entre `Servicio de energia` e `IVA y otros conceptos` para conceptos fijos y reglas por formula,
- ajuste por periodo de franjas, cargos e impuestos,
- carga opcional del total mensual medido por la compania para usarlo como consumo facturable,
- selector por periodo para calcular la factura usando compania o inversor,
- marca manual por periodo para indicar errores de medicion del inversor,
- deteccion automatica de dias faltantes sin medicion del inversor, sin tratar como error un dia con consumo `0`,
- comparativo visual entre lo medido por el inversor y lo declarado por la compania,
- visualizacion paralela de montos calculados con ambas fuentes cuando existe lectura de compania,
- graficos por periodo con selector entre vista de barras y vista de area,
- calculo usando `grid_kwh` del endpoint diario de `points`, o el total manual de la compania si fue cargado.

## Estructura

- `src/sa_costs_web`: codigo fuente
- `tests/`: pruebas
- `requirements.txt`: dependencias
- `data/`: base SQLite local generada en ejecucion

## Instalacion

```bash
sudo apt update
sudo apt install -y python3-full python3-venv
sudo mkdir -p /opt/solarcost/web
sudo chown -R "$USER":"$USER" /opt/solarcost/web
cd /opt/solarcost/web
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install solarcost-web
sudo "$(command -v sa_web)" init
```

## Desinstalacion

`pip uninstall` solo elimina el paquete instalado dentro del entorno virtual. No borra automaticamente:

- el archivo `solarcost-web.env`,
- la base SQLite,
- el directorio de trabajo,
- los unit files de `systemd`,
- ni los servicios habilitados.

Eso es intencional para no perder configuracion o datos sin querer.

Si quieres una desinstalacion conservando configuracion:

```bash
source .venv/bin/activate
python -m pip uninstall solarcost-web
```

O con el asistente interactivo:

```bash
sa_web uninstall
```

Si quieres tocar un servicio `system`, usa:

```bash
sudo "$(command -v sa_web)" uninstall
```

Si quieres una desinstalacion limpia completa:

```bash
sudo systemctl stop solarcost-web.service
sudo systemctl disable solarcost-web.service
sudo rm -f /etc/systemd/system/solarcost-web.service
sudo systemctl daemon-reload

rm -f /ruta/a/solarcost-web.env
rm -f /ruta/a/data/energy_costs.sqlite3
rm -rf /ruta/al/directorio/de/trabajo
```

## Uso diario

El bridge debe estar funcionando antes de iniciar la web.

Para ver logs:

```bash
sudo journalctl -u solarcost-web.service -f
```

Para reiniciarla:

```bash
sudo systemctl restart solarcost-web.service
```

Si prefieres ejecutarla manualmente:

```bash
cd /opt/solarcost/web
source .venv/bin/activate
sa_web run
```

La web queda por defecto en `http://127.0.0.1:8890`.

## API JSON (integraciones, ej. Home Assistant)

Ademas de la interfaz web, hay una API de solo lectura protegida por token,
pensada para integraciones automaticas que no pueden usar el login por cookie.

Se habilita definiendo `SA_COSTS_API_TOKEN` con un valor largo y unico
(si queda vacia, la API responde `404 api_disabled`).

Endpoints:

- `GET /api/current-period` — resumen del periodo de facturacion en curso.
- `GET /api/periods` — todos los periodos configurados.

Autenticacion por cabecera `Authorization: Bearer <token>` (o `?token=<token>`).

```bash
curl -H "Authorization: Bearer TU_TOKEN" http://127.0.0.1:8890/api/current-period
```

Devuelve importes (`total`, `energy_cost`, `fixed_total`, `tax_total`, `subtotal`),
consumos (`consumption_kwh`, `load_kwh`, `solar_pv_kwh`), `average_price_per_kwh`,
metadatos del periodo y los desgloses por seccion.

## Actualizacion

```bash
cd /opt/solarcost/web
source .venv/bin/activate
python -m pip install --upgrade solarcost-web
sudo systemctl restart solarcost-web.service
```

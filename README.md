# Solar Assistant Costs Web

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
python3 -m venv .venv
source .venv/bin/activate
pip install .
```

Bootstrap automatico desde el repo:

```bash
./init.sh
```

Tambien puedes instalarla directamente desde GitHub:

```bash
pipx install "git+https://github.com/gteijeiro/solar-assistant-costs-costs-web.git"
```

Y luego ejecutar el asistente:

```bash
sa-costs-web init
```

## Despliegue rapido en Raspberry Pi

1. Asegurate de tener el bridge funcionando en la misma maquina o accesible por red.
2. Clona el repo en la Pi.
3. Entra a la carpeta del proyecto.
4. Ejecuta `./init.sh`.
5. Responde el asistente interactivo y apunta `SA_COSTS_BRIDGE_URL` al bridge, por ejemplo `http://127.0.0.1:8765`.

Ejemplo:

```bash
git clone https://github.com/gteijeiro/solar-assistant-costs-costs-web.git
cd solar-assistant-costs-costs-web
./init.sh
```

Para ver logs:

```bash
sudo journalctl -u sa-costs-web.service -f
```

## Ejecucion

Primero asegúrate de tener el bridge levantado.

```bash
export SA_COSTS_BRIDGE_URL="http://127.0.0.1:8765"
export SA_COSTS_SECRET_KEY="GENERA_AQUI_UN_SECRETO_LARGO_Y_UNICO"
sa-costs-web
```

Tambien puedes usar el subcomando explicito:

```bash
sa-costs-web run
```

La web queda por defecto en:

- `http://127.0.0.1:8890`

## Primer inicio

- Al abrir la web por primera vez, si no existen usuarios, se muestra la pantalla de alta inicial.
- Ese primer usuario queda como administrador.
- Luego los administradores pueden crear mas usuarios, cambiar roles y deshabilitarlos.

## Variables disponibles

- `SA_COSTS_BRIDGE_URL`
- `SA_COSTS_BIND_HOST`
- `SA_COSTS_BIND_PORT`
- `SA_COSTS_DB_PATH`
- `SA_COSTS_SECRET_KEY`
- `SA_COSTS_LOG_LEVEL`
- `SA_COSTS_HTTP_TIMEOUT`

## Recomendacion de seguridad

- No subas `SA_COSTS_SECRET_KEY` real a GitHub.
- No subas la base `data/energy_costs.sqlite3` porque puede contener usuarios y configuracion real.
- Si usas `.env`, mantenlo fuera del repositorio.

## Expresiones de impuestos

Ejemplos soportados:

- `21% de total_factura`
- `5% de costo_energia`
- `1500`
- `0.03 * subtotal`

Variables disponibles:

- `costo_energia`
- `energia_electrica`
- `cargos_fijos`
- `cargos_fijos_servicio`
- `cargos_fijos_impuestos`
- `subtotal`
- `impuestos_acumulados`
- `total_servicio_energia`
- `iva_otros_conceptos`
- `total_factura`
- `consumo_kwh`
- `consumo_inversor_kwh`
- `consumo_compania_kwh`

Tambien puedes usar alias de cargos fijos o impuestos previos:

- `cargo_fijo + compensacion_mop`
- `(cargo_fijo + compensacion_mop + iva_21) * 10 / 100`

Si no defines alias, puedes usar el nombre normalizado en minusculas y con guion bajo. Si una referencia no existe en el periodo, vale `0`.

## Secciones del comprobante

Cada concepto fijo o por formula puede pertenecer a una de estas secciones:

- `Servicio de energia`: se suma junto con `Energia electrica` para formar `Total servicio de energia`.
- `IVA y otros conceptos`: se suma aparte y luego se agrega al total final de la factura.

## Regla de periodos

Cada periodo tiene solo fecha de inicio. El fin se calcula automaticamente:

- si existe un periodo siguiente, termina el dia anterior a ese inicio,
- si es el ultimo periodo, queda abierto hasta la fecha actual.

## Regla de tarifas mensuales

- las franjas de `Configuracion` funcionan como plantilla inicial,
- al crear un periodo nuevo, la app copia primero las tarifas del ultimo periodo anterior que ya tenga precios propios,
- si no existe un periodo anterior con tarifas, copia la plantilla,
- despues puedes editar las franjas del mes cuando llegue la factura real.

## Regla de cargos fijos mensuales

- los cargos fijos de `Configuracion` funcionan como plantilla inicial,
- al crear un periodo nuevo, la app copia primero los cargos fijos del ultimo periodo anterior que ya tenga valores propios,
- si no existe un periodo anterior con cargos fijos, copia la plantilla,
- despues puedes editar los importes del mes cuando llegue la factura real.

## Build y publicacion

Build local:

```bash
python -m pip install --upgrade build
python -m build
```

El repo incluye workflows de GitHub Actions para:

- CI en `push` y `pull_request`,
- publicacion manual a TestPyPI con `workflow_dispatch`,
- publicacion a PyPI al crear un tag `v*`.

# ConnexMonitor · Reconexiones

Servicio hermano de **ConnexMonitor**. ConnexMonitor solo desconecta; este servicio solo **reconecta**. Se coordinan a través de la misma base MySQL (`connex_circuits`).

- No desconecta, no marca línea base, no usa OpenAI.
- No toca `connex_baseline_pending`, `connex_contract_clients`, `connex_analysis_cache` ni `connex_meta`.
- Nunca borra ni reescribe un circuito en `disconnect_requested`.

## Estados de `connex_circuits`

| state | quién lo escribe | qué hace este servicio |
|---|---|---|
| (sin fila) = activo | bot de cortes / este servicio (borra la fila) | nada |
| `disconnect_requested` | bot de cortes | **ignorar** |
| `disconnected` | bot de cortes o línea base | candidato a reconectar si el servicio no tiene mora vencida |
| `reconnect_requested` | este servicio | vigilar el ticket: `Closed` → borrar fila; `Closed Incomplete`/`Deleted` → `disconnected` + `connex_failed` |

Las escrituras van **condicionadas al estado leído** (`UPDATE … WHERE state = %s`). Si el bot de cortes cambió la fila entretanto, no se pisa.

## Ciclo

1. Candado `connex_lock.name = 'reconnect_cycle'`. El bot de cortes usa `'cycle'`, así que no se bloquean entre sí.
2. Lista de clientes desde MySQL, sin usar `client.list`:
   ```sql
   SELECT DISTINCT client_id FROM connex_circuits
   WHERE state IN ('disconnected', 'reconnect_requested');
   ```
3. Por cliente (en paralelo, `CONNEX_CONCURRENCY`, por defecto 8):
   - `client.service_list` (`pack_type_select=4`, `metadata=1`).
   - Servicio inexistente, cancelado (`active` fuera de 1/2/3) o terminado → `DELETE` del circuito.
   - `reconnect_requested` → `support.ticket_get` y tipos de la cola (`support.ticket_type_list`).
   - `disconnected`: se omite si es FiberX, si tiene `period=0` o si tiene pausa en `connex_failed (service_id, 'reconnect')`.
   - Facturas impagas (`client.invoice_list paid=0`) con `due_date < hoy` (zona America/Puerto_Rico). Las líneas se atribuyen a servicios por `packid`/`parentpack` con `client.invoice_get`, usando la caché `connex_invoice_packs` (TTL 90 días). Una línea cuenta si `unpaid > 0` y no está cancelada.
   - Se reconecta solo si **ese servicio tiene 0 facturas vencidas**. Lo que el cliente deba por otros conceptos no importa.
4. Un ticket por cliente con todos los servicios a reconectar:
   - Asunto: `Reconexión de servicios - Cliente {id} - Servicio {sid}`, o `… - N servicios`.
   - Cola: `CONNEX_QUEUE_RECONNECT`, si no `SUPPORT_QUEUE_ID`, si no `57`.
   - No se crea si ya hay un ticket abierto en esa cola con menos de `CONNEX_OPEN_TICKET_MAX_AGE_DAYS` días **cuyo asunto mencione reconex/reconect/reconnect**. Los tickets de corte de la misma cola no bloquean.
   - Si el circuito venía de la línea base, el motivo añade: *(marcado como desconectado en la línea base inicial; confirmar estado físico)*.
   - Después del POST, el circuito pasa a `reconnect_requested` con `ticket_id`, `queue`, `previous_state=disconnected`, `reason`, `requested_at` y el `source` original. Se registra en `connex_requests` con `action='reconnect'`.

## Dry-run (por defecto)

`CONNEX_DRY_RUN=1` (por defecto) **no crea tickets y no escribe en `connex_circuits`, `connex_failed` ni `connex_requests`**. Solo registra en el log y en el resumen lo que haría. Lo único que sí escribe es la caché compartida `connex_invoice_packs` y el candado. Con `CONNEX_DRY_RUN=0` el servicio funciona de verdad.

## Uso

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env   # rellenar; .env no se commitea
.venv/bin/python run_reconnect.py --once                 # un ciclo
.venv/bin/python run_reconnect.py --once --client-id 123 # solo un cliente (debe tener circuitos en MySQL)
.venv/bin/python run_reconnect.py --once --max-clients 20
.venv/bin/python run_reconnect.py --interval 3600        # servicio continuo
```

Las variables ya exportadas en el entorno tienen prioridad sobre `.env`.

## Tests

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Usan un store en memoria y un Ubersmith falso; no necesitan red ni MySQL.

## Estructura

- [`core/ubersmith_client.py`](core/ubersmith_client.py): cliente de la API 2.0, copiado de ConnexMonitor.
- [`core/rules.py`](core/rules.py): reglas puras (atribución de facturas, FiberX, decisión, asunto/cuerpo).
- [`core/store.py`](core/store.py): MySQL con conexiones thread-local, reintentos y transiciones condicionadas.
- [`core/bot.py`](core/bot.py): ciclo de reconexión.
- [`run_reconnect.py`](run_reconnect.py): CLI.

# ConnexMonitor · Reconexiones

Servicio hermano de **ConnexMonitor**. ConnexMonitor solo desconecta; este servicio solo **reconecta**. Se coordinan a través de la misma base MySQL (`connex_circuits`, `connex_services`, `connex_invoice_packs`, `connex_failed`, `connex_requests`, `connex_lock`).

- No desconecta, no marca línea base, no usa OpenAI.
- No toca `connex_baseline_pending`, `connex_contract_clients`, `connex_analysis_cache` ni `connex_meta`.
- Nunca borra ni reescribe un circuito en `disconnect_requested`.

## Estados y coordinación con `connex_services`

`connex_services` es el padrón plano de todos los circuitos evaluables (`conectado` | `desconectado`), mientras que `connex_circuits` guarda el detalle de los que no están activos.

| Evento / Estado | `connex_circuits` | `connex_services.estado` |
|---|---|---|
| Candidato a reconectar (`disconnected`, mora del servicio = 0) | No cambies aún | `desconectado` |
| Ticket de reconexión creado | `reconnect_requested` + `ticket_id`, `previous_state=disconnected` (conserva `source=baseline`) | `desconectado` |
| Ticket Closed (completo/OK) | `DELETE` fila (`active`) | `conectado` (`set_service_status`) |
| Closed Incomplete / Deleted | Vuelve a `disconnected` + `mark_failed(service_id, 'reconnect', ...)` | `desconectado` |
| Servicio cancelado / no evaluable | `DELETE` circuito | Se elimina de `connex_services` (al sincronizar cliente) |
| `disconnect_requested` (bot de cortes) | **Ignorar**, no tocar | `desconectado` (nunca marcar conectado) |
| Dry-run (no POST) | No escribe `reconnect_requested` | No pone conectado; sigue `desconectado` |

Las escrituras en `connex_circuits` van **condicionadas al estado leído** (`UPDATE … WHERE state = %s`). Si el bot de cortes cambió la fila entretanto, no se pisa.

Tras procesar un cliente, si se listaron sus servicios, se llama a `sync_client_services` con el estado real (visión inspectores): `reconnect_requested` / `disconnected` / `disconnect_requested` / Ubersmith `active=3` → `desconectado`; activo sin fila de circuito → `conectado`.

## Ciclo

1. **Candado obligatorio**: `connex_lock.name = 'reconnect_cycle'`. El bot de cortes usa `'cycle'`, permitiendo que ambos servicios operen en paralelo sin bloquearse.
2. **Arranque desde MySQL**: Sin escaneo masivo de `client.list`:
   ```sql
   SELECT DISTINCT client_id FROM connex_circuits
   WHERE state IN ('disconnected', 'reconnect_requested');
   ```
3. **Por cliente** (en paralelo, `CONNEX_CONCURRENCY`, por defecto 8):
   - `client.service_list` (`pack_type_select=4`, `metadata=1`).
   - Servicio inexistente, cancelado (`active` fuera de 1/2/3) o terminado → `DELETE` del circuito.
   - `reconnect_requested` → Sincroniza con `support.ticket_get` y tipos de la cola (`support.ticket_type_list`).
   - `disconnected`: se omite si es FiberX, si tiene `period=0` o si tiene pausa en `connex_failed (service_id, 'reconnect')`.
   - Facturas impagas (`client.invoice_list paid=0`) con `due_date < hoy` (zona America/Puerto_Rico). Las líneas se atribuyen a servicios por `packid`/`parentpack` con `client.invoice_get`, usando la caché `connex_invoice_packs` (TTL 90 días). Una línea cuenta si `unpaid > 0` y no está cancelada.
   - Se reconecta solo si **ese servicio tiene 0 facturas vencidas**. Lo que el cliente deba por otros conceptos no importa.
4. **Un ticket por cliente**:
   - Asunto: `Reconexión de servicios - Cliente {id} - Servicio {sid}`, o `… - N servicios`.
   - Cola: `CONNEX_QUEUE_RECONNECT`, si no `SUPPORT_QUEUE_ID`, si no `57`.
   - No se crea si ya hay un ticket abierto en esa cola con menos de `CONNEX_OPEN_TICKET_MAX_AGE_DAYS` días **cuyo asunto mencione reconex/reconect/reconnect**.
   - Si el circuito venía de la línea base, el motivo añade: *(marcado como desconectado en la línea base inicial; confirmar estado físico)*.
   - Tras el POST: pasa a `reconnect_requested`, se registra en `connex_requests` y se mantiene `desconectado` en `connex_services`.
5. **Sincronización final de servicios**:
   - Actualiza `connex_services` para todos los servicios evaluables y purga los que ya no existen.

## Dry-run (por defecto)

`CONNEX_DRY_RUN=1` (por defecto) **no crea tickets y no escribe en `connex_circuits`, `connex_services`, `connex_failed` ni `connex_requests`**. Solo registra en el log y en el resumen lo que haría. Con `CONNEX_DRY_RUN=0` el servicio ejecuta las escrituras reales.

## Uso

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env   # rellenar credenciales; .env no se commitea
.venv/bin/python run_reconnect.py --once                 # un ciclo
.venv/bin/python run_reconnect.py --once --client-id 123 # solo un cliente (debe tener circuitos en MySQL)
.venv/bin/python run_reconnect.py --once --max-clients 20
.venv/bin/python run_reconnect.py --interval 3600        # servicio continuo
```

## Tests

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Usan un store en memoria y un cliente Ubersmith simulado; no requieren red ni base de datos MySQL real.

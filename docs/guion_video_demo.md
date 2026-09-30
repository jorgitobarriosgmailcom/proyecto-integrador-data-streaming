# Guion de video / demostración (3 a 4 minutos)

**Integrante:** Jorge Ismael Barrios Leon

## 0:00-0:35 - Caso y arquitectura
Mostrar `docs/arquitectura_integrador.png`. Explicar: productor sintético -> Kafka `payments.raw` -> Beam -> SQLite idempotente -> consumidor. Métricas por comercio/minuto: monto confirmado, cantidad confirmada y cantidad de alto riesgo.

## 0:35-1:10 - Contrato y Kafka
Abrir `data/event_example.json`. Señalar `event_id`, `key=merchant_id`, `event_time`, `schema_version=1.0` y payload. Explicar 3 particiones y orden/afinidad por comercio.

## 1:10-2:10 - Ejecución end-to-end
Ejecutar `make demo`. Mostrar `docker compose ps`, producción de eventos y logs del pipeline. Resaltar un evento normal, el duplicado y el evento fuera de orden. Mostrar `DUPLICATE_DROPPED` e `INVALID_EVENT`.

## 2:10-2:50 - Ventanas, lateness e idempotencia
Explicar ventana fija 60 s, lateness 120 s, panes EARLY/ON_TIME/LATE y acumulación. Ejecutar `make results` y mostrar la clave `merchant_id|window_start`; explicar que UPSERT reemplaza la revisión anterior y evita filas duplicadas por reintento.

## 2:50-3:25 - Pruebas y garantías
Mostrar `uv run pytest -q`, CI y pruebas TestPipeline/TestStream. Explicar la semántica: at-least-once + deduplicación + sink idempotente; no se promete exactly-once end-to-end.

## 3:25-3:45 - Límites y cierre
Mencionar límites: un broker, DirectRunner, SQLite local y horizonte finito de deduplicación. Cerrar señalando que una persona externa puede reproducir el proyecto con `make demo` siguiendo el README.

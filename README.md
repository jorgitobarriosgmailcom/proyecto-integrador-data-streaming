# Proyecto integrador - Data Streaming

**Caso:** monitoreo de pagos confirmados y riesgo por comercio en tiempo de evento.  
**Curso:** Streaming de datos y sus aplicaciones - Maestría en Inteligencia Artificial, FPUNA.  
**Integrante:** Jorge Ismael Barrios Leon.

## 1. Objetivo

Implementar un pipeline end-to-end reproducible:

`productor sintético -> Kafka (payments.raw) -> Apache Beam -> SQLite idempotente -> consumidor`

El sistema calcula, por comercio y por minuto de **event time**:

- monto total confirmado;
- cantidad de pagos confirmados;
- cantidad de pagos de alto riesgo (`fraud_score >= 0.80`).

La fuente sintética incluye eventos normales, un duplicado, un evento fuera de orden, un estado `PENDING` y un evento inválido para evidenciar comportamiento adverso.

## 2. Contrato de evento v1.0

Campos mínimos: `event_id`, `key`, `event_time`, `event_type`, `schema_version`, `payload`. La `key` es `merchant_id`, de modo que eventos del mismo comercio mantienen afinidad de partición y se reduce el costo de estado por clave.

Ejemplo: `data/event_example.json`.

## 3. Kafka

- tópico de entrada: `payments.raw`;
- 3 particiones, clave `merchant_id`;
- tópico auxiliar conceptual para inválidos: `payments.invalid` (los inválidos se registran en logs del pipeline en esta versión);
- productor con `enable.idempotence=true` y `acks=all`.

**Justificación:** tres particiones permiten paralelismo sin sobredimensionar un demo académico. La clave por comercio preserva afinidad y orden por entidad dentro de la partición. Debe observarse skew si un comercio concentra una fracción excesiva del tráfico.

## 4. Apache Beam

El pipeline usa `ReadFromKafka` (KafkaIO), valida contrato, asigna timestamp con `event_time`, filtra solo `CONFIRMED`, aplica ventanas fijas de 60 s, allowed lateness de 120 s, panes EARLY/ON_TIME/LATE en modo ACCUMULATING, deduplicación por `event_id` mediante estado por clave y timer de watermark, `CombinePerKey` y salida con metadatos de ventana/pane.

Los eventos inválidos se separan mediante side output y quedan visibles en logs con prefijo `INVALID_EVENT`.

## 5. Salida idempotente

SQLite actúa como sink consumible. La clave lógica es:

`merchant_id|window_start`

La escritura usa `INSERT ... ON CONFLICT DO UPDATE` y solo acepta una revisión cuyo `pane_index` sea igual o mayor al materializado. Reintentar la misma salida converge en una sola fila visible.

## 6. Semántica y límites

La solución declara **at-least-once en transporte/procesamiento**, con deduplicación por `event_id` dentro del horizonte de ventana + lateness e idempotencia en el sink. No se afirma exactly-once end-to-end. Un evento que reaparezca después de expirar el estado de deduplicación podría volver a ser procesado; el sink evita duplicar una misma ventana, pero una corrección histórica fuera del horizonte requeriría reproceso controlado.

## 7. Ejecutar

Prerrequisitos: Docker Desktop/Engine con Compose.

### Demostración completa

```bash
make demo
```

Ese comando:

1. levanta Kafka y crea tópicos;
2. inicia Beam;
3. produce el escenario adverso;
4. consulta SQLite;
5. imprime logs del pipeline y guarda evidencia en `evidence/runtime/`.

### Paso a paso

```bash
make up
make produce
make results
make stop
```

## 8. Pruebas

```bash
uv sync
uv run pytest -q
uv run ruff check app tests
```

Cobertura: contrato, timestamp UTC, clave idempotente, deduplicación por clave/ventana, agregación, TestStream fuera de orden e idempotencia del upsert.

## 9. Evidencia requerida

Después de `make demo`, conservar:

- `evidence/runtime/e2e_*.log`;
- captura de `docker compose ps`;
- captura de `make results` mostrando filas materializadas;
- captura del log `DUPLICATE_DROPPED`;
- captura del log `INVALID_EVENT`;
- captura de CI en verde.

No se incluyen capturas fabricadas: la evidencia debe provenir de una ejecución real del repositorio.

## 10. Trade-offs

- **Latencia:** panes early cada 30 s entregan señal rápida, pero aumentan escrituras.
- **Completitud:** 120 s de lateness permite correcciones moderadas y limita crecimiento de estado.
- **Costo:** ventanas fijas son más económicas que deslizantes para la métrica por minuto.
- **Confiabilidad:** deduplicación y UPSERT atacan fallos distintos; la primera evita contar un evento repetido y el segundo evita duplicar el resultado lógico.

## 11. Integrante y contribución

**Jorge Ismael Barrios Leon:** diseño del caso, contrato de eventos, productor Kafka, pipeline Beam, deduplicación y timers, sink SQLite idempotente, pruebas, Docker Compose, documentación, evidencia y presentación.

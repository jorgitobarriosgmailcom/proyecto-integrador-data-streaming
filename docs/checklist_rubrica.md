# Checklist de rúbrica - Proyecto integrador

- [x] Caso de uso relevante y arquitectura end-to-end.
- [x] Diagrama consistente con la implementación.
- [x] Contrato versionado con event_id, key, event_time y payload.
- [x] Kafka con tópico de entrada, 3 particiones y key justificada.
- [x] Productor reproducible con duplicado, fuera de orden e inválido.
- [x] Beam lee desde KafkaIO.
- [x] Validación y side output/log de inválidos.
- [x] Transformaciones y CombinePerKey incremental.
- [x] Event time y ventana fija de 60 s.
- [x] Allowed lateness 120 s, triggers early/on-time/late y accumulating.
- [x] Deduplicación con estado por key/ventana y timer de expiración.
- [x] Sink SQLite idempotente con UPSERT y clave estable.
- [x] Semántica declarada sin sobreprometer exactly-once.
- [x] Pruebas unitarias, TestPipeline y TestStream.
- [x] Escenarios adversos: duplicado y fuera de orden.
- [x] Smoke test end-to-end automatizado con `make demo`.
- [x] Docker Compose, comandos de inicio/prueba/demo/stop.
- [x] Logs observables y carpeta evidence.
- [x] Documento técnico, README, diagrama y guion de video.
- [x] Integrante y contribución principal documentados.

## Evidencia pendiente de ejecución real antes de entregar
- [ ] Ejecutar `make demo` en Docker.
- [ ] Guardar `evidence/runtime/e2e_*.log`.
- [ ] Capturar `docker compose ps`.
- [ ] Capturar resultado del consumidor con `ROWS>0`.
- [ ] Capturar `DUPLICATE_DROPPED` e `INVALID_EVENT`.
- [ ] Ejecutar CI / pytest y conservar captura verde.
- [ ] Grabar video breve siguiendo `docs/guion_video_demo.md`.

# Evidencia de ejecución

Esta carpeta se completa con evidencia real. Ejecutar `make demo` para producir `runtime/e2e_*.log`.

Checklist antes de entregar:
- CI verde (pytest + ruff).
- `E2E_SMOKE_COMPLETED` en el log.
- al menos una línea `DUPLICATE_DROPPED`.
- al menos una línea `INVALID_EVENT`.
- tabla final del consumidor con `ROWS>0`.
- capturas de Docker Compose y resultados.

# Changelog

Todas las versiones notables del agente de AuraNode se documentan aquí.
El formato sigue [Keep a Changelog](https://keepachangelog.com/) y
[SemVer](https://semver.org/lang/es/).

## [1.0.0] — 2026-06-20

Primer release público del agente.

### Añadido
- Recolección de métricas: CPU, RAM/swap, disco, red (delta/s), load avg y top-10
  procesos (vía gopsutil).
- Conexión WebSocket al backend con reconexión exponencial (backoff 2s → 5min).
- Heartbeat y métricas con intervalos configurables desde el backend.
- Buffer offline persistente en disco (bbolt) y drenado al reconectar.
- Ejecución remota de comandos (`exec`) con timeout y output acotado.
- Motor de reglas local: condición + duración, cooldown y máximo por día.
- Port forwarding / túneles (Tipo 1 local-CLI, Tipo 2 remote y dest=CLI).
- Migraciones entre VPS (Tipo B: directorio, modo relay) con reanudación.
- Instalador con verificación SHA256, servicio systemd con hardening y Dockerfile.

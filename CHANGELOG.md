# Changelog

Todas las versiones notables del agente de AuraNode se documentan aquí.
El formato sigue [Keep a Changelog](https://keepachangelog.com/) y
[SemVer](https://semver.org/lang/es/).

## [1.2.1] — 2026-06-20

### Corregido
- **Deadlock de half-close en túneles:** al cerrarse una dirección del stream (p.ej.
  fin de la petición mientras la respuesta sigue fluyendo), el stream se eliminaba del
  mapa y los créditos (`tunnel_window`) de la dirección aún activa se perdían → el
  lector se quedaba sin crédito y la conexión se colgaba. Ahora el cierre sólo afecta a
  su dirección (half-close real) y el stream se elimina cuando ambas terminan.

## [1.2.0] — 2026-06-20

### Añadido / Mejorado
- **Control de flujo por créditos en los túneles** (port forwarding): cada dirección
  de cada stream tiene una ventana de bytes en vuelo; el receptor concede crédito
  (`tunnel_window`) al drenar y el emisor deja de leer su TCP local cuando se agota,
  aplicando backpressure real al origen. Antes, un consumidor sostenidamente lento
  saturaba el buffer y reseteaba el stream; ahora se frena sin perder bytes.
- Negociación de capacidad: el control de flujo sólo se activa si ambos extremos lo
  soportan (compatibilidad con versiones antiguas; fallback al modo previo).

## [1.1.0] — 2026-06-20

### Añadido
- **Comprobación de actualizaciones (check-and-notify):** el agente consulta cada
  6 h GitHub Releases y, si hay una versión más reciente, lo registra en log y avisa
  al backend (el panel muestra "actualización disponible"). El agente **no** se
  auto-reemplaza, para preservar el hardening del servicio.
- **Imágenes Docker multi-arch** en GHCR (`ghcr.io/koyere/auranode-agent`),
  `linux/amd64` y `linux/arm64`, publicadas automáticamente en cada release.

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

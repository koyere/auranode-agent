# Política de Seguridad — AuraNode Agent

La seguridad del agente es prioritaria: corre en los servidores de nuestros usuarios.
Este repositorio es **público y auditable** precisamente para que cualquiera pueda
revisar exactamente qué ejecuta el agente.

## Reportar una vulnerabilidad

- Email: **security@auranode.app**
- Acuse de recibo: **48 horas**
- Evaluación inicial: **7 días**

Por favor incluye pasos de reproducción y el impacto estimado. No abras un issue
público para vulnerabilidades.

### Safe harbor

No emprenderemos acciones legales contra investigadores de buena fe que sigan esta
política y eviten la destrucción de datos o la interrupción del servicio. Los reportes
válidos se reconocen públicamente (Hall of Fame) si el reportante lo desea.

## Alcance

**En scope:**
- El código del agente de este repositorio
- El script de instalación `install.sh`
- La plantilla de servicio systemd y el `Dockerfile`

**Fuera de scope:**
- Los VPS de los usuarios (configuración propia)
- El backend / panel de AuraNode (reportar igualmente a security@auranode.app)

## Garantías de seguridad del agente

- **Sin privilegios:** corre como usuario de sistema `auranode` (no root), con
  `NoNewPrivileges`, `ProtectSystem=strict`, `CapabilityBoundingSet=` vacío y límites
  de CPU/memoria en systemd.
- **Token protegido:** el token vive en `/etc/auranode/agent.env` con permisos `600`,
  nunca en argumentos de proceso (`ps aux`) ni en logs.
- **Comunicación cifrada:** TLS con verificación de certificado contra el backend.
  No existe modo `InsecureSkipVerify`.
- **Integridad del binario:** cada release incluye `checksums.txt` (SHA256) y el
  instalador verifica el hash antes de instalar.
- **Ejecución remota auditada:** los comandos provienen de una acción humana
  confirmada en el panel y quedan en el audit log; el daño está acotado por correr
  sin privilegios.

## Verificación manual del binario

```bash
VERSION=v1.0.0
ARCH=amd64
BASE=https://github.com/koyere/auranode-agent/releases/download/$VERSION
curl -fsSLO $BASE/auranode-agent_${VERSION#v}_linux_${ARCH}.tar.gz
curl -fsSLO $BASE/checksums.txt
sha256sum -c --ignore-missing checksums.txt
```

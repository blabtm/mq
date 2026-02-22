# MQ

MQ is a fully compliant, embeddable MQTTv5 broker with integrated full-duplex VCAS support. For
information about MQTT broker inside MQ, see [Mochi MQTT](https://github.com/mochi-mqtt/server).

## Quick Start

### Configuration

MQ expects a YAML configuration file. You can specify the configuration location with the
`MQ_CONFIG_PATH` environment variable or the `-conf` flag; the environment variable takes
precedence. Here is an example:

```yaml
listeners:
  - type: "tcp"
    id: "tcp1"
    address: ":1883"
  - type: "ws"
    id: "ws1"
    address: ":1882"
  - type: "sysinfo"
    id: "stats"
    address: ":1880"
  - type: "tcp" # vcas listener, id must be set to `vcas`
    id: "vcas"
    address: ":20041"
hooks:
  auth:
    allow_all: true
options:
  vcas: true # enable vcas
  db_addr: postgresql://user:pass@host:port/name # vcas db address
logging:
  level: INFO
```

Default configuration location (if not specified explicitly) is `/etc/mq/config.yaml`.

### Build & Run

```bash
go build -o mq ./cmd
./mq -conf config.yaml
```

### Docker

You can pull and run the MQ image from the organization's GitHub repo:

```bash
docker pull ghcr.io/blabtm/v2k.mq:latest
docker run                                          \
    -n mq                                           \
    -v config.yaml:/etc/mq/config.yaml              \
    ghcr.io/blabtm/v2k.mq:latest
```

### Platform

In production, MQ will be deployed as part of the [v2k](https://github.com/blabtm/v2k) platform.
Configurations will be registered as Docker secrets and confidential information (e.g. database
credentials) will be injected automatically.

## Contributing

MQ is a submodule of the [v2k](https://github.com/blabtm/v2k) platform and it's recommended to
develop MQ as part of it. The platform already contains a devcontainer with all the necessary tools
and dependencies as well as development environment with required services (e.g. database). All you
need is Docker Engine installed on your machine. See appropriate instructions at
[v2k](https://github.com/blabtm/v2k) for details.

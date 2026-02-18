# Kernel

A modular and sandboxed Discord bot framework built with [hikari](https://github.com/hikari-py/hikari), [hikari-arc](https://github.com/hypergonial/hikari-arc), and [hikari-miru](https://github.com/hypergonial/hikari-miru).

## Usage

### Slash Commands

#### Module

- `/bot module load <url>` — Load a module from a remote HTTPS Git repository
- `/bot module unload <module>` — Unload and remove a module
- `/bot module update <module>` — Update a module to its latest version
- `/bot module info <module>` — View information about a module
- `/bot module list` — List all currently loaded modules

#### Kernel

- `/bot kernel info` — View information about the kernel
- `/bot kernel update` — Update the kernel to the latest version

#### Debug

- `/bot debug download` — Download the current runtime
- `/bot debug restart` — Restart the bot
- `/bot debug export` — Export files

### Deployment

#### Requirements

- Python 3.14
- [Firejail](https://github.com/netblue30/firejail)
- [Node.js](https://github.com/nodesource/distributions)
- [PM2](https://github.com/Unitech/pm2)

#### Installation

```bash
git clone https://github.com/Discord-Bot-Framework/Kernel.git
cd Kernel
```

#### Configuration

1. Copy the environment template:

   ```bash
   cp template.env .env
   ```

2. Update `.env` with your values:

   | Variable   | Description                       | Default |
   | ---------- | --------------------------------- | ------- |
   | `TOKEN`    | Discord bot token                 | —       |
   | `GUILD_ID` | Guild ID for command registration | `0`     |
   | `ROLE_ID`  | Role ID for command access        | `0`     |

#### Running the Bot

Use the PM2 control script:

```bash
./pm2.sh start
```

To apply configuration changes:

```bash
./pm2.sh delete
# Edit your .env file
./pm2.sh start
```

### Project Structure

| Path                  | Description                        |
| --------------------- | ---------------------------------- |
| `.env`                | Environment variable configuration |
| `requirements.txt`    | Python dependencies                |
| `ecosystem.config.js` | PM2 configuration                  |
| `pm2.sh`              | PM2 script                         |
| `launch.sh`           | Launch script                      |
| `firejail.profile`    | Firejail configuration             |
| `main.py`             | Entry point                        |
| `main.log`            | Log output                         |
| `src/`                | Framework                          |
| `extensions/`         | User-defined modules               |

### Module Structure

Each module must include:

- `main.py` — Module entry point

#### Example

See: [Discord-Bot-Framework/Example](https://github.com/Discord-Bot-Framework/Example)

#### Dependencies

If a module requires additional Python packages, include a `requirements.txt` file in the module directory.

## Licenses

- [retr0-init/Discord-Bot-Framework-Kernel](https://github.com/retr0-init/Discord-Bot-Framework-Kernel) (GPL-3.0)

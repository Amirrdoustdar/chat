# Real-Time Chat Platform

A production-grade real-time chat platform built with **Python sockets**, **MySQL**, and **bcrypt** — featuring private messaging, group chat, media transfers, and TTL (self-destructing) messages.

---

## Architecture

```
chat_platform/
├── server/
│   └── server.py          # Multi-threaded TCP server — ClientRegistry + ClientHandler
├── client/
│   └── client.py          # Interactive CLI client with ANSI colours
├── auth/
│   └── auth.py            # bcrypt hashing, token generation, login/register/logout
├── database/
│   ├── db.py              # Connection pool (mysql-connector, thread-safe)
│   ├── repository.py      # Repository layer: UserRepo, SessionRepo, GroupRepo, MessageRepo, MediaRepo
│   └── schema.sql         # Full MySQL schema with TTL stored procedure & scheduler event
├── media/
│   └── media_handler.py   # Base64 encode/decode, SHA-256 integrity checks, disk storage
├── utils/
│   └── protocol.py        # Length-prefixed JSON wire protocol + MsgType enum
└── tests/
    ├── test_auth.py
    ├── test_protocol.py
    └── test_media.py
```

---

## Features

| Feature | Details |
|---|---|
| **Secure Auth** | bcrypt (12 rounds) password hashing; random 64-hex session tokens stored in MySQL |
| **Private DMs** | Real-time delivery to online recipients; full history with pagination |
| **Group Chat** | Create, join, leave; broadcast to all online members; group history |
| **Media Transfer** | Upload any file as base64; SHA-256 integrity verification; retrieve by `media_id` |
| **TTL Messages** | `ttl_seconds` field; MySQL Event Scheduler purges expired rows every minute |
| **Read Receipts** | Per-message, per-user read tracking |
| **Presence** | Online/away/offline status; broadcast on connect/disconnect |
| **Protocol** | 4-byte length-prefixed UTF-8 JSON frames over TCP |

---

## Quick Start

### 1. MySQL Setup

```sql
-- as MySQL root:
CREATE USER 'chat_user'@'localhost' IDENTIFIED BY 'change_me';
GRANT ALL PRIVILEGES ON chat_platform.* TO 'chat_user'@'localhost';
FLUSH PRIVILEGES;
```

```bash
mysql -u root -p < database/schema.sql
```

### 2. Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Environment

```bash
cp .env.example .env
# Edit .env — at minimum set DB_PASSWORD
```

### 4. Start the server

```bash
cd chat_platform
python -m server.server
# or:  python server/server.py
```

### 5. Start a client (new terminal)

```bash
python client/client.py
```

---

## Client Usage

```
guest> register alice alice@example.com MyP@ss123
guest> login alice MyP@ss123

alice> dm 2 Hey Bob!
alice> dm-ttl 2 30 This disappears in 30 seconds
alice> history 2

alice> group-create DevTeam Backend engineers
alice> group-join 1
alice> group-send 1 Hello everyone!
alice> group-send-ttl 1 60 This vanishes in a minute

alice> upload /path/to/photo.png          # returns media_id
alice> dm-img 2 5                         # send media_id=5 as DM
alice> fetch 5                            # saves to ./downloads/

alice> search bob
alice> delete 42
alice> ping
alice> logout
alice> quit
```

---

## Running Tests

```bash
# from chat_platform/ directory
pytest tests/ -v --tb=short
```

Tests mock all DB/IO — no MySQL connection required.

---

## Wire Protocol

Every packet on the wire:

```
┌──────────────┬─────────────────────────────────┐
│  4 bytes     │  N bytes                        │
│  length (LE) │  UTF-8 JSON payload             │
└──────────────┴─────────────────────────────────┘
```

All packets carry a `"type"` field matching a `MsgType` enum value, plus an `"ok": true/false` flag on responses.

---

## Database Schema Summary

| Table | Purpose |
|---|---|
| `users` | Accounts with bcrypt hashes, status, timestamps |
| `sessions` | Token-based sessions with expiry |
| `groups_` | Group metadata and owner |
| `group_members` | Many-to-many with role (owner/admin/member) |
| `messages` | DMs and group messages; `expires_at` for TTL |
| `media_files` | File metadata + SHA-256 checksum |
| `read_receipts` | Per-message, per-user read tracking |
| `events` | Audit log (JSON payload) |

---

## Security Notes

- Passwords are never stored in plaintext; bcrypt with cost factor 12.
- Session tokens are 32-byte cryptographically random hex strings.
- Media files are validated by MIME type and size before storage.
- Each media file's integrity is verified with SHA-256 on every read.
- TTL messages are hard-deleted by the MySQL Event Scheduler.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | MySQL host |
| `DB_PORT` | `3306` | MySQL port |
| `DB_NAME` | `chat_platform` | Database name |
| `DB_USER` | `chat_user` | MySQL user |
| `DB_PASSWORD` | `change_me` | MySQL password |
| `DB_POOL_SIZE` | `10` | Connection pool size |
| `SERVER_HOST` | `0.0.0.0` | Server bind address |
| `SERVER_PORT` | `9000` | Server TCP port |
| `SESSION_TTL_HOURS` | `24` | Session lifetime |
| `MEDIA_ROOT` | `media_store` | Media storage directory |
| `MAX_FILE_MB` | `20` | Max upload size in MB |

-- ============================================================
-- Chat Platform — PostgreSQL Schema v2
-- Changes from v1:
--   1. chat_key (conversation_id) for every DM
--   2. invite_code system for first login
--   3. Yahoo-style public groups
--   4. MinIO media storage (path-based)
--   5. Full async PostgreSQL (asyncpg)
-- ============================================================

-- ─── Extensions ──────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- for full-text search

-- ─── INVITE CODES ────────────────────────────────────────────
-- Admin creates these before users can register
CREATE TABLE IF NOT EXISTS invite_codes (
    id           SERIAL PRIMARY KEY,
    code         VARCHAR(32)  NOT NULL UNIQUE,
    created_by   INT          DEFAULT NULL,  -- NULL = system generated
    used_by      INT          DEFAULT NULL,  -- who used it
    used_at      TIMESTAMPTZ  DEFAULT NULL,
    expires_at   TIMESTAMPTZ  DEFAULT NULL,  -- NULL = never expires
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Seed with initial admin invite code
INSERT INTO invite_codes (code, created_by) VALUES ('ADMIN-INVITE-2024', NULL)
ON CONFLICT DO NOTHING;

-- ─── USERS ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(50)  NOT NULL UNIQUE,
    email         VARCHAR(120) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    system_id     VARCHAR(64)  DEFAULT NULL,   -- invite code used
    avatar_key    VARCHAR(512) DEFAULT NULL,   -- MinIO object key
    status        VARCHAR(10)  NOT NULL DEFAULT 'offline'
                  CHECK (status IN ('online','away','offline')),
    is_verified   BOOLEAN      NOT NULL DEFAULT FALSE,
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_seen     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_status   ON users(status);

-- ─── SESSIONS ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    id         SERIAL PRIMARY KEY,
    user_id    INT          NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token      VARCHAR(512) NOT NULL UNIQUE,
    ip_address VARCHAR(45)  DEFAULT NULL,
    user_agent TEXT         DEFAULT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_token   ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

-- ─── CONVERSATIONS (Chat Key) ─────────────────────────────────
-- Every DM gets a unique conversation_id (chat key)
CREATE TABLE IF NOT EXISTS conversations (
    id           UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_a       INT          NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    user_b       INT          NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_msg_at  TIMESTAMPTZ  DEFAULT NULL,
    UNIQUE (user_a, user_b),
    CHECK (user_a < user_b)   -- enforce canonical ordering
);

CREATE INDEX IF NOT EXISTS idx_conv_user_a ON conversations(user_a);
CREATE INDEX IF NOT EXISTS idx_conv_user_b ON conversations(user_b);

-- ─── GROUPS ───────────────────────────────────────────────────
-- Yahoo Messenger style — public groups with invite links
CREATE TABLE IF NOT EXISTS groups_ (
    id           SERIAL PRIMARY KEY,
    name         VARCHAR(80)  NOT NULL,
    description  TEXT         DEFAULT NULL,
    topic        VARCHAR(140) DEFAULT NULL,     -- like Yahoo groups topic
    owner_id     INT          NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    is_public    BOOLEAN      NOT NULL DEFAULT TRUE,   -- public = discoverable
    invite_code  VARCHAR(16)  NOT NULL UNIQUE,          -- join link
    avatar_key   VARCHAR(512) DEFAULT NULL,             -- MinIO object key
    max_members  INT          NOT NULL DEFAULT 500,
    member_count INT          NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_groups_owner    ON groups_(owner_id);
CREATE INDEX IF NOT EXISTS idx_groups_public   ON groups_(is_public) WHERE is_public = TRUE;
CREATE INDEX IF NOT EXISTS idx_groups_name_trgm ON groups_ USING gin(name gin_trgm_ops);

-- ─── GROUP MEMBERS ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS group_members (
    group_id   INT         NOT NULL REFERENCES groups_(id) ON DELETE CASCADE,
    user_id    INT         NOT NULL REFERENCES users(id)   ON DELETE CASCADE,
    role       VARCHAR(10) NOT NULL DEFAULT 'member'
               CHECK (role IN ('owner','admin','member')),
    joined_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_banned  BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (group_id, user_id)
);

-- ─── MESSAGES ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS messages (
    id              BIGSERIAL    PRIMARY KEY,
    -- routing
    conversation_id UUID         DEFAULT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    group_id        INT          DEFAULT NULL REFERENCES groups_(id) ON DELETE CASCADE,
    sender_id       INT          NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    -- content
    msg_type        VARCHAR(10)  NOT NULL DEFAULT 'text'
                    CHECK (msg_type IN ('text','image','file','system')),
    body            TEXT         DEFAULT NULL,
    media_key       VARCHAR(512) DEFAULT NULL,   -- MinIO object key
    reply_to_id     BIGINT       DEFAULT NULL,   -- reply threading
    -- TTL
    ttl_seconds     INT          DEFAULT NULL,
    expires_at      TIMESTAMPTZ  DEFAULT NULL,
    -- meta
    is_edited       BOOLEAN      NOT NULL DEFAULT FALSE,
    is_deleted      BOOLEAN      NOT NULL DEFAULT FALSE,
    sent_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- reactions stored as JSONB: {"👍": [1,2,3], "❤️": [4]}
    reactions       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT chk_routing CHECK (
        (conversation_id IS NOT NULL AND group_id IS NULL) OR
        (conversation_id IS NULL     AND group_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_msg_conv    ON messages(conversation_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_msg_group   ON messages(group_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_msg_expires ON messages(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_msg_search  ON messages USING gin(body gin_trgm_ops);

-- ─── MESSAGE EDIT HISTORY ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS message_edits (
    id         BIGSERIAL    PRIMARY KEY,
    message_id BIGINT       NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    old_body   TEXT         NOT NULL,
    edited_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ─── READ RECEIPTS ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS read_receipts (
    message_id BIGINT      NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    user_id    INT         NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    read_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_id, user_id)
);

-- ─── MEDIA FILES ──────────────────────────────────────────────
-- MinIO-based storage — only metadata in DB
CREATE TABLE IF NOT EXISTS media_files (
    id           BIGSERIAL    PRIMARY KEY,
    uploader_id  INT          NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    object_key   VARCHAR(512) NOT NULL UNIQUE,  -- MinIO key
    bucket       VARCHAR(64)  NOT NULL DEFAULT 'chat-media',
    filename     VARCHAR(255) NOT NULL,
    mime_type    VARCHAR(100) NOT NULL,
    file_size    BIGINT       NOT NULL,
    checksum     CHAR(64)     NOT NULL,         -- SHA-256
    uploaded_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ─── USER BLOCKS ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_blocks (
    blocker_id INT         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    blocked_id INT         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (blocker_id, blocked_id)
);

-- ─── EVENT LOG ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    id         BIGSERIAL    PRIMARY KEY,
    user_id    INT          DEFAULT NULL,
    event_type VARCHAR(60)  NOT NULL,
    payload    JSONB        DEFAULT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id, event_type);

-- ─── TTL CLEANUP FUNCTION ─────────────────────────────────────
CREATE OR REPLACE FUNCTION purge_expired_messages()
RETURNS void AS $$
BEGIN
    UPDATE messages
    SET is_deleted = TRUE
    WHERE expires_at IS NOT NULL
      AND expires_at < NOW()
      AND is_deleted = FALSE;
END;
$$ LANGUAGE plpgsql;

-- ─── HELPER: get or create conversation ───────────────────────
CREATE OR REPLACE FUNCTION get_or_create_conversation(uid_a INT, uid_b INT)
RETURNS UUID AS $$
DECLARE
    v_id UUID;
    a INT := LEAST(uid_a, uid_b);
    b INT := GREATEST(uid_a, uid_b);
BEGIN
    SELECT id INTO v_id FROM conversations WHERE user_a = a AND user_b = b;
    IF NOT FOUND THEN
        INSERT INTO conversations (user_a, user_b) VALUES (a, b)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Real-Time Chat Platform - MySQL/MariaDB Schema
-- Docker entrypoint runs this automatically on first start
-- ============================================================

USE chat_platform;

-- ─── USERS ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username      VARCHAR(50)  NOT NULL UNIQUE,
    email         VARCHAR(120) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    avatar        MEDIUMBLOB   DEFAULT NULL,
    status        ENUM('online','away','offline') NOT NULL DEFAULT 'offline',
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active     TINYINT(1)   NOT NULL DEFAULT 1,
    INDEX idx_username (username),
    INDEX idx_status   (status)
) ENGINE=InnoDB;

-- ─── SESSIONS ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    id         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id    INT UNSIGNED NOT NULL,
    token      VARCHAR(512) NOT NULL UNIQUE,
    ip_address VARCHAR(45)  DEFAULT NULL,
    user_agent TEXT         DEFAULT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME     NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_token   (token(64)),
    INDEX idx_user_id (user_id)
) ENGINE=InnoDB;

-- ─── GROUPS ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS groups_ (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(80)  NOT NULL,
    description VARCHAR(255) DEFAULT NULL,
    owner_id    INT UNSIGNED NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active   TINYINT(1)   NOT NULL DEFAULT 1,
    FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE RESTRICT,
    INDEX idx_owner (owner_id)
) ENGINE=InnoDB;

-- ─── GROUP MEMBERS ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS group_members (
    group_id   INT UNSIGNED NOT NULL,
    user_id    INT UNSIGNED NOT NULL,
    role       ENUM('owner','admin','member') NOT NULL DEFAULT 'member',
    joined_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id) REFERENCES groups_(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)  REFERENCES users(id)   ON DELETE CASCADE
) ENGINE=InnoDB;

-- ─── MESSAGES ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS messages (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sender_id    INT UNSIGNED    NOT NULL,
    recipient_id INT UNSIGNED    DEFAULT NULL,
    group_id     INT UNSIGNED    DEFAULT NULL,
    msg_type     ENUM('text','image','file','system') NOT NULL DEFAULT 'text',
    body         TEXT            DEFAULT NULL,
    media_id     BIGINT UNSIGNED DEFAULT NULL,
    ttl_seconds  INT UNSIGNED    DEFAULT NULL,
    expires_at   DATETIME        DEFAULT NULL,
    sent_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted   TINYINT(1)      NOT NULL DEFAULT 0,
    FOREIGN KEY (sender_id)    REFERENCES users(id)   ON DELETE RESTRICT,
    FOREIGN KEY (recipient_id) REFERENCES users(id)   ON DELETE SET NULL,
    FOREIGN KEY (group_id)     REFERENCES groups_(id) ON DELETE CASCADE,
    INDEX idx_dm      (sender_id, recipient_id, sent_at),
    INDEX idx_group   (group_id, sent_at),
    INDEX idx_expires (expires_at)
) ENGINE=InnoDB;

-- ─── MEDIA FILES ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS media_files (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    uploader_id  INT UNSIGNED    NOT NULL,
    filename     VARCHAR(255)    NOT NULL,
    mime_type    VARCHAR(100)    NOT NULL,
    file_size    INT UNSIGNED    NOT NULL,
    storage_path VARCHAR(512)    NOT NULL,
    checksum     CHAR(64)        NOT NULL,
    uploaded_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (uploader_id) REFERENCES users(id) ON DELETE RESTRICT,
    INDEX idx_uploader (uploader_id)
) ENGINE=InnoDB;

-- ─── READ RECEIPTS ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS read_receipts (
    message_id BIGINT UNSIGNED NOT NULL,
    user_id    INT UNSIGNED    NOT NULL,
    read_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (message_id, user_id),
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)    REFERENCES users(id)    ON DELETE CASCADE
) ENGINE=InnoDB;

-- ─── EVENT LOG ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id    INT UNSIGNED DEFAULT NULL,
    event_type VARCHAR(60)  NOT NULL,
    payload    JSON         DEFAULT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_event (user_id, event_type)
) ENGINE=InnoDB;

-- ─── TTL CLEANUP PROCEDURE ────────────────────────────────────
CREATE PROCEDURE IF NOT EXISTS purge_expired_messages()
BEGIN
    DELETE FROM messages
    WHERE expires_at IS NOT NULL
      AND expires_at < NOW()
      AND is_deleted = 0;
END;

-- ─── SCHEDULED EVENT ──────────────────────────────────────────
CREATE EVENT IF NOT EXISTS evt_purge_ttl
    ON SCHEDULE EVERY 1 MINUTE
    DO CALL purge_expired_messages();

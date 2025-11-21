-- ==================== INIT DATABASE FOR AI AGENT ====================
-- This script runs automatically when PostgreSQL container starts

-- Tạo bảng chat_messages để lưu lịch sử trò chuyện
CREATE TABLE IF NOT EXISTS chat_messages (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at)
);

-- Tạo bảng checkpoints cho LangGraph persistence
-- (LangGraph sẽ tự tạo các bảng này, nhưng để chắc chắn ta tạo trước)
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id VARCHAR(255) NOT NULL,
    checkpoint_ns VARCHAR(255) NOT NULL DEFAULT '',
    checkpoint_id VARCHAR(255) NOT NULL,
    parent_checkpoint_id VARCHAR(255),
    type VARCHAR(128),
    checkpoint JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);

CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id VARCHAR(255) NOT NULL,
    checkpoint_ns VARCHAR(255) NOT NULL DEFAULT '',
    channel VARCHAR(128) NOT NULL,
    version VARCHAR(128) NOT NULL,
    type VARCHAR(128) NOT NULL,
    blob BYTEA,
    PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
);

CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id VARCHAR(255) NOT NULL,
    checkpoint_ns VARCHAR(255) NOT NULL DEFAULT '',
    checkpoint_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    idx INTEGER NOT NULL,
    channel VARCHAR(128) NOT NULL,
    type VARCHAR(128),
    blob BYTEA NOT NULL,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_checkpoints_thread ON checkpoints(thread_id, checkpoint_ns);
CREATE INDEX IF NOT EXISTS idx_checkpoint_blobs_thread ON checkpoint_blobs(thread_id, checkpoint_ns);
CREATE INDEX IF NOT EXISTS idx_checkpoint_writes_thread ON checkpoint_writes(thread_id, checkpoint_ns, checkpoint_id);

-- Insert initial system message (optional)
INSERT INTO chat_messages (user_id, role, content, created_at)
VALUES ('system', 'system', 'AI Agent initialized successfully', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Database initialized successfully for AI Agent';
END $$;

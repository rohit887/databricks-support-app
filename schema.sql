-- schema.sql — reference only. Run this manually against your Lakebase Postgres
-- database (e.g. via psql or the Databricks SQL editor) before deploying the app.

CREATE TABLE IF NOT EXISTS tickets (
    ticket_id   BIGSERIAL PRIMARY KEY,
    title       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open'
                CHECK (status IN ('open','in_progress','resolved')),
    priority    TEXT NOT NULL DEFAULT 'medium'
                CHECK (priority IN ('low','medium','high')),
    created_by  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ticket_messages (
    message_id   BIGSERIAL PRIMARY KEY,
    ticket_id    BIGINT NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
    message_text TEXT NOT NULL,
    author       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_ticket ON ticket_messages(ticket_id);

-- ---------------------------------------------------------------------------
-- Seed data: 3 tickets across at least two statuses, 2+ messages per ticket.
-- ---------------------------------------------------------------------------

INSERT INTO tickets (title, status, priority, created_by) VALUES
    ('Cluster fails to start in workspace', 'open',        'high',   'alice@example.com'),
    ('Dashboard renders stale data',        'in_progress', 'medium', 'bob@example.com'),
    ('Cannot export query results to CSV',  'resolved',    'low',    'carol@example.com');

INSERT INTO ticket_messages (ticket_id, message_text, author) VALUES
    (1, 'Starting a job cluster returns a timeout after 20 minutes.', 'alice@example.com'),
    (1, 'Thanks for reporting — can you share the cluster event log?', 'support@example.com'),
    (1, 'Attached the event log; it stalls waiting for instances.',    'alice@example.com'),

    (2, 'The revenue dashboard shows yesterday''s numbers.',          'bob@example.com'),
    (2, 'We are investigating the refresh schedule now.',             'support@example.com'),

    (3, 'CSV export button does nothing when clicked.',               'carol@example.com'),
    (3, 'Fixed in the latest release — please refresh and retry.',    'support@example.com'),
    (3, 'Confirmed working now, thank you!',                          'carol@example.com');

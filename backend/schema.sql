-- Hellio HR -- schema (14 tables)

-- pgvector must be created before tables that use vector columns
CREATE EXTENSION IF NOT EXISTS vector;

-- Users (auth)
CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    username   VARCHAR(50) UNIQUE NOT NULL,
    password   VARCHAR(255) NOT NULL,
    role       VARCHAR(20) NOT NULL DEFAULT 'hr-viewer',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_user_role CHECK (role IN ('hr-editor', 'hr-viewer'))
);

-- Core entities
CREATE TABLE IF NOT EXISTS candidates (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             VARCHAR(200) NOT NULL,
    status           VARCHAR(20) NOT NULL CHECK (status IN ('active', 'inactive')),
    experience_level VARCHAR(20) NOT NULL,
    phone            VARCHAR(50),
    email            VARCHAR(200),
    location         VARCHAR(200),
    linkedin         VARCHAR(500),
    github           VARCHAR(500),
    summary          TEXT NOT NULL DEFAULT '',
    cv_file          VARCHAR(255),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ,
    embedding        vector(1536),
    embedding_text   TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title                 VARCHAR(200) NOT NULL,
    status                VARCHAR(20) NOT NULL CHECK (status IN ('open', 'closed')),
    company               VARCHAR(200) NOT NULL,
    hiring_manager_name   VARCHAR(200) NOT NULL DEFAULT '',
    hiring_manager_title  VARCHAR(200) NOT NULL DEFAULT '',
    hiring_manager_email  VARCHAR(200) NOT NULL DEFAULT '',
    experience_level      VARCHAR(50) NOT NULL,
    location              VARCHAR(200) NOT NULL,
    work_arrangement      VARCHAR(200) NOT NULL,
    compensation          VARCHAR(200) DEFAULT '',
    salary_min            INTEGER,
    salary_max            INTEGER,
    timeline              VARCHAR(200) DEFAULT '',
    summary               TEXT NOT NULL DEFAULT '',
    job_file              VARCHAR(255),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    embedding             vector(1536),
    embedding_text        TEXT
);

-- Skills (stored as text directly, no shared lookup table)
CREATE TABLE IF NOT EXISTS candidate_skills (
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE,
    skill        VARCHAR(100) NOT NULL,
    PRIMARY KEY (candidate_id, skill)
);

CREATE TABLE IF NOT EXISTS position_skills (
    position_id UUID REFERENCES positions(id) ON DELETE CASCADE,
    skill       VARCHAR(100) NOT NULL,
    PRIMARY KEY (position_id, skill)
);

-- Candidate detail tables
CREATE TABLE IF NOT EXISTS candidate_languages (
    id           SERIAL PRIMARY KEY,
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE NOT NULL,
    language     VARCHAR(100) NOT NULL,
    UNIQUE (candidate_id, language)
);

CREATE TABLE IF NOT EXISTS experience (
    id           SERIAL PRIMARY KEY,
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE NOT NULL,
    title        VARCHAR(200) NOT NULL,
    company      VARCHAR(200) NOT NULL,
    location     VARCHAR(200),
    start_date   VARCHAR(20),
    end_date     VARCHAR(20),
    description  TEXT NOT NULL DEFAULT '',
    sort_order   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS education (
    id           SERIAL PRIMARY KEY,
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE NOT NULL,
    degree       VARCHAR(200) NOT NULL,
    institution  VARCHAR(200) NOT NULL,
    start_date   VARCHAR(20),
    end_date     VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS certifications (
    id           SERIAL PRIMARY KEY,
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE NOT NULL,
    name         VARCHAR(200) NOT NULL,
    year         INTEGER
);

-- Position detail tables (requirements, nice-to-have, and responsibilities unified)
CREATE TABLE IF NOT EXISTS position_requirements (
    id          SERIAL PRIMARY KEY,
    position_id UUID REFERENCES positions(id) ON DELETE CASCADE NOT NULL,
    item        TEXT NOT NULL,
    type        VARCHAR(20) NOT NULL CHECK (type IN ('required', 'nice_to_have', 'responsibility')),
    sort_order  INTEGER NOT NULL DEFAULT 0
);

-- Junction + metadata
CREATE TABLE IF NOT EXISTS candidate_positions (
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE,
    position_id  UUID REFERENCES positions(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (candidate_id, position_id)
);

CREATE TABLE IF NOT EXISTS documents (
    id          SERIAL PRIMARY KEY,
    entity_type VARCHAR(20) NOT NULL,
    entity_id   UUID NOT NULL,
    filename    VARCHAR(255) NOT NULL,
    file_type   VARCHAR(50),
    stored_path VARCHAR(500),
    raw_text    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_candidates_email ON candidates (LOWER(email));
CREATE INDEX IF NOT EXISTS idx_candidate_skills_skill ON candidate_skills(skill);
CREATE INDEX IF NOT EXISTS idx_position_skills_skill ON position_skills(skill);
CREATE INDEX IF NOT EXISTS idx_experience_candidate ON experience(candidate_id);
CREATE INDEX IF NOT EXISTS idx_education_candidate ON education(candidate_id);
CREATE INDEX IF NOT EXISTS idx_certifications_candidate ON certifications(candidate_id);
CREATE INDEX IF NOT EXISTS idx_documents_entity ON documents(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_position_requirements_pos ON position_requirements(position_id);
CREATE INDEX IF NOT EXISTS idx_candidate_languages_cand ON candidate_languages(candidate_id);

-- Agent state tracking (Exercise 6)
CREATE TABLE IF NOT EXISTS agent_processed_emails (
    email_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    email_type   TEXT NOT NULL,
    action_taken TEXT NOT NULL,
    draft_id     TEXT
);

CREATE TABLE IF NOT EXISTS agent_notifications (
    id               SERIAL PRIMARY KEY,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    type             TEXT NOT NULL,
    summary          TEXT NOT NULL,
    action_url       TEXT,
    status           TEXT NOT NULL DEFAULT 'pending',
    related_email_id TEXT REFERENCES agent_processed_emails(email_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_notifications_status ON agent_notifications(status);
CREATE INDEX IF NOT EXISTS idx_agent_notifications_email ON agent_notifications(related_email_id);

-- 1. INITIALIZE NAMESPACES
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. BRONZE LAYER
CREATE TABLE IF NOT EXISTS bronze.raw_items (
    url_hash        VARCHAR PRIMARY KEY,
    source          VARCHAR,
    collection_date DATE,
    url             VARCHAR,
    title           VARCHAR,
    body            VARCHAR,
    created_at      TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- 3. SILVER LAYER
CREATE TABLE IF NOT EXISTS silver.embeddings (
    url_hash        VARCHAR PRIMARY KEY,
    embedding       FLOAT[384],
    embedding_date  DATE
);

CREATE TABLE IF NOT EXISTS silver.clusters (
    cluster_date DATE,
    cluster_id   INTEGER,
    cluster_size INTEGER,
    confidence   DOUBLE,
    PRIMARY KEY (cluster_date, cluster_id)
);

CREATE TABLE IF NOT EXISTS silver.cluster_members (
    cluster_date DATE,
    cluster_id   INTEGER,
    url_hash     VARCHAR,
    PRIMARY KEY (cluster_date, cluster_id, url_hash)
);


-- 4. GOLD LAYER
CREATE TABLE IF NOT EXISTS gold.issues (
    cluster_date     DATE,
    cluster_id       INTEGER,
    canonical_title  VARCHAR,
    category         VARCHAR,
    description      VARCHAR,
    would_pay_signal BOOLEAN,
    impact_level     VARCHAR,
    cluster_size     INTEGER,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cluster_date, cluster_id)
);

CREATE TABLE IF NOT EXISTS gold.issue_evidence (
    cluster_date DATE,
    cluster_id   INTEGER,
    url_hash     VARCHAR,
    source       VARCHAR,
    url          VARCHAR,
    title        VARCHAR,
    body         VARCHAR,
    posted_at    TIMESTAMP,
    PRIMARY KEY (cluster_date, cluster_id, url_hash)
);

CREATE TABLE IF NOT EXISTS gold.live_issues (
    cluster_date DATE,
    cluster_id   INTEGER,
    issue_id     VARCHAR,
    synced_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cluster_date, cluster_id)
);



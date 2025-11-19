-- Simplified, unnested schema for AI papers dashboard
-- This schema flattens the nested JSON structure into a single table

CREATE TABLE papers (
    -- Identifiers
    id SERIAL PRIMARY KEY,
    openalex_id VARCHAR(255) UNIQUE NOT NULL,
    doi VARCHAR(500),
    
    -- Basic paper information
    title TEXT NOT NULL,
    paper_type VARCHAR(50),  -- article, etc.
    publication_date DATE,
    publication_year INTEGER,
    
    -- Primary topic/classification (flattened from nested structure)
    primary_topic_name VARCHAR(255),
    primary_topic_score DECIMAL(10, 8),
    subfield_name VARCHAR(255),  -- e.g., "Artificial Intelligence"
    field_name VARCHAR(255),     -- e.g., "Computer Science"
    domain_name VARCHAR(255),
    
    -- Open Access information (flattened)
    is_open_access BOOLEAN,
    oa_status VARCHAR(50),  -- gold, bronze, green, closed, hybrid
    
    -- Citation metrics (quantitative)
    cited_by_count INTEGER DEFAULT 0,
    citation_percentile DECIMAL(10, 8),  -- 0.0 to 1.0
    is_top_1_percent BOOLEAN,
    is_top_10_percent BOOLEAN,
    citation_percentile_min INTEGER,  -- min percentile year
    citation_percentile_max INTEGER,  -- max percentile year
    fwci DECIMAL(10, 8),  -- Field-Weighted Citation Impact
    
    -- Collaboration metrics (quantitative)
    countries_count INTEGER DEFAULT 0,
    institutions_count INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common dashboard queries
CREATE INDEX idx_papers_publication_date ON papers(publication_date);
CREATE INDEX idx_papers_publication_year ON papers(publication_year);
CREATE INDEX idx_papers_cited_by_count ON papers(cited_by_count);
CREATE INDEX idx_papers_oa_status ON papers(oa_status);
CREATE INDEX idx_papers_subfield ON papers(subfield_name);
CREATE INDEX idx_papers_field ON papers(field_name);
CREATE INDEX idx_papers_primary_topic ON papers(primary_topic_name);
CREATE INDEX idx_papers_citation_percentile ON papers(citation_percentile);
CREATE INDEX idx_papers_fwci ON papers(fwci);

-- Index for text search on titles
CREATE INDEX idx_papers_title_trgm ON papers USING gin(title gin_trgm_ops);

-- Enable pg_trgm extension for text search (if not already enabled)
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;

COMMENT ON TABLE papers IS 'Flattened schema for AI research papers tracking dashboard';
COMMENT ON COLUMN papers.openalex_id IS 'OpenAlex unique identifier (e.g., https://openalex.org/W7105757696)';
COMMENT ON COLUMN papers.citation_percentile IS 'Normalized citation percentile (0.0 to 1.0)';
COMMENT ON COLUMN papers.fwci IS 'Field-Weighted Citation Impact';
COMMENT ON COLUMN papers.oa_status IS 'Open access status: gold, bronze, green, hybrid, or closed';


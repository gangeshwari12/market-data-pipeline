#!/usr/bin/env python3
"""
Script to load AI papers from JSON file into the database.

This script:
1. Takes a JSON filepath as command-line argument
2. Loads the data from JSON
3. Connects to the database using db_connection
4. Creates papers table if necessary
5. Processes the data for the papers table
6. Inserts the data into the table with deduplication (based on openalex_id)
"""

import json
import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime

from db_connection import DatabaseConnection


def table_exists() -> bool:
    """Check if the papers table exists in the database."""
    with DatabaseConnection.get_cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'papers'
            );
        """)
        result = cur.fetchone()
        return result[0] if result else False


def create_papers_table_if_needed():
    """Create the papers table if it doesn't exist."""
    if table_exists():
        print("ℹ Papers table already exists. Skipping creation.")
        return
    
    print("Creating papers table...")
    
    # Create pg_trgm extension (needed for text search index)
    with DatabaseConnection.get_cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        print("✓ pg_trgm extension checked/created")
    
    # Create the table
    create_table_sql = """
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
    """
    
    with DatabaseConnection.get_cursor() as cur:
        cur.execute(create_table_sql)
        print("✓ Papers table created")
    
    # Create indexes
    indexes = [
        "CREATE INDEX idx_papers_publication_date ON papers(publication_date);",
        "CREATE INDEX idx_papers_publication_year ON papers(publication_year);",
        "CREATE INDEX idx_papers_cited_by_count ON papers(cited_by_count);",
        "CREATE INDEX idx_papers_oa_status ON papers(oa_status);",
        "CREATE INDEX idx_papers_subfield ON papers(subfield_name);",
        "CREATE INDEX idx_papers_field ON papers(field_name);",
        "CREATE INDEX idx_papers_primary_topic ON papers(primary_topic_name);",
        "CREATE INDEX idx_papers_citation_percentile ON papers(citation_percentile);",
        "CREATE INDEX idx_papers_fwci ON papers(fwci);",
        "CREATE INDEX idx_papers_title_trgm ON papers USING gin(title gin_trgm_ops);",
    ]
    
    with DatabaseConnection.get_cursor() as cur:
        for idx_sql in indexes:
            try:
                cur.execute(idx_sql)
            except Exception as e:
                # Index might already exist, continue
                print(f"  Note: {e}")
        print("✓ Indexes created")
    
    # Add comments
    comments = [
        ("TABLE", "papers", "Flattened schema for AI research papers tracking dashboard"),
        ("COLUMN", "papers.openalex_id", "OpenAlex unique identifier (e.g., https://openalex.org/W7105757696)"),
        ("COLUMN", "papers.citation_percentile", "Normalized citation percentile (0.0 to 1.0)"),
        ("COLUMN", "papers.fwci", "Field-Weighted Citation Impact"),
        ("COLUMN", "papers.oa_status", "Open access status: gold, bronze, green, hybrid, or closed"),
    ]
    
    with DatabaseConnection.get_cursor() as cur:
        for obj_type, obj_name, comment_text in comments:
            cur.execute(f"COMMENT ON {obj_type} {obj_name} IS %s;", (comment_text,))
        print("✓ Comments added")


def extract_doi(paper: Dict[str, Any]) -> Optional[str]:
    """Extract DOI from paper data."""
    doi = paper.get('doi')
    if doi:
        # Remove 'https://doi.org/' prefix if present
        if doi.startswith('https://doi.org/'):
            return doi.replace('https://doi.org/', '')
        return doi
    return None


def process_paper(paper: Dict[str, Any]) -> tuple:
    """
    Process a single paper from JSON and extract fields for database insertion.
    
    Returns:
        Tuple of values in the order of database columns
    """
    # Identifiers
    openalex_id = paper.get('id', '').replace('https://openalex.org/', '')
    doi = extract_doi(paper)
    
    # Basic information
    title = paper.get('title', '') or paper.get('display_name', '')
    paper_type = paper.get('type')
    publication_date = paper.get('publication_date')
    publication_year = paper.get('publication_year')
    
    # Primary topic (flattened)
    primary_topic = paper.get('primary_topic', {})
    primary_topic_name = primary_topic.get('display_name') if primary_topic else None
    primary_topic_score = primary_topic.get('score') if primary_topic else None
    
    subfield = primary_topic.get('subfield', {}) if primary_topic else {}
    subfield_name = subfield.get('display_name') if subfield else None
    
    field = primary_topic.get('field', {}) if primary_topic else {}
    field_name = field.get('display_name') if field else None
    
    domain = primary_topic.get('domain', {}) if primary_topic else {}
    domain_name = domain.get('display_name') if domain else None
    
    # Open Access information
    open_access = paper.get('open_access', {})
    is_open_access = open_access.get('is_oa', False) if open_access else False
    oa_status = open_access.get('oa_status') if open_access else None
    
    # Citation metrics
    cited_by_count = paper.get('cited_by_count', 0) or 0
    
    citation_normalized = paper.get('citation_normalized_percentile', {})
    citation_percentile = citation_normalized.get('value') if citation_normalized else None
    is_top_1_percent = citation_normalized.get('is_in_top_1_percent', False) if citation_normalized else False
    is_top_10_percent = citation_normalized.get('is_in_top_10_percent', False) if citation_normalized else False
    
    cited_by_percentile_year = paper.get('cited_by_percentile_year', {})
    citation_percentile_min = cited_by_percentile_year.get('min') if cited_by_percentile_year else None
    citation_percentile_max = cited_by_percentile_year.get('max') if cited_by_percentile_year else None
    
    # FWCI (Field-Weighted Citation Impact) - not directly in JSON, set to None
    fwci = None
    
    # Collaboration metrics
    countries_count = paper.get('countries_distinct_count', 0) or 0
    institutions_count = paper.get('institutions_distinct_count', 0) or 0
    
    return (
        openalex_id,
        doi,
        title,
        paper_type,
        publication_date,
        publication_year,
        primary_topic_name,
        primary_topic_score,
        subfield_name,
        field_name,
        domain_name,
        is_open_access,
        oa_status,
        cited_by_count,
        citation_percentile,
        is_top_1_percent,
        is_top_10_percent,
        citation_percentile_min,
        citation_percentile_max,
        fwci,
        countries_count,
        institutions_count
    )


def load_json_file(filepath: str) -> Dict[str, Any]:
    """Load JSON data from file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"JSON file not found: {filepath}")
    
    print(f"Loading JSON file: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle both formats: direct list or dict with 'papers' key
    if isinstance(data, list):
        papers = data
    elif isinstance(data, dict) and 'papers' in data:
        papers = data['papers']
        if 'metadata' in data:
            print(f"  Metadata: {data['metadata']}")
    else:
        raise ValueError("JSON file must contain either a list of papers or a dict with 'papers' key")
    
    print(f"  Loaded {len(papers)} papers from JSON")
    return papers


def insert_papers_with_deduplication(papers: list, batch_size: int = 100):
    """
    Insert papers into database with deduplication.
    Uses ON CONFLICT to skip duplicates based on openalex_id.
    """
    insert_sql = """
    INSERT INTO papers (
        openalex_id, doi, title, paper_type, publication_date, publication_year,
        primary_topic_name, primary_topic_score, subfield_name, field_name, domain_name,
        is_open_access, oa_status,
        cited_by_count, citation_percentile, is_top_1_percent, is_top_10_percent,
        citation_percentile_min, citation_percentile_max, fwci,
        countries_count, institutions_count,
        updated_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (openalex_id) 
    DO UPDATE SET
        doi = EXCLUDED.doi,
        title = EXCLUDED.title,
        paper_type = EXCLUDED.paper_type,
        publication_date = EXCLUDED.publication_date,
        publication_year = EXCLUDED.publication_year,
        primary_topic_name = EXCLUDED.primary_topic_name,
        primary_topic_score = EXCLUDED.primary_topic_score,
        subfield_name = EXCLUDED.subfield_name,
        field_name = EXCLUDED.field_name,
        domain_name = EXCLUDED.domain_name,
        is_open_access = EXCLUDED.is_open_access,
        oa_status = EXCLUDED.oa_status,
        cited_by_count = EXCLUDED.cited_by_count,
        citation_percentile = EXCLUDED.citation_percentile,
        is_top_1_percent = EXCLUDED.is_top_1_percent,
        is_top_10_percent = EXCLUDED.is_top_10_percent,
        citation_percentile_min = EXCLUDED.citation_percentile_min,
        citation_percentile_max = EXCLUDED.citation_percentile_max,
        fwci = EXCLUDED.fwci,
        countries_count = EXCLUDED.countries_count,
        institutions_count = EXCLUDED.institutions_count,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    processed_papers = []
    skipped = 0
    
    print(f"\nProcessing {len(papers)} papers...")
    
    for paper in papers:
        try:
            processed = process_paper(paper)
            # Validate that openalex_id is not empty
            if not processed[0]:
                skipped += 1
                continue
            processed_papers.append(processed)
        except Exception as e:
            print(f"  Warning: Error processing paper {paper.get('id', 'unknown')}: {e}")
            skipped += 1
            continue
    
    if skipped > 0:
        print(f"  Skipped {skipped} papers due to errors or missing openalex_id")
    
    print(f"  Processed {len(processed_papers)} papers for insertion")
    
    # Insert in batches
    total_inserted = 0
    total_updated = 0
    
    with DatabaseConnection.get_connection_context() as conn:
        cur = conn.cursor()
        
        for i in range(0, len(processed_papers), batch_size):
            batch = processed_papers[i:i + batch_size]
            
            try:
                # Use executemany for batch insertion
                cur.executemany(insert_sql, batch)
                conn.commit()
                
                # Count how many were inserted vs updated
                # We can't easily distinguish, but we can check affected rows
                # For simplicity, we'll just report the batch size
                batch_size_actual = len(batch)
                total_inserted += batch_size_actual
                
                print(f"  Inserted/updated batch {i//batch_size + 1} ({batch_size_actual} papers)")
                
            except Exception as e:
                conn.rollback()
                print(f"  Error inserting batch {i//batch_size + 1}: {e}")
                # Try inserting one by one to identify problematic records
                for paper_data in batch:
                    try:
                        cur.execute(insert_sql, paper_data)
                        conn.commit()
                        total_inserted += 1
                    except Exception as e2:
                        conn.rollback()
                        print(f"    Failed to insert paper {paper_data[0]}: {e2}")
        
        cur.close()
    
    print(f"\n✓ Successfully processed {total_inserted} papers")
    return total_inserted


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python load_papers_from_json.py <json_filepath>")
        print("Example: python load_papers_from_json.py temp/ai_papers_20251118_122003.json")
        sys.exit(1)
    
    json_filepath = sys.argv[1]
    
    try:
        # Step 1: Load JSON data
        papers = load_json_file(json_filepath)
        
        if not papers:
            print("⚠ No papers found in JSON file.")
            return
        
        # Step 2: Connect to database and create table if needed
        print("\n" + "=" * 50)
        print("Database Setup")
        print("=" * 50)
        create_papers_table_if_needed()
        
        # Step 3: Process and insert papers with deduplication
        print("\n" + "=" * 50)
        print("Data Processing and Insertion")
        print("=" * 50)
        insert_papers_with_deduplication(papers)
        
        print("\n" + "=" * 50)
        print("✓ Successfully completed!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        DatabaseConnection.close_all_connections()


if __name__ == '__main__':
    main()


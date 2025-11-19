#!/usr/bin/env python3
"""
Pipeline class for AI papers data processing.

This pipeline consolidates the following steps:
1. Queries API to get recent papers
2. Creates the DB table if needed
3. Uploads the papers to the database
4. Runs data quality tests

Uses existing logic from:
- fetch_ai_papers.py (API querying)
- create_papers_table.py (table creation)
- load_papers_from_json.py (data processing and insertion)
- test_papers_data.py (data quality validation)
"""

import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pyalex import Works, Topics

from db_connection import DatabaseConnection


class PapersDataPipeline:
    """Pipeline for processing AI papers from OpenAlex API to database."""
    
    def __init__(self, days: int = 3, batch_size: int = 100):
        """
        Initialize the pipeline.
        
        Args:
            days: Number of days to fetch papers from (default: 3)
            batch_size: Batch size for database insertion (default: 100)
        """
        self.days = days
        self.batch_size = batch_size
        self.papers = []
    
    # =============================================================================
    # Step 1: Query API to get recent papers
    # =============================================================================
    
    def search_ai_field_subfield(self):
        """Search for 'artificial intelligence' as Field or Subfield in OpenAlex Topics."""
        print("Searching for 'artificial intelligence' as Field or Subfield...")
        
        field_id = None
        subfield_id = None
        
        # Search for topics related to artificial intelligence
        topics = Topics().search("artificial intelligence").get()
        
        if topics and len(topics) > 0:
            # Look through topics to find field or subfield named "Artificial Intelligence"
            for topic in topics:
                # Check if this topic's field is "Artificial Intelligence"
                field = topic.get('field')
                if field:
                    field_name = field.get('display_name', '').lower()
                    if 'artificial intelligence' in field_name or field_name == 'artificial intelligence':
                        field_id_full = field.get('id')
                        # Extract numeric ID from URL (e.g., https://openalex.org/fields/123 -> 123)
                        if field_id_full and '/' in field_id_full:
                            field_id = field_id_full.split('/')[-1]
                        else:
                            field_id = field_id_full
                        print(f"Found Field: {field.get('display_name')} (ID: {field_id})")
                        break
                
                # Check if this topic's subfield is "Artificial Intelligence"
                subfield = topic.get('subfield')
                if subfield:
                    subfield_name = subfield.get('display_name', '').lower()
                    if 'artificial intelligence' in subfield_name or subfield_name == 'artificial intelligence':
                        subfield_id_full = subfield.get('id')
                        # Extract numeric ID from URL (e.g., https://openalex.org/subfields/1702 -> 1702)
                        if subfield_id_full and '/' in subfield_id_full:
                            subfield_id = subfield_id_full.split('/')[-1]
                        else:
                            subfield_id = subfield_id_full
                        print(f"Found Subfield: {subfield.get('display_name')} (ID: {subfield_id})")
                        break
        
        # If still not found, search more broadly by checking multiple topics
        if not field_id and not subfield_id:
            print("Searching more broadly through topics...")
            # Get more topics and check their field/subfield
            all_topics = Topics().search("artificial intelligence").get(per_page=50)
            
            for topic in all_topics:
                field = topic.get('field')
                if field:
                    field_name = field.get('display_name', '').lower()
                    if 'artificial intelligence' in field_name:
                        if not field_id:  # Only set if not already found
                            field_id_full = field.get('id')
                            # Extract numeric ID from URL
                            if field_id_full and '/' in field_id_full:
                                field_id = field_id_full.split('/')[-1]
                            else:
                                field_id = field_id_full
                            print(f"Found Field: {field.get('display_name')} (ID: {field_id})")
                
                subfield = topic.get('subfield')
                if subfield:
                    subfield_name = subfield.get('display_name', '').lower()
                    if 'artificial intelligence' in subfield_name:
                        if not subfield_id:  # Only set if not already found
                            subfield_id_full = subfield.get('id')
                            # Extract numeric ID from URL
                            if subfield_id_full and '/' in subfield_id_full:
                                subfield_id = subfield_id_full.split('/')[-1]
                            else:
                                subfield_id = subfield_id_full
                            print(f"Found Subfield: {subfield.get('display_name')} (ID: {subfield_id})")
        
        if not field_id and not subfield_id:
            raise ValueError("Could not find 'Artificial Intelligence' as a Field or Subfield. "
                            "You may need to manually specify the field/subfield ID.")
        
        return field_id, subfield_id
    
    def fetch_recent_works(self, field_id: Optional[str], subfield_id: Optional[str]) -> List[Dict[str, Any]]:
        """Fetch works filtered by PRIMARY field or subfield ID from the last N days."""
        print(f"Fetching works from the last {self.days} days...")
        print("Using PRIMARY topic filter to get only papers where AI is the main focus...")
        
        # Calculate the date N days ago
        date_from = (datetime.now() - timedelta(days=self.days)).strftime('%Y-%m-%d')
        
        # Filter works by primary_topic.field.id OR primary_topic.subfield.id and publication date
        all_works = []
        seen_ids = set()  # Track work IDs to avoid duplicates
        
        per_page = 200  # OpenAlex allows up to 200 per page
        
        # Fetch works for field if available (using PRIMARY topic filter)
        if field_id:
            print(f"Fetching works with PRIMARY field ID: {field_id}")
            page = 1
            while True:
                print(f"  Fetching field page {page}...")
                works_query = Works().filter(
                    **{"primary_topic.field.id": field_id, "from_publication_date": date_from}
                )
                works = works_query.get(per_page=per_page, page=page)
                
                if not works or len(works) == 0:
                    break
                
                # Add unique works
                for work in works:
                    work_id = work.get('id')
                    if work_id and work_id not in seen_ids:
                        seen_ids.add(work_id)
                        all_works.append(work)
                
                print(f"    Found {len(works)} works on page {page} (unique total so far: {len(all_works)})")
                
                if len(works) < per_page:
                    break
                page += 1
        
        # Fetch works for subfield if available (using PRIMARY topic filter)
        if subfield_id:
            print(f"Fetching works with PRIMARY subfield ID: {subfield_id}")
            page = 1
            while True:
                print(f"  Fetching subfield page {page}...")
                works_query = Works().filter(
                    **{"primary_topic.subfield.id": subfield_id, "from_publication_date": date_from}
                )
                works = works_query.get(per_page=per_page, page=page)
                
                if not works or len(works) == 0:
                    break
                
                # Add unique works
                for work in works:
                    work_id = work.get('id')
                    if work_id and work_id not in seen_ids:
                        seen_ids.add(work_id)
                        all_works.append(work)
                
                print(f"    Found {len(works)} works on page {page} (unique total so far: {len(all_works)})")
                
                if len(works) < per_page:
                    break
                page += 1
        
        print(f"Total unique works fetched: {len(all_works)}")
        return all_works
    
    def query_api(self) -> List[Dict[str, Any]]:
        """Query OpenAlex API to get recent papers."""
        print("\n" + "=" * 70)
        print("STEP 1: Querying OpenAlex API for Recent Papers")
        print("=" * 70)
        
        # Search for AI field/subfield
        field_id, subfield_id = self.search_ai_field_subfield()
        
        # Fetch recent works
        papers = self.fetch_recent_works(field_id, subfield_id)
        
        self.papers = papers
        return papers
    
    # =============================================================================
    # Step 2: Create the DB table if needed
    # =============================================================================
    
    def table_exists(self) -> bool:
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
    
    def create_pg_trgm_extension(self):
        """Create the pg_trgm extension if it doesn't exist."""
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            print("✓ pg_trgm extension checked/created")
    
    def create_papers_table(self):
        """Create the papers table with all columns."""
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
    
    def create_indexes(self):
        """Create all indexes for the papers table."""
        indexes = [
            ("idx_papers_publication_date", "CREATE INDEX idx_papers_publication_date ON papers(publication_date);"),
            ("idx_papers_publication_year", "CREATE INDEX idx_papers_publication_year ON papers(publication_year);"),
            ("idx_papers_cited_by_count", "CREATE INDEX idx_papers_cited_by_count ON papers(cited_by_count);"),
            ("idx_papers_oa_status", "CREATE INDEX idx_papers_oa_status ON papers(oa_status);"),
            ("idx_papers_subfield", "CREATE INDEX idx_papers_subfield ON papers(subfield_name);"),
            ("idx_papers_field", "CREATE INDEX idx_papers_field ON papers(field_name);"),
            ("idx_papers_primary_topic", "CREATE INDEX idx_papers_primary_topic ON papers(primary_topic_name);"),
            ("idx_papers_citation_percentile", "CREATE INDEX idx_papers_citation_percentile ON papers(citation_percentile);"),
            ("idx_papers_fwci", "CREATE INDEX idx_papers_fwci ON papers(fwci);"),
            ("idx_papers_title_trgm", "CREATE INDEX idx_papers_title_trgm ON papers USING gin(title gin_trgm_ops);"),
        ]
        
        with DatabaseConnection.get_cursor() as cur:
            for idx_name, idx_sql in indexes:
                # Check if index already exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_indexes 
                        WHERE schemaname = 'public' 
                        AND indexname = %s
                    );
                """, (idx_name,))
                exists = cur.fetchone()[0]
                
                if exists:
                    print(f"  - Index already exists: {idx_name}")
                else:
                    try:
                        cur.execute(idx_sql)
                        print(f"  ✓ Created index: {idx_name}")
                    except Exception as e:
                        print(f"  ✗ Failed to create index {idx_name}: {e}")
                        raise
    
    def add_comments(self):
        """Add comments to the table and key columns."""
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
                print(f"  ✓ Added comment to {obj_name}")
    
    def create_table_if_needed(self):
        """Create the papers table if it doesn't exist."""
        print("\n" + "=" * 70)
        print("STEP 2: Creating Database Table (if needed)")
        print("=" * 70)
        
        # Check if table already exists
        if self.table_exists():
            print("ℹ Papers table already exists. Skipping creation.")
            return
        
        # Create pg_trgm extension (needed for text search index)
        print("\n1. Checking pg_trgm extension...")
        self.create_pg_trgm_extension()
        
        # Create the table
        print("\n2. Creating papers table...")
        self.create_papers_table()
        
        # Create indexes
        print("\n3. Creating indexes...")
        self.create_indexes()
        
        # Add comments
        print("\n4. Adding table and column comments...")
        self.add_comments()
        
        print("\n✓ Database table setup complete!")
    
    # =============================================================================
    # Step 3: Upload papers to the database
    # =============================================================================
    
    def extract_doi(self, paper: Dict[str, Any]) -> Optional[str]:
        """Extract DOI from paper data."""
        doi = paper.get('doi')
        if doi:
            # Remove 'https://doi.org/' prefix if present
            if doi.startswith('https://doi.org/'):
                return doi.replace('https://doi.org/', '')
            return doi
        return None
    
    def process_paper(self, paper: Dict[str, Any]) -> tuple:
        """
        Process a single paper from JSON and extract fields for database insertion.
        
        Returns:
            Tuple of values in the order of database columns
        """
        # Identifiers
        openalex_id = paper.get('id', '').replace('https://openalex.org/', '')
        doi = self.extract_doi(paper)
        
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
    
    def insert_papers_with_deduplication(self, papers: List[Dict[str, Any]]) -> int:
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
                processed = self.process_paper(paper)
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
        
        with DatabaseConnection.get_connection_context() as conn:
            cur = conn.cursor()
            
            for i in range(0, len(processed_papers), self.batch_size):
                batch = processed_papers[i:i + self.batch_size]
                
                try:
                    # Use executemany for batch insertion
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    
                    batch_size_actual = len(batch)
                    total_inserted += batch_size_actual
                    
                    print(f"  Inserted/updated batch {i//self.batch_size + 1} ({batch_size_actual} papers)")
                    
                except Exception as e:
                    conn.rollback()
                    print(f"  Error inserting batch {i//self.batch_size + 1}: {e}")
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
    
    def upload_papers(self, papers: Optional[List[Dict[str, Any]]] = None) -> int:
        """Upload papers to the database."""
        print("\n" + "=" * 70)
        print("STEP 3: Uploading Papers to Database")
        print("=" * 70)
        
        if papers is None:
            papers = self.papers
        
        if not papers:
            print("⚠ No papers to upload.")
            return 0
        
        return self.insert_papers_with_deduplication(papers)
    
    # =============================================================================
    # Step 4: Run data quality tests
    # =============================================================================
    
    def run_test(self, test_name: str, query: str, description: str, 
                 expect_zero: bool = True) -> Dict[str, Any]:
        """
        Run a single test query.
        
        Args:
            test_name: Name of the test
            query: SQL query to execute
            description: Human-readable description
            expect_zero: If True, test passes when result is 0. If False, test passes when result > 0.
        
        Returns:
            Dictionary with test results
        """
        try:
            with DatabaseConnection.get_cursor(dict_cursor=True) as cur:
                cur.execute(query)
                result = cur.fetchone()
                
                # Get the first value from the result (count or similar)
                count = list(result.values())[0] if result else 0
                
                # Determine if test passed
                passed = (count == 0) if expect_zero else (count > 0)
                
                if passed:
                    status = "✓ PASS"
                else:
                    status = "✗ FAIL"
                
                test_result = {
                    'name': test_name,
                    'status': status,
                    'description': description,
                    'count': count,
                    'passed': passed
                }
                
                return test_result
                
        except Exception as e:
            test_result = {
                'name': test_name,
                'status': '✗ ERROR',
                'description': description,
                'error': str(e),
                'passed': False
            }
            return test_result
    
    def run_data_quality_tests(self) -> int:
        """Run all data quality tests."""
        print("\n" + "=" * 70)
        print("STEP 4: Running Data Quality Tests")
        print("=" * 70)
        
        # Check if table exists
        if not self.table_exists():
            print("\n✗ ERROR: 'papers' table does not exist in the database.")
            print("Please create the table first.")
            return 1
        
        # Get total record count
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM papers;")
            total_count = cur.fetchone()[0]
            print(f"\nTotal records in papers table: {total_count:,}")
        
        results = []
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        
        # Test 1: Missing Required Fields
        print("\n" + "=" * 70)
        print("TEST 1: Missing Required Fields")
        print("=" * 70)
        
        test = self.run_test(
            test_name="Missing openalex_id",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE openalex_id IS NULL OR openalex_id = '';
            """,
            description="Papers with NULL or empty openalex_id (required field)"
        )
        results.append(test)
        total_tests += 1
        if test['passed']:
            passed_tests += 1
        else:
            failed_tests += 1
        print(f"\n{test['status']} - {test['name']}")
        print(f"  {test['description']}")
        if 'error' in test:
            print(f"  Error: {test['error']}")
        else:
            print(f"  Found: {test['count']} record(s)")
        
        test = self.run_test(
            test_name="Missing title",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE title IS NULL OR title = '';
            """,
            description="Papers with NULL or empty title (required field)"
        )
        results.append(test)
        total_tests += 1
        if test['passed']:
            passed_tests += 1
        else:
            failed_tests += 1
        print(f"\n{test['status']} - {test['name']}")
        print(f"  {test['description']}")
        if 'error' in test:
            print(f"  Error: {test['error']}")
        else:
            print(f"  Found: {test['count']} record(s)")
        
        # Test 2: Citation Count Validation
        print("\n" + "=" * 70)
        print("TEST 2: Citation Count Validation")
        print("=" * 70)
        
        for test_name, query, desc in [
            ("Negative cited_by_count",
             "SELECT COUNT(*) as count FROM papers WHERE cited_by_count < 0;",
             "Papers with negative cited_by_count (should be >= 0)"),
            ("Negative countries_count",
             "SELECT COUNT(*) as count FROM papers WHERE countries_count < 0;",
             "Papers with negative countries_count (should be >= 0)"),
            ("Negative institutions_count",
             "SELECT COUNT(*) as count FROM papers WHERE institutions_count < 0;",
             "Papers with negative institutions_count (should be >= 0)")
        ]:
            test = self.run_test(test_name, query, desc)
            results.append(test)
            total_tests += 1
            if test['passed']:
                passed_tests += 1
            else:
                failed_tests += 1
                print(f"\n{test['status']} - {test['name']}")
                print(f"  {test['description']}")
                if 'error' in test:
                    print(f"  Error: {test['error']}")
                else:
                    print(f"  Found: {test['count']} record(s)")
        
        # Test 3: Score Range Validation
        print("\n" + "=" * 70)
        print("TEST 3: Score Range Validation")
        print("=" * 70)
        
        for test_name, query, desc in [
            ("Invalid citation_percentile range",
             "SELECT COUNT(*) as count FROM papers WHERE citation_percentile IS NOT NULL AND (citation_percentile < 0.0 OR citation_percentile > 1.0);",
             "Papers with citation_percentile outside valid range [0.0, 1.0]"),
            ("Invalid primary_topic_score range",
             "SELECT COUNT(*) as count FROM papers WHERE primary_topic_score IS NOT NULL AND (primary_topic_score < 0.0 OR primary_topic_score > 1.0);",
             "Papers with primary_topic_score outside valid range [0.0, 1.0]"),
            ("Negative fwci",
             "SELECT COUNT(*) as count FROM papers WHERE fwci IS NOT NULL AND fwci < 0;",
             "Papers with negative fwci (Field-Weighted Citation Impact)")
        ]:
            test = self.run_test(test_name, query, desc)
            results.append(test)
            total_tests += 1
            if test['passed']:
                passed_tests += 1
            else:
                failed_tests += 1
                print(f"\n{test['status']} - {test['name']}")
                print(f"  {test['description']}")
                if 'error' in test:
                    print(f"  Error: {test['error']}")
                else:
                    print(f"  Found: {test['count']} record(s)")
        
        # Test 4: Duplicate Detection
        print("\n" + "=" * 70)
        print("TEST 4: Duplicate Detection")
        print("=" * 70)
        
        # Duplicate openalex_id test (must pass)
        test = self.run_test(
            test_name="Duplicate openalex_id",
            query="SELECT COUNT(*) as count FROM (SELECT openalex_id, COUNT(*) as cnt FROM papers WHERE openalex_id IS NOT NULL GROUP BY openalex_id HAVING COUNT(*) > 1) as duplicates;",
            description="Number of openalex_id values that appear more than once (should be 0)"
        )
        results.append(test)
        total_tests += 1
        if test['passed']:
            passed_tests += 1
        else:
            failed_tests += 1
            print(f"\n{test['status']} - {test['name']}")
            print(f"  {test['description']}")
            if 'error' in test:
                print(f"  Error: {test['error']}")
            else:
                print(f"  Found: {test['count']} record(s)")
        
        # Duplicate DOI test (informational - always passes)
        test = self.run_test(
            test_name="Duplicate DOI",
            query="SELECT COUNT(*) as count FROM (SELECT doi, COUNT(*) as cnt FROM papers WHERE doi IS NOT NULL AND doi != '' GROUP BY doi HAVING COUNT(*) > 1) as duplicates;",
            description="Number of DOI values that appear more than once (informational)"
        )
        results.append(test)
        total_tests += 1
        # DOI test is informational, so it always counts as passed for the summary
        passed_tests += 1
        print(f"\n{test['status']} - {test['name']}")
        print(f"  {test['description']}")
        if 'error' in test:
            print(f"  Error: {test['error']}")
        else:
            print(f"  Found: {test['count']} duplicate DOI(s) (informational)")
        
        # Print summary
        print("\n" + "=" * 70)
        print("TEST RESULTS SUMMARY")
        print("=" * 70)
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print("-" * 70)
        
        if failed_tests == 0:
            print("\n✓ All tests passed!")
            return 0
        else:
            print(f"\n✗ {failed_tests} test(s) failed. Please review the results above.")
            return 1
    
    # =============================================================================
    # Main pipeline execution
    # =============================================================================
    
    def run(self, skip_tests: bool = False) -> int:
        """
        Run the complete pipeline.
        
        Args:
            skip_tests: If True, skip data quality tests (default: False)
        
        Returns:
            Exit code (0 for success, 1 for failure)
        """
        try:
            # Step 1: Query API to get recent papers
            papers = self.query_api()
            
            if not papers:
                print("\n⚠ No papers found from API. Exiting.")
                return 0
            
            # Step 2: Create the DB table if needed
            self.create_table_if_needed()
            
            # Step 3: Upload papers to the database
            self.upload_papers(papers)
            
            # Step 4: Run data quality tests
            if not skip_tests:
                exit_code = self.run_data_quality_tests()
            else:
                print("\n" + "=" * 70)
                print("STEP 4: Skipping Data Quality Tests")
                print("=" * 70)
                exit_code = 0
            
            print("\n" + "=" * 70)
            print("PIPELINE COMPLETE")
            print("=" * 70)
            
            return exit_code
            
        except Exception as e:
            print(f"\n✗ Pipeline error: {e}")
            import traceback
            traceback.print_exc()
            return 1
        finally:
            DatabaseConnection.close_all_connections()


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='AI Papers Data Pipeline - Query API, create table, upload papers, and run tests'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=3,
        help='Number of days to fetch papers from (default: 3)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for database insertion (default: 100)'
    )
    parser.add_argument(
        '--skip-tests',
        action='store_true',
        help='Skip data quality tests'
    )
    
    args = parser.parse_args()
    
    pipeline = PapersDataPipeline(days=args.days, batch_size=args.batch_size)
    exit_code = pipeline.run(skip_tests=args.skip_tests)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()


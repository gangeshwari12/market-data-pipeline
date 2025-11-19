#!/usr/bin/env python3
"""
Data validation tests for the papers table.

This script runs essential data quality tests:
1. Missing required fields
2. Citation count validation
3. Score range validation
4. Duplicate detection
"""

import sys
from typing import Dict, List, Any
from db_connection import DatabaseConnection


class PapersDataValidator:
    """Validates data quality in the papers table."""
    
    def __init__(self):
        self.results = []
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
    
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
        self.total_tests += 1
        
        try:
            with DatabaseConnection.get_cursor(dict_cursor=True) as cur:
                cur.execute(query)
                result = cur.fetchone()
                
                # Get the first value from the result (count or similar)
                count = list(result.values())[0] if result else 0
                
                # Determine if test passed
                passed = (count == 0) if expect_zero else (count > 0)
                
                if passed:
                    self.passed_tests += 1
                    status = "✓ PASS"
                else:
                    self.failed_tests += 1
                    status = "✗ FAIL"
                
                test_result = {
                    'name': test_name,
                    'status': status,
                    'description': description,
                    'count': count,
                    'passed': passed
                }
                
                self.results.append(test_result)
                return test_result
                
        except Exception as e:
            self.failed_tests += 1
            test_result = {
                'name': test_name,
                'status': '✗ ERROR',
                'description': description,
                'error': str(e),
                'passed': False
            }
            self.results.append(test_result)
            return test_result
    
    def test_missing_required_fields(self):
        """Test 1: Missing Required Fields"""
        print("\n" + "=" * 70)
        print("TEST 1: Missing Required Fields")
        print("=" * 70)
        
        # Test for NULL openalex_id
        self.run_test(
            test_name="Missing openalex_id",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE openalex_id IS NULL OR openalex_id = '';
            """,
            description="Papers with NULL or empty openalex_id (required field)"
        )
        
        # Test for NULL title
        self.run_test(
            test_name="Missing title",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE title IS NULL OR title = '';
            """,
            description="Papers with NULL or empty title (required field)"
        )
    
    def test_citation_count_validation(self):
        """Test 2: Citation Count Validation"""
        print("\n" + "=" * 70)
        print("TEST 2: Citation Count Validation")
        print("=" * 70)
        
        # Test for negative cited_by_count
        self.run_test(
            test_name="Negative cited_by_count",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE cited_by_count < 0;
            """,
            description="Papers with negative cited_by_count (should be >= 0)"
        )
        
        # Test for negative countries_count
        self.run_test(
            test_name="Negative countries_count",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE countries_count < 0;
            """,
            description="Papers with negative countries_count (should be >= 0)"
        )
        
        # Test for negative institutions_count
        self.run_test(
            test_name="Negative institutions_count",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE institutions_count < 0;
            """,
            description="Papers with negative institutions_count (should be >= 0)"
        )
    
    def test_score_range_validation(self):
        """Test 3: Score Range Validation"""
        print("\n" + "=" * 70)
        print("TEST 3: Score Range Validation")
        print("=" * 70)
        
        # Test citation_percentile range (should be 0.0 to 1.0)
        self.run_test(
            test_name="Invalid citation_percentile range",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE citation_percentile IS NOT NULL
                  AND (citation_percentile < 0.0 OR citation_percentile > 1.0);
            """,
            description="Papers with citation_percentile outside valid range [0.0, 1.0]"
        )
        
        # Test primary_topic_score range (should be 0.0 to 1.0)
        self.run_test(
            test_name="Invalid primary_topic_score range",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE primary_topic_score IS NOT NULL
                  AND (primary_topic_score < 0.0 OR primary_topic_score > 1.0);
            """,
            description="Papers with primary_topic_score outside valid range [0.0, 1.0]"
        )
        
        # Test fwci range (should be >= 0, no upper bound specified)
        self.run_test(
            test_name="Negative fwci",
            query="""
                SELECT COUNT(*) as count
                FROM papers
                WHERE fwci IS NOT NULL AND fwci < 0;
            """,
            description="Papers with negative fwci (Field-Weighted Citation Impact)"
        )
    
    def test_duplicate_detection(self):
        """Test 4: Duplicate Detection"""
        print("\n" + "=" * 70)
        print("TEST 4: Duplicate Detection")
        print("=" * 70)
        
        # Test for duplicate openalex_id (should be unique)
        self.run_test(
            test_name="Duplicate openalex_id",
            query="""
                SELECT COUNT(*) as count
                FROM (
                    SELECT openalex_id, COUNT(*) as cnt
                    FROM papers
                    WHERE openalex_id IS NOT NULL
                    GROUP BY openalex_id
                    HAVING COUNT(*) > 1
                ) as duplicates;
            """,
            description="Number of openalex_id values that appear more than once (should be 0)"
        )
        
        # Test for duplicate DOI (informational - DOIs should ideally be unique)
        self.run_test(
            test_name="Duplicate DOI",
            query="""
                SELECT COUNT(*) as count
                FROM (
                    SELECT doi, COUNT(*) as cnt
                    FROM papers
                    WHERE doi IS NOT NULL AND doi != ''
                    GROUP BY doi
                    HAVING COUNT(*) > 1
                ) as duplicates;
            """,
            description="Number of DOI values that appear more than once (informational)"
        )
    
    def print_results(self):
        """Print formatted test results."""
        print("\n" + "=" * 70)
        print("TEST RESULTS SUMMARY")
        print("=" * 70)
        
        for result in self.results:
            print(f"\n{result['status']} - {result['name']}")
            print(f"  {result['description']}")
            
            if 'error' in result:
                print(f"  Error: {result['error']}")
            else:
                if result['name'] == 'Duplicate DOI':
                    # For DOI duplicates, show it's informational
                    print(f"  Found: {result['count']} duplicate DOI(s) (informational)")
                else:
                    print(f"  Found: {result['count']} record(s)")
        
        print("\n" + "-" * 70)
        print(f"Total Tests: {self.total_tests}")
        print(f"Passed: {self.passed_tests}")
        print(f"Failed: {self.failed_tests}")
        print("-" * 70)
        
        if self.failed_tests == 0:
            print("\n✓ All tests passed!")
            return 0
        else:
            print(f"\n✗ {self.failed_tests} test(s) failed. Please review the results above.")
            return 1
    
    def run_all_tests(self):
        """Run all validation tests."""
        print("=" * 70)
        print("PAPERS TABLE DATA VALIDATION TESTS")
        print("=" * 70)
        
        # Check if table exists
        try:
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'papers'
                    );
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    print("\n✗ ERROR: 'papers' table does not exist in the database.")
                    print("Please create the table first using schema.sql or load_papers_from_json.py")
                    return 1
                
                # Get total record count
                cur.execute("SELECT COUNT(*) FROM papers;")
                total_count = cur.fetchone()[0]
                print(f"\nTotal records in papers table: {total_count:,}")
                
        except Exception as e:
            print(f"\n✗ ERROR: Could not connect to database or check table: {e}")
            return 1
        
        # Run all tests
        self.test_missing_required_fields()
        self.test_citation_count_validation()
        self.test_score_range_validation()
        self.test_duplicate_detection()
        
        # Print summary
        return self.print_results()


def main():
    """Main function."""
    validator = PapersDataValidator()
    
    try:
        exit_code = validator.run_all_tests()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nTest execution interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        DatabaseConnection.close_all_connections()


if __name__ == '__main__':
    main()


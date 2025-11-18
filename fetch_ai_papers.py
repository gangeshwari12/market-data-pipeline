#!/usr/bin/env python3
"""
Script to fetch recent AI research papers from OpenAlex API.

This script:
1. Searches OpenAlex Topics for "artificial intelligence" as Field or Subfield
2. Gets the field/subfield IDs from the search results
3. Uses PRIMARY topic filters to fetch only papers where AI is the PRIMARY topic
   (not just one of many topics) - this ensures more relevant results
4. Filters Works from the last 3 days using primary_topic.field.id or primary_topic.subfield.id
5. Saves all papers (with all fields) in a timestamped JSON file inside temp/ folder
"""

import json
import os
from datetime import datetime, timedelta
from pyalex import Works, Topics


def search_ai_field_subfield():
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


def fetch_recent_works(field_id, subfield_id, days=3):
    """Fetch works filtered by PRIMARY field or subfield ID from the last N days.
    
    Uses primary_topic filters to only get papers where AI is the PRIMARY topic,
    not just one of many topics. This significantly reduces irrelevant papers.
    """
    print(f"Fetching works from the last {days} days...")
    print("Using PRIMARY topic filter to get only papers where AI is the main focus...")
    
    # Calculate the date N days ago
    date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # Filter works by primary_topic.field.id OR primary_topic.subfield.id and publication date
    # Using primary_topic ensures we only get papers where AI is the PRIMARY topic
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


def save_to_json(works, days=3, output_dir='temp'):
    """Save works to a timestamped JSON file with metadata."""
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a timestamp for the filename
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.join(output_dir, f'ai_papers_{timestamp_str}.json')
    
    # Create timestamp for metadata (ISO format)
    timestamp_iso = datetime.now().isoformat()
    
    # Create the data structure with metadata and papers
    data = {
        "metadata": {
            "timestamp": timestamp_iso,
            "total_papers": len(works),
            "data_range_days": days,
            "source": "OpenAlex API"
        },
        "papers": works
    }
    
    # Save the data to the file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"\nSaved {len(works)} AI research papers to {filename}")
    return filename


def main():
    """Main function to orchestrate the script."""
    try:
        # Step 1 & 2: Search for AI field/subfield and get IDs
        field_id, subfield_id = search_ai_field_subfield()
        
        # Step 3: Fetch recent works
        works = fetch_recent_works(field_id, subfield_id, days=3)
        
        # Step 4: Save to JSON file
        if works:
            filename = save_to_json(works, days=3)
            print(f"\n✓ Successfully completed! Results saved to: {filename}")
        else:
            print("\n⚠ No works found for the specified criteria.")
    
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        raise


if __name__ == '__main__':
    main()


This projects builds a Data visualization of AI Research papers with key metrics.

Searches OpenAlex API papers for "artificial intelligence"
Gets the concept ID from the search results filtering for a date range of 3 days.
Save all papers (with all fields) in a timestamped JSON file.
Loads the data from JSON.
Connects to a Neon database, creates table.
Processes the data from the papers table.
Inserts the data into the table with deduplication.
Builds visualization using Streamlit.
Resources: Cursor AI, Py, Streamlit, Open Alex API

Dashboard link: https://gangeshwari12-market-data-pipeline-dashboard-7a881w.streamlit.app/

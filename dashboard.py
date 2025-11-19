#!/usr/bin/env python3
"""
Streamlit Dashboard for Market Data Pipeline

This dashboard displays fundamental insights about the research papers data
stored in the Neon database.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys

# Import database connection module
from db_connection import DatabaseConnection, execute_query_dict

# Page configuration
st.set_page_config(
    page_title="Research Papers Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_total_papers():
    """Get total number of papers."""
    query = "SELECT COUNT(*) as count FROM papers;"
    result = execute_query_dict(query)
    return result[0]['count'] if result else 0


@st.cache_data(ttl=300)
def get_papers_by_year():
    """Get papers count grouped by publication year."""
    query = """
        SELECT 
            publication_year,
            COUNT(*) as count
        FROM papers
        WHERE publication_year IS NOT NULL
        GROUP BY publication_year
        ORDER BY publication_year;
    """
    return execute_query_dict(query)


@st.cache_data(ttl=300)
def get_papers_by_field():
    """Get papers count grouped by field."""
    query = """
        SELECT 
            field_name,
            COUNT(*) as count
        FROM papers
        WHERE field_name IS NOT NULL
        GROUP BY field_name
        ORDER BY count DESC
        LIMIT 10;
    """
    return execute_query_dict(query)


@st.cache_data(ttl=300)
def get_papers_by_subfield():
    """Get papers count grouped by subfield."""
    query = """
        SELECT 
            subfield_name,
            COUNT(*) as count
        FROM papers
        WHERE subfield_name IS NOT NULL
        GROUP BY subfield_name
        ORDER BY count DESC
        LIMIT 10;
    """
    return execute_query_dict(query)


@st.cache_data(ttl=300)
def get_open_access_stats():
    """Get open access statistics."""
    query = """
        SELECT 
            oa_status,
            COUNT(*) as count,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM papers WHERE oa_status IS NOT NULL), 2) as percentage
        FROM papers
        WHERE oa_status IS NOT NULL
        GROUP BY oa_status
        ORDER BY count DESC;
    """
    return execute_query_dict(query)


@st.cache_data(ttl=300)
def get_citation_stats():
    """Get citation statistics."""
    query = """
        SELECT 
            COUNT(*) as total_papers,
            AVG(cited_by_count) as avg_citations,
            MAX(cited_by_count) as max_citations,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cited_by_count) as median_citations,
            COUNT(CASE WHEN is_top_10_percent THEN 1 END) as top_10_percent_count,
            COUNT(CASE WHEN is_top_1_percent THEN 1 END) as top_1_percent_count
        FROM papers
        WHERE cited_by_count IS NOT NULL;
    """
    result = execute_query_dict(query)
    return result[0] if result else {}


@st.cache_data(ttl=300)
def get_top_papers(limit=10):
    """Get top papers by citation count."""
    query = """
        SELECT 
            title,
            publication_year,
            cited_by_count,
            field_name,
            subfield_name,
            oa_status,
            citation_percentile
        FROM papers
        WHERE cited_by_count IS NOT NULL
        ORDER BY cited_by_count DESC
        LIMIT %s;
    """
    return execute_query_dict(query, params=(limit,))


@st.cache_data(ttl=300)
def get_collaboration_stats():
    """Get collaboration statistics."""
    query = """
        SELECT 
            AVG(countries_count) as avg_countries,
            AVG(institutions_count) as avg_institutions,
            MAX(countries_count) as max_countries,
            MAX(institutions_count) as max_institutions
        FROM papers
        WHERE countries_count IS NOT NULL OR institutions_count IS NOT NULL;
    """
    result = execute_query_dict(query)
    return result[0] if result else {}


@st.cache_data(ttl=300)
def get_fwci_stats():
    """Get Field-Weighted Citation Impact statistics."""
    query = """
        SELECT 
            AVG(fwci) as avg_fwci,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fwci) as median_fwci,
            MAX(fwci) as max_fwci
        FROM papers
        WHERE fwci IS NOT NULL;
    """
    result = execute_query_dict(query)
    return result[0] if result else {}


def main():
    """Main dashboard function."""
    # Header
    st.markdown('<h1 class="main-header">📊 Research Papers Dashboard</h1>', unsafe_allow_html=True)
    
    # Test database connection
    try:
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT 1;")
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        st.info("Please ensure your .env file contains the DB_PASSWORD variable.")
        return
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Get total papers for context
    total_papers = get_total_papers()
    st.sidebar.metric("Total Papers", f"{total_papers:,}")
    
    # Main content
    if total_papers == 0:
        st.warning("No papers found in the database. Please load data first.")
        return
    
    # Key Metrics Row
    st.header("📈 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    citation_stats = get_citation_stats()
    with col1:
        st.metric(
            "Average Citations",
            f"{citation_stats.get('avg_citations', 0):.1f}" if citation_stats.get('avg_citations') else "N/A"
        )
    with col2:
        st.metric(
            "Median Citations",
            f"{citation_stats.get('median_citations', 0):.0f}" if citation_stats.get('median_citations') else "N/A"
        )
    with col3:
        top_10_pct = citation_stats.get('top_10_percent_count', 0)
        st.metric(
            "Top 10% Papers",
            f"{top_10_pct:,}",
            f"({100.0 * top_10_pct / total_papers:.1f}%)"
        )
    with col4:
        top_1_pct = citation_stats.get('top_1_percent_count', 0)
        st.metric(
            "Top 1% Papers",
            f"{top_1_pct:,}",
            f"({100.0 * top_1_pct / total_papers:.1f}%)"
        )
    
    # Papers by Year
    st.header("📅 Papers by Publication Year")
    papers_by_year = get_papers_by_year()
    if papers_by_year:
        df_year = pd.DataFrame(papers_by_year)
        fig_year = px.bar(
            df_year,
            x='publication_year',
            y='count',
            title='Number of Papers Published by Year',
            labels={'publication_year': 'Year', 'count': 'Number of Papers'},
            color='count',
            color_continuous_scale='Blues'
        )
        fig_year.update_layout(showlegend=False)
        st.plotly_chart(fig_year, use_container_width=True)
    
    # Two-column layout for field/subfield
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔬 Top Fields")
        papers_by_field = get_papers_by_field()
        if papers_by_field:
            df_field = pd.DataFrame(papers_by_field)
            fig_field = px.pie(
                df_field,
                values='count',
                names='field_name',
                title='Papers by Field'
            )
            st.plotly_chart(fig_field, use_container_width=True)
    
    with col2:
        st.subheader("🧪 Top Subfields")
        papers_by_subfield = get_papers_by_subfield()
        if papers_by_subfield:
            df_subfield = pd.DataFrame(papers_by_subfield)
            fig_subfield = px.bar(
                df_subfield,
                x='count',
                y='subfield_name',
                orientation='h',
                title='Top 10 Subfields by Paper Count',
                labels={'count': 'Number of Papers', 'subfield_name': 'Subfield'},
                color='count',
                color_continuous_scale='Greens'
            )
            fig_subfield.update_layout(showlegend=False, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_subfield, use_container_width=True)
    
    # Open Access Statistics
    st.header("🔓 Open Access Statistics")
    oa_stats = get_open_access_stats()
    if oa_stats:
        col1, col2 = st.columns(2)
        
        with col1:
            df_oa = pd.DataFrame(oa_stats)
            fig_oa = px.pie(
                df_oa,
                values='count',
                names='oa_status',
                title='Open Access Status Distribution',
                color_discrete_map={
                    'gold': '#FFD700',
                    'bronze': '#CD7F32',
                    'green': '#90EE90',
                    'hybrid': '#9370DB',
                    'closed': '#808080'
                }
            )
            st.plotly_chart(fig_oa, use_container_width=True)
        
        with col2:
            st.dataframe(
                df_oa[['oa_status', 'count', 'percentage']].style.format({
                    'count': '{:,.0f}',
                    'percentage': '{:.2f}%'
                }),
                use_container_width=True,
                hide_index=True
            )
    
    # Citation Analysis
    st.header("📊 Citation Analysis")
    citation_stats = get_citation_stats()
    if citation_stats:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Max Citations",
                f"{citation_stats.get('max_citations', 0):,}" if citation_stats.get('max_citations') else "N/A"
            )
        
        fwci_stats = get_fwci_stats()
        with col2:
            st.metric(
                "Average FWCI",
                f"{fwci_stats.get('avg_fwci', 0):.2f}" if fwci_stats.get('avg_fwci') else "N/A"
            )
        
        with col3:
            st.metric(
                "Median FWCI",
                f"{fwci_stats.get('median_fwci', 0):.2f}" if fwci_stats.get('median_fwci') else "N/A"
            )
    
    # Top Papers
    st.header("🏆 Top Papers by Citations")
    top_papers = get_top_papers(limit=20)
    if top_papers:
        df_top = pd.DataFrame(top_papers)
        # Truncate long titles for display
        df_top['title_display'] = df_top['title'].apply(
            lambda x: x[:80] + '...' if len(x) > 80 else x
        )
        
        st.dataframe(
            df_top[['title_display', 'publication_year', 'cited_by_count', 'field_name', 'oa_status']].rename(columns={
                'title_display': 'Title',
                'publication_year': 'Year',
                'cited_by_count': 'Citations',
                'field_name': 'Field',
                'oa_status': 'OA Status'
            }),
            use_container_width=True,
            hide_index=True
        )
    
    # Collaboration Metrics
    st.header("🤝 Collaboration Metrics")
    collab_stats = get_collaboration_stats()
    if collab_stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Avg Countries",
                f"{collab_stats.get('avg_countries', 0):.2f}" if collab_stats.get('avg_countries') else "N/A"
            )
        with col2:
            st.metric(
                "Avg Institutions",
                f"{collab_stats.get('avg_institutions', 0):.2f}" if collab_stats.get('avg_institutions') else "N/A"
            )
        with col3:
            st.metric(
                "Max Countries",
                f"{collab_stats.get('max_countries', 0):.0f}" if collab_stats.get('max_countries') else "N/A"
            )
        with col4:
            st.metric(
                "Max Institutions",
                f"{collab_stats.get('max_institutions', 0):.0f}" if collab_stats.get('max_institutions') else "N/A"
            )
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: #666;'>"
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Total Papers: {total_papers:,}"
        f"</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()



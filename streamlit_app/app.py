import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Page configuration
st.set_page_config(
    page_title="Yelp Analytics Dashboard",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    """Load all processed data files"""
    data = {}
    
    # Enhanced data files
    enhanced_files = {
        'top_businesses_all_cities': '../data/processed/top_businesses_all_cities.parquet',
        'city_comparison_enhanced': '../data/processed/city_comparison_enhanced.parquet',
        'category_analysis': '../data/processed/category_analysis.parquet',
        'top_performing_cities': '../data/processed/top_performing_cities.parquet',
        'rating_tiers_analysis': '../data/processed/rating_tiers_analysis.parquet',
        'user_engagement': '../data/processed/user_engagement.parquet',
        'user_business_interactions': '../data/processed/user_business_interactions.parquet',
        'user_sentiment': '../data/processed/user_sentiment.parquet',
        'user_compliments': '../data/processed/user_compliments.parquet',
        'user_activity_timeline': '../data/processed/user_activity_timeline.parquet',
        'elite_users': '../data/processed/elite_users.parquet'
    }
    
    # Legacy files for backward compatibility
    legacy_files = {
        'philadelphia_top': 'data/philadelphia_top.parquet',
        'city_comparison': 'data/city_comparison.parquet',
        'high_rated_businesses': 'data/high_rated_businesses.parquet'
    }
    
    # Load enhanced files
    for key, file_path in enhanced_files.items():
        try:
            if os.path.exists(file_path):
                data[key] = pd.read_parquet(file_path)
                st.success(f"✅ Loaded {key}")
            else:
                st.warning(f"⚠️ File not found: {file_path}")
        except Exception as e:
            st.error(f"❌ Error loading {key}: {str(e)}")
    
    # Load legacy files if enhanced versions not available
    for key, file_path in legacy_files.items():
        if key not in data:
            try:
                if os.path.exists(file_path):
                    data[key] = pd.read_parquet(file_path)
                    st.info(f"📁 Loaded legacy file: {key}")
            except Exception as e:
                st.warning(f"⚠️ Could not load legacy file {key}: {str(e)}")
    
    return data

def show_dashboard_overview(data):
    """Main dashboard overview page with enhanced insights"""
    st.markdown('<h1 class="main-header">🍽️ Yelp Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'top_businesses_all_cities' in data:
            st.metric("Top Businesses", len(data['top_businesses_all_cities']))
        elif 'philadelphia_top' in data:
            st.metric("Philadelphia Businesses", len(data['philadelphia_top']))
    
    with col2:
        if 'city_comparison_enhanced' in data:
            st.metric("Cities Analyzed", len(data['city_comparison_enhanced']))
        elif 'city_comparison' in data:
            st.metric("Cities Analyzed", len(data['city_comparison']))
    
    with col3:
        if 'user_engagement' in data:
            st.metric("Active Users", len(data['user_engagement']))
    
    with col4:
        if 'elite_users' in data:
            st.metric("Elite Users", len(data['elite_users']))
    
    st.markdown("---")
    
    # Overview charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<h3 class="section-header">🏢 Business Performance by City</h3>', unsafe_allow_html=True)
        if 'city_comparison_enhanced' in data:
            fig = px.bar(
                data['city_comparison_enhanced'],
                x='city',
                y='total_reviews',
                title="Total Reviews by City",
                color='avg_business_rating',
                color_continuous_scale='viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        elif 'city_comparison' in data:
            fig = px.bar(
                data['city_comparison'],
                x='city',
                y='total_reviews',
                title="Total Reviews by City",
                color='avg_business_rating',
                color_continuous_scale='viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown('<h3 class="section-header">👥 User Engagement Distribution</h3>', unsafe_allow_html=True)
        if 'user_engagement' in data:
            fig = px.histogram(
                data['user_engagement'],
                x='review_count',
                nbins=20,
                title="User Review Count Distribution",
                color_discrete_sequence=['#FF6B6B']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Enhanced top performers section
    st.markdown('<h3 class="section-header">🏆 Top Performers</h3>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**⭐ Top Rated Businesses**")
        if 'top_businesses_all_cities' in data:
            top_businesses = data['top_businesses_all_cities'].head(5)
            for _, row in top_businesses.iterrows():
                st.markdown(f"• **{row['name']}** ({row['city']}) - ⭐ {row['avg_review_rating']:.2f} ({row['rating_category']})")
        elif 'high_rated_businesses' in data:
            top_businesses = data['high_rated_businesses'].head(5)
            for _, row in top_businesses.iterrows():
                st.markdown(f"• **{row['name']}** ({row['city']}) - ⭐ {row['avg_review_rating']:.2f} ({row['rating_category']})")
    
    with col2:
        st.markdown("**👤 Most Active Users**")
        if 'user_engagement' in data:
            top_users = data['user_engagement'].head(5)
            for _, row in top_users.iterrows():
                st.markdown(f"• **{row['name']}** - 📝 {row['review_count']} reviews ({row['user_category']})")
    
    # Rating category insights
    st.markdown('<h3 class="section-header">📊 Rating Category Insights</h3>', unsafe_allow_html=True)
    
    if 'top_businesses_all_cities' in data:
        col1, col2 = st.columns(2)
        
        with col1:
            # Rating category distribution
            category_counts = data['top_businesses_all_cities']['rating_category'].value_counts()
            fig = px.pie(
                values=category_counts.values,
                names=category_counts.index,
                title="High-Rated Businesses by Category",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Average rating by category
            category_stats = data['top_businesses_all_cities'].groupby('rating_category').agg({
                'avg_review_rating': 'mean',
                'total_reviews': 'sum',
                'business_id': 'count'
            }).reset_index()
            
            fig = px.bar(
                category_stats,
                x='rating_category',
                y='avg_review_rating',
                title="Average Rating by Category",
                color='total_reviews',
                color_continuous_scale='viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
    elif 'high_rated_businesses' in data:
        col1, col2 = st.columns(2)
        
        with col1:
            # Rating category distribution
            category_counts = data['high_rated_businesses']['rating_category'].value_counts()
            fig = px.pie(
                values=category_counts.values,
                names=category_counts.index,
                title="High-Rated Businesses by Category",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Average rating by category
            category_stats = data['high_rated_businesses'].groupby('rating_category').agg({
                'avg_review_rating': 'mean',
                'total_reviews': 'sum',
                'business_id': 'count'
            }).reset_index()
            
            fig = px.bar(
                category_stats,
                x='rating_category',
                y='avg_review_rating',
                title="Average Rating by Category",
                color='total_reviews',
                color_continuous_scale='viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # City performance insights
    st.markdown('<h3 class="section-header">🏙️ City Performance Insights</h3>', unsafe_allow_html=True)
    
    if 'top_performing_cities' in data:
        col1, col2 = st.columns(2)
        
        with col1:
            # Best performing cities by rating
            best_cities = data['top_performing_cities'].sort_values('avg_business_rating', ascending=False).head(5)
            fig = px.bar(
                best_cities,
                x='city',
                y='avg_business_rating',
                title="Top Cities by Average Business Rating",
                color_discrete_sequence=['#4ECDC4']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Most active cities by reviews
            active_cities = data['top_performing_cities'].sort_values('total_reviews', ascending=False).head(5)
            fig = px.bar(
                active_cities,
                x='city',
                y='total_reviews',
                title="Most Active Cities by Total Reviews",
                color_discrete_sequence=['#45B7D1']
            )
            st.plotly_chart(fig, use_container_width=True)
    elif 'city_comparison_enhanced' in data:
        col1, col2 = st.columns(2)
        
        with col1:
            # Best performing cities by rating
            best_cities = data['city_comparison_enhanced'].sort_values('avg_business_rating', ascending=False).head(5)
            fig = px.bar(
                best_cities,
                x='city',
                y='avg_business_rating',
                title="Top Cities by Average Business Rating",
                color_discrete_sequence=['#4ECDC4']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Most active cities by reviews
            active_cities = data['city_comparison_enhanced'].sort_values('total_reviews', ascending=False).head(5)
            fig = px.bar(
                active_cities,
                x='city',
                y='total_reviews',
                title="Most Active Cities by Total Reviews",
                color_discrete_sequence=['#45B7D1']
            )
            st.plotly_chart(fig, use_container_width=True)
    elif 'city_comparison' in data:
        col1, col2 = st.columns(2)
        
        with col1:
            # Best performing cities by rating
            best_cities = data['city_comparison'].sort_values('avg_business_rating', ascending=False).head(5)
            fig = px.bar(
                best_cities,
                x='city',
                y='avg_business_rating',
                title="Top Cities by Average Business Rating",
                color_discrete_sequence=['#4ECDC4']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Most active cities by reviews
            active_cities = data['city_comparison'].sort_values('total_reviews', ascending=False).head(5)
            fig = px.bar(
                active_cities,
                x='city',
                y='total_reviews',
                title="Most Active Cities by Total Reviews",
                color_discrete_sequence=['#45B7D1']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Category analysis insights
    if 'category_analysis' in data:
        st.markdown('<h3 class="section-header">🏷️ Category Analysis Insights</h3>', unsafe_allow_html=True)
        
        # Top categories across all cities
        top_categories = data['category_analysis'].groupby('primary_category').agg({
            'business_count': 'sum',
            'total_reviews': 'sum',
            'avg_business_rating': 'mean'
        }).reset_index().sort_values('total_reviews', ascending=False).head(10)
        
        fig = px.bar(
            top_categories,
            x='primary_category',
            y='total_reviews',
            title="Top Categories by Total Reviews",
            color='avg_business_rating',
            color_continuous_scale='viridis'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

def show_business_analysis(data):
    """Business analysis page with enhanced filtering and insights"""
    st.markdown('<h1 class="main-header">🏢 Business Analysis</h1>', unsafe_allow_html=True)
    
    # Filter section
    st.markdown('<h3 class="section-header">🔍 Filter Options</h3>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # City filter
        if 'top_businesses_all_cities' in data:
            cities = sorted(data['top_businesses_all_cities']['city'].unique())
            selected_city = st.selectbox("Select City", ["All Cities"] + list(cities))
        elif 'philadelphia_top' in data:
            selected_city = "Philadelphia"
        else:
            selected_city = "All Cities"
    
    with col2:
        # Category filter
        if 'top_businesses_all_cities' in data:
            categories = sorted(data['top_businesses_all_cities']['primary_category'].unique())
            selected_category = st.selectbox("Select Category", ["All Categories"] + list(categories))
        elif 'high_rated_businesses' in data:
            categories = sorted(data['high_rated_businesses']['primary_category'].unique())
            selected_category = st.selectbox("Select Category", ["All Categories"] + list(categories))
        else:
            selected_category = "All Categories"
    
    with col3:
        # Rating filter
        rating_options = ["All Ratings", "5-Star", "4-Star+", "3-Star+"]
        selected_rating = st.selectbox("Minimum Rating", rating_options)
    
    # Apply filters
    if 'top_businesses_all_cities' in data:
        filtered_data = data['top_businesses_all_cities'].copy()
        
        if selected_city != "All Cities":
            filtered_data = filtered_data[filtered_data['city'] == selected_city]
        
        if selected_category != "All Categories":
            filtered_data = filtered_data[filtered_data['primary_category'] == selected_category]
        
        if selected_rating != "All Ratings":
            if selected_rating == "5-Star":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 5.0]
            elif selected_rating == "4-Star+":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 4.0]
            elif selected_rating == "3-Star+":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 3.0]
    
    elif 'high_rated_businesses' in data:
        filtered_data = data['high_rated_businesses'].copy()
        
        if selected_category != "All Categories":
            filtered_data = filtered_data[filtered_data['primary_category'] == selected_category]
        
        if selected_rating != "All Ratings":
            if selected_rating == "5-Star":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 5.0]
            elif selected_rating == "4-Star+":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 4.0]
            elif selected_rating == "3-Star+":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 3.0]
    
    elif 'philadelphia_top' in data:
        filtered_data = data['philadelphia_top'].copy()
        
        if selected_category != "All Categories":
            filtered_data = filtered_data[filtered_data['primary_category'] == selected_category]
        
        if selected_rating != "All Ratings":
            if selected_rating == "5-Star":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 5.0]
            elif selected_rating == "4-Star+":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 4.0]
            elif selected_rating == "3-Star+":
                filtered_data = filtered_data[filtered_data['avg_review_rating'] >= 3.0]
    
    else:
        st.error("No business data available")
        return
    
    # Display filtered results
    st.markdown(f"<h3 class='section-header'>📊 Analysis Results ({len(filtered_data)} businesses)</h3>", unsafe_allow_html=True)
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Businesses", len(filtered_data))
    
    with col2:
        avg_rating = filtered_data['avg_review_rating'].mean()
        st.metric("Average Rating", f"{avg_rating:.2f} ⭐")
    
    with col3:
        total_reviews = filtered_data['total_reviews'].sum()
        st.metric("Total Reviews", f"{total_reviews:,}")
    
    with col4:
        if 'city' in filtered_data.columns:
            unique_cities = filtered_data['city'].nunique()
            st.metric("Cities", unique_cities)
        else:
            st.metric("Categories", filtered_data['primary_category'].nunique())
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Rating distribution
        fig = px.histogram(
            filtered_data,
            x='avg_review_rating',
            nbins=20,
            title="Rating Distribution",
            color_discrete_sequence=['#FF6B6B']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Review count vs Rating scatter
        fig = px.scatter(
            filtered_data,
            x='total_reviews',
            y='avg_review_rating',
            title="Reviews vs Rating",
            color='avg_review_rating',
            color_continuous_scale='viridis',
            hover_data=['name', 'city'] if 'city' in filtered_data.columns else ['name']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Category analysis
    if 'primary_category' in filtered_data.columns:
        st.markdown('<h3 class="section-header">🏷️ Category Analysis</h3>', unsafe_allow_html=True)
        
        category_stats = filtered_data.groupby('primary_category').agg({
            'business_id': 'count',
            'avg_review_rating': 'mean',
            'total_reviews': 'sum'
        }).reset_index()
        category_stats.columns = ['Category', 'Business Count', 'Avg Rating', 'Total Reviews']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top categories by business count
            top_categories = category_stats.sort_values('Business Count', ascending=False).head(10)
            fig = px.bar(
                top_categories,
                x='Category',
                y='Business Count',
                title="Top Categories by Business Count",
                color_discrete_sequence=['#4ECDC4']
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Top categories by average rating
            top_rated_categories = category_stats.sort_values('Avg Rating', ascending=False).head(10)
            fig = px.bar(
                top_rated_categories,
                x='Category',
                y='Avg Rating',
                title="Top Categories by Average Rating",
                color_discrete_sequence=['#45B7D1']
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
    
    # City analysis (if multiple cities)
    if 'city' in filtered_data.columns and filtered_data['city'].nunique() > 1:
        st.markdown('<h3 class="section-header">🏙️ City Analysis</h3>', unsafe_allow_html=True)
        
        city_stats = filtered_data.groupby('city').agg({
            'business_id': 'count',
            'avg_review_rating': 'mean',
            'total_reviews': 'sum'
        }).reset_index()
        city_stats.columns = ['City', 'Business Count', 'Avg Rating', 'Total Reviews']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Cities by business count
            top_cities = city_stats.sort_values('Business Count', ascending=False).head(10)
            fig = px.bar(
                top_cities,
                x='City',
                y='Business Count',
                title="Cities by Business Count",
                color_discrete_sequence=['#FFA07A']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Cities by average rating
            top_rated_cities = city_stats.sort_values('Avg Rating', ascending=False).head(10)
            fig = px.bar(
                top_rated_cities,
                x='City',
                y='Avg Rating',
                title="Cities by Average Rating",
                color_discrete_sequence=['#98D8C8']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Business details table
    st.markdown('<h3 class="section-header">📋 Business Details</h3>', unsafe_allow_html=True)
    
    # Sort options
    sort_by = st.selectbox("Sort by", ["Name", "Rating (High to Low)", "Reviews (High to Low)", "City"])
    
    if sort_by == "Name":
        display_data = filtered_data.sort_values('name')
    elif sort_by == "Rating (High to Low)":
        display_data = filtered_data.sort_values('avg_review_rating', ascending=False)
    elif sort_by == "Reviews (High to Low)":
        display_data = filtered_data.sort_values('total_reviews', ascending=False)
    elif sort_by == "City":
        display_data = filtered_data.sort_values('city')
    
    # Display table
    if 'city' in display_data.columns:
        display_columns = ['name', 'city', 'primary_category', 'avg_review_rating', 'total_reviews', 'rating_category']
        column_names = ['Business Name', 'City', 'Category', 'Avg Rating', 'Total Reviews', 'Rating Category']
    else:
        display_columns = ['name', 'primary_category', 'avg_review_rating', 'total_reviews', 'rating_category']
        column_names = ['Business Name', 'Category', 'Avg Rating', 'Total Reviews', 'Rating Category']
    
    st.dataframe(
        display_data[display_columns].rename(columns=dict(zip(display_columns, column_names))),
        use_container_width=True,
        height=400
    )

def show_user_analytics(data):
    """User analytics page with enhanced user insights"""
    st.markdown('<h1 class="main-header">👥 User Analytics</h1>', unsafe_allow_html=True)
    
    # Check for user data availability
    user_data_available = any(key in data for key in ['user_engagement', 'user_business_interactions', 'user_sentiment', 'elite_users'])
    
    if not user_data_available:
        st.warning("User analytics data not available. Please ensure user data files are processed.")
        return
    
    # Key user metrics
    st.markdown('<h3 class="section-header">📊 User Overview</h3>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'user_engagement' in data:
            st.metric("Total Users", len(data['user_engagement']))
        else:
            st.metric("Total Users", "N/A")
    
    with col2:
        if 'elite_users' in data:
            st.metric("Elite Users", len(data['elite_users']))
        else:
            st.metric("Elite Users", "N/A")
    
    with col3:
        if 'user_engagement' in data:
            avg_reviews = data['user_engagement']['review_count'].mean()
            st.metric("Avg Reviews/User", f"{avg_reviews:.1f}")
        else:
            st.metric("Avg Reviews/User", "N/A")
    
    with col4:
        if 'user_engagement' in data:
            avg_rating = data['user_engagement']['avg_rating'].mean()
            st.metric("Avg User Rating", f"{avg_rating:.2f} ⭐")
        else:
            st.metric("Avg User Rating", "N/A")
    
    # User engagement analysis
    if 'user_engagement' in data:
        st.markdown('<h3 class="section-header">📈 User Engagement Analysis</h3>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Review count distribution
            fig = px.histogram(
                data['user_engagement'],
                x='review_count',
                nbins=30,
                title="User Review Count Distribution",
                color_discrete_sequence=['#FF6B6B']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Average rating distribution
            fig = px.histogram(
                data['user_engagement'],
                x='avg_rating',
                nbins=20,
                title="User Average Rating Distribution",
                color_discrete_sequence=['#4ECDC4']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # User categories analysis
        st.markdown('<h3 class="section-header">👤 User Categories</h3>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # User category distribution
            category_counts = data['user_engagement']['user_category'].value_counts()
            fig = px.pie(
                values=category_counts.values,
                names=category_counts.index,
                title="User Category Distribution",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Average metrics by user category
            category_stats = data['user_engagement'].groupby('user_category').agg({
                'review_count': 'mean',
                'avg_rating': 'mean',
                'useful_votes': 'mean',
                'funny_votes': 'mean',
                'cool_votes': 'mean'
            }).reset_index()
            
            fig = px.bar(
                category_stats,
                x='user_category',
                y=['review_count', 'useful_votes', 'funny_votes', 'cool_votes'],
                title="Average Metrics by User Category",
                barmode='group'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Elite users analysis
    if 'elite_users' in data:
        st.markdown('<h3 class="section-header">👑 Elite Users Analysis</h3>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Elite user review count distribution
            fig = px.histogram(
                data['elite_users'],
                x='review_count',
                nbins=20,
                title="Elite User Review Count Distribution",
                color_discrete_sequence=['#FFD700']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Elite user average rating
            fig = px.histogram(
                data['elite_users'],
                x='avg_rating',
                nbins=15,
                title="Elite User Average Rating Distribution",
                color_discrete_sequence=['#FFA500']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Top elite users
        st.markdown('<h3 class="section-header">🏆 Top Elite Users</h3>', unsafe_allow_html=True)
        
        top_elite = data['elite_users'].sort_values('review_count', ascending=False).head(10)
        st.dataframe(
            top_elite[['name', 'review_count', 'avg_rating', 'useful_votes', 'funny_votes', 'cool_votes']].rename(columns={
                'name': 'User Name',
                'review_count': 'Reviews',
                'avg_rating': 'Avg Rating',
                'useful_votes': 'Useful Votes',
                'funny_votes': 'Funny Votes',
                'cool_votes': 'Cool Votes'
            }),
            use_container_width=True,
            height=300
        )
    
    # User sentiment analysis
    if 'user_sentiment' in data:
        st.markdown('<h3 class="section-header">😊 User Sentiment Analysis</h3>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Sentiment distribution
            sentiment_counts = data['user_sentiment']['sentiment'].value_counts()
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="User Sentiment Distribution",
                color_discrete_map={
                    'Positive': '#4ECDC4',
                    'Neutral': '#45B7D1',
                    'Negative': '#FF6B6B'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Sentiment by review count
            sentiment_stats = data['user_sentiment'].groupby('sentiment').agg({
                'total_reviews': 'mean',
                'avg_rating_given': 'mean'
            }).reset_index()
            
            fig = px.bar(
                sentiment_stats,
                x='sentiment',
                y='avg_rating_given',
                title="Average Rating by Sentiment",
                color_discrete_sequence=['#98D8C8']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # User business interactions
    if 'user_business_interactions' in data:
        st.markdown('<h3 class="section-header">🤝 User-Business Interactions</h3>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Interaction types distribution
            interaction_counts = data['user_business_interactions']['interaction_type'].value_counts()
            fig = px.bar(
                x=interaction_counts.index,
                y=interaction_counts.values,
                title="User-Business Interaction Types",
                color_discrete_sequence=['#FFA07A']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Average interaction metrics
            interaction_stats = data['user_business_interactions'].groupby('interaction_type').agg({
                'interaction_count': 'mean',
                'avg_rating': 'mean'
            }).reset_index()
            
            fig = px.scatter(
                interaction_stats,
                x='interaction_count',
                y='avg_rating',
                size='interaction_count',
                color='interaction_type',
                title="Interaction Count vs Average Rating",
                hover_data=['interaction_type']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # User activity timeline
    if 'user_activity_timeline' in data:
        st.markdown('<h3 class="section-header">📅 User Activity Timeline</h3>', unsafe_allow_html=True)
        
        # Activity over time
        timeline_data = data['user_activity_timeline'].groupby('activity_date').agg({
            'user_count': 'sum',
            'review_count': 'sum',
            'avg_rating': 'mean'
        }).reset_index()
        
        fig = px.line(
            timeline_data,
            x='activity_date',
            y=['user_count', 'review_count'],
            title="User Activity Over Time",
            labels={'value': 'Count', 'variable': 'Metric'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # User compliments analysis
    if 'user_compliments' in data:
        st.markdown('<h3 class="section-header">💝 User Compliments Analysis</h3>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Compliment types distribution
            compliment_counts = data['user_compliments']['compliment_type'].value_counts()
            fig = px.pie(
                values=compliment_counts.values,
                names=compliment_counts.index,
                title="User Compliment Types Distribution",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Top complimented users
            top_complimented = data['user_compliments'].groupby('user_name').agg({
                'compliment_count': 'sum'
            }).reset_index().sort_values('compliment_count', ascending=False).head(10)
            
            fig = px.bar(
                top_complimented,
                x='user_name',
                y='compliment_count',
                title="Top Complimented Users",
                color_discrete_sequence=['#FFB6C1']
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)

def main():
    """Main application"""
    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        color: #FF6B6B;
        text-align: center;
        margin-bottom: 2rem;
        font-size: 2.5rem;
        font-weight: bold;
    }
    .section-header {
        color: #4ECDC4;
        margin-top: 2rem;
        margin-bottom: 1rem;
        font-size: 1.5rem;
        font-weight: bold;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #FF6B6B;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Load data
    data = load_data()
    
    # Sidebar navigation
    st.sidebar.markdown('<h2 class="section-header">📊 Navigation</h2>', unsafe_allow_html=True)
    
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["🏠 Dashboard Overview", "🏢 Business Analysis", "👥 User Analytics"]
    )
    
    # Page routing
    if page == "🏠 Dashboard Overview":
        show_dashboard_overview(data)
    elif page == "🏢 Business Analysis":
        show_business_analysis(data)
    elif page == "👥 User Analytics":
        show_user_analytics(data)

if __name__ == "__main__":
    main()

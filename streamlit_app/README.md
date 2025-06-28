# 🍽️ Yelp Analytics Dashboard

A comprehensive Streamlit dashboard for visualizing Yelp dataset analytics, including business performance, user engagement, and interactive data exploration.

## 🚀 Features

### 📊 Dashboard Overview
- **Key Metrics**: Quick overview of Philadelphia businesses, cities analyzed, active users, and elite users
- **Business Performance**: Visual comparison of business performance across cities
- **User Engagement**: Distribution analysis of user review activity
- **Top Performers**: Lists of top-rated businesses and most active users

### 🏢 Business Analytics
- **Philadelphia Analysis**: Detailed analysis of Philadelphia businesses with scatter plots and bar charts
- **Cross-City Comparison**: Comparison of business metrics across different cities
- **High-Rated Businesses**: Analysis of businesses with excellent ratings and significant review counts

### 👥 User Analytics
- **User Engagement**: Analysis of user activity patterns, elite status, and user categories
- **User Sentiment**: Sentiment analysis showing positive/negative review patterns
- **User Compliments**: Analysis of user recognition through different compliment types
- **Elite Users**: Detailed analysis of elite user characteristics and behavior

### 📊 Interactive Analysis
- **Dynamic Filtering**: Filter businesses by city and minimum rating
- **User-Business Interactions**: Analysis of how users interact with businesses
- **Real-time Visualization**: Interactive charts that respond to user selections

### 📈 Trends & Insights
- **Key Insights**: Automated generation of key findings from the data
- **Correlation Analysis**: Correlation matrices for user engagement metrics
- **Distribution Analysis**: Box plots and distribution charts for deeper insights

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8+
- pip package manager

### Quick Start
1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the dashboard**:
   ```bash
   ./run_app.sh
   ```
   
   Or manually:
   ```bash
   streamlit run app.py --server.port 8501 --server.address 0.0.0.0
   ```

3. **Access the dashboard**:
   Open your browser and navigate to: `http://localhost:8501`

## 📁 Data Sources

The dashboard reads from the following processed data files:
- `philadelphia_top.parquet` - Philadelphia business analysis
- `city_comparison.parquet` - Cross-city business comparison
- `high_rated_businesses.parquet` - High-rated businesses analysis
- `user_engagement.parquet` - User engagement patterns
- `user_business_interactions.parquet` - User-business interaction analysis
- `user_sentiment.parquet` - User sentiment analysis
- `user_compliments.parquet` - User compliment analysis
- `user_activity_timeline.parquet` - User activity timeline
- `elite_users.parquet` - Elite user analysis

## 🎨 Visualization Features

### Interactive Charts
- **Scatter Plots**: Multi-dimensional data visualization with size and color encoding
- **Bar Charts**: Comparative analysis with grouped and stacked options
- **Pie Charts**: Distribution analysis for categorical data
- **Histograms**: Frequency distribution analysis
- **Box Plots**: Statistical distribution analysis
- **Heatmaps**: Correlation analysis matrices

### Custom Styling
- Modern, responsive design
- Custom CSS styling for better user experience
- Color-coded metrics and categories
- Hover tooltips with detailed information

## 🔧 Configuration

### Port Configuration
The default port is 8501. You can change it by modifying the `run_app.sh` script or running:
```bash
streamlit run app.py --server.port <YOUR_PORT>
```

### Data Path Configuration
The app expects data files to be in the `../data/processed/` directory relative to the app location. You can modify the paths in the `load_data()` function if needed.

## 📊 Key Metrics Explained

### Business Metrics
- **Total Reviews**: Number of reviews for each business
- **Average Rating**: Mean rating given by users
- **Business Stars**: Official business rating
- **Review Count**: Number of reviews the business has received

### User Metrics
- **Review Count**: Total number of reviews written by the user
- **Average Stars**: Average rating given by the user
- **Useful/Funny/Cool**: Engagement metrics for user reviews
- **Fans**: Number of followers
- **Elite Status**: Whether the user has elite status

### Sentiment Metrics
- **Positive Review Percentage**: Percentage of reviews with 4+ stars
- **Negative Review Percentage**: Percentage of reviews with 2 or fewer stars
- **Rating Consistency**: Standard deviation of user ratings

## 🚀 Performance Tips

- The app uses `@st.cache_data` for efficient data loading
- Large datasets are processed efficiently with pandas
- Interactive charts are optimized for smooth user experience

## 🔍 Troubleshooting

### Common Issues
1. **Missing data files**: Ensure all processed data files are in the correct location
2. **Port conflicts**: Change the port number if 8501 is already in use
3. **Dependency issues**: Run `pip install -r requirements.txt` to install all dependencies

### Data Loading Errors
- Check file paths in the `load_data()` function
- Ensure parquet files are not corrupted
- Verify file permissions

## 📈 Future Enhancements

Potential improvements for the dashboard:
- Real-time data updates
- Export functionality for charts and data
- Advanced filtering options
- Machine learning insights
- Geographic visualization with maps
- Time series analysis
- Comparative analysis tools

## 🤝 Contributing

To contribute to this dashboard:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is part of the Yelp Analytics pipeline and follows the same licensing terms. 
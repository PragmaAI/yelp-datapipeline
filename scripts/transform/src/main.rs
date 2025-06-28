use datafusion::prelude::*;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::dataframe::DataFrameWriteOptions;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    println!("📊 Registering business data...");
    ctx.register_parquet("business", "/app/data/raw/business.parquet", ParquetReadOptions::default()).await?;
    
    println!("📊 Registering review data with wildcard pattern...");
    ctx.register_parquet("review", "/app/data/raw/review_part_*.parquet", ParquetReadOptions::default()).await?;

    println!("📊 Registering user data...");
    ctx.register_parquet("user", "/app/data/raw/user.parquet", ParquetReadOptions::default()).await?;

    // First, let's see what data we have
    println!("📊 Checking available data...");
    let count_df = ctx.sql("SELECT COUNT(*) as total_businesses FROM business").await?;
    let count_results = count_df.collect().await?;
    print_batches(&count_results)?;

    let review_count_df = ctx.sql("SELECT COUNT(*) as total_reviews FROM review").await?;
    let review_count_results = review_count_df.collect().await?;
    print_batches(&review_count_results)?;

    let user_count_df = ctx.sql("SELECT COUNT(*) as total_users FROM user").await?;
    let user_count_results = user_count_df.collect().await?;
    print_batches(&user_count_results)?;

    let sample_df = ctx.sql("SELECT name, city, stars, review_count FROM business LIMIT 5").await?;
    let sample_results = sample_df.collect().await?;
    println!("📊 Sample business data:");
    print_batches(&sample_results)?;

    let city_df = ctx.sql("SELECT city, COUNT(*) as count FROM business GROUP BY city ORDER BY count DESC LIMIT 20").await?;
    let city_results = city_df.collect().await?;
    println!("📊 Top cities:");
    print_batches(&city_results)?;

    // ===== ENHANCED CITY ANALYSIS SECTION =====

    // 1. Top businesses analysis for all cities (not just Philadelphia)
    println!("📊 Analyzing top businesses across all cities...");
    let top_businesses_df = ctx.sql(
        r#"
        SELECT
            b.business_id,
            b.name,
            b.city,
            b.stars as business_stars,
            b.review_count as business_review_count,
            COUNT(r.review_id) AS total_reviews,
            AVG(r.stars) AS avg_review_rating,
            MIN(r.stars) AS min_review_rating,
            MAX(r.stars) AS max_review_rating,
            STDDEV(r.stars) AS rating_stddev,
            CASE 
                WHEN AVG(r.stars) >= 4.5 THEN 'Excellent'
                WHEN AVG(r.stars) >= 4.0 THEN 'Very Good'
                WHEN AVG(r.stars) >= 3.5 THEN 'Good'
                ELSE 'Average'
            END as rating_category,
            CASE 
                WHEN b.categories IS NOT NULL AND b.categories != '' THEN
                    CASE 
                        WHEN POSITION(',' IN b.categories) > 0 THEN
                            TRIM(SUBSTRING(b.categories, 1, POSITION(',' IN b.categories) - 1))
                        ELSE
                            TRIM(b.categories)
                    END
                ELSE 'Unknown'
            END as primary_category
        FROM business b
        JOIN review r ON b.business_id = r.business_id
        WHERE b.stars >= 3.5 AND b.review_count > 10
        GROUP BY b.business_id, b.name, b.city, b.stars, b.review_count, b.categories
        HAVING COUNT(r.review_id) >= 15
        ORDER BY avg_review_rating DESC, total_reviews DESC
        LIMIT 50
        "#,
    ).await?;

    println!("📊 Top businesses across all cities:");
    let top_businesses_results = top_businesses_df.clone().collect().await?;
    print_batches(&top_businesses_results)?;

    // Save top businesses analysis
    top_businesses_df.write_parquet("/app/data/processed/top_businesses_all_cities.parquet", DataFrameWriteOptions::new(), None).await?;

    // 2. Enhanced cross-city comparison analysis
    println!("📊 Performing comprehensive cross-city comparison analysis...");
    let city_comparison_df = ctx.sql(
        r#"
        SELECT
            b.city,
            COUNT(DISTINCT b.business_id) AS total_businesses,
            COUNT(r.review_id) AS total_reviews,
            AVG(b.stars) AS avg_business_rating,
            AVG(r.stars) AS avg_review_rating,
            AVG(b.review_count) AS avg_review_count_per_business,
            STDDEV(b.stars) AS business_rating_stddev,
            STDDEV(r.stars) AS review_rating_stddev,
            COUNT(DISTINCT CASE WHEN b.stars >= 4.5 THEN b.business_id END) AS excellent_businesses,
            COUNT(DISTINCT CASE WHEN b.stars >= 4.0 THEN b.business_id END) AS very_good_businesses,
            COUNT(DISTINCT CASE WHEN b.stars >= 3.5 THEN b.business_id END) AS good_businesses
        FROM business b
        JOIN review r ON b.business_id = r.business_id
        GROUP BY b.city
        HAVING COUNT(r.review_id) >= 100
        ORDER BY total_reviews DESC
        LIMIT 30
        "#,
    ).await?;

    println!("📊 Enhanced cross-city comparison:");
    let city_comparison_results = city_comparison_df.clone().collect().await?;
    print_batches(&city_comparison_results)?;

    // Save enhanced city comparison
    city_comparison_df.write_parquet("/app/data/processed/city_comparison_enhanced.parquet", DataFrameWriteOptions::new(), None).await?;

    // 3. Category analysis across all cities
    println!("📊 Analyzing business categories across all cities...");
    let category_analysis_df = ctx.sql(
        r#"
        SELECT
            b.city,
            -- Extract first category using string functions
            CASE 
                WHEN b.categories IS NOT NULL AND b.categories != '' THEN
                    CASE 
                        WHEN POSITION(',' IN b.categories) > 0 THEN
                            TRIM(SUBSTRING(b.categories, 1, POSITION(',' IN b.categories) - 1))
                        ELSE
                            TRIM(b.categories)
                    END
                ELSE 'Unknown'
            END as primary_category,
            COUNT(DISTINCT b.business_id) AS business_count,
            COUNT(r.review_id) AS total_reviews,
            AVG(b.stars) AS avg_business_rating,
            AVG(r.stars) AS avg_review_rating,
            AVG(b.review_count) AS avg_review_count
        FROM business b
        JOIN review r ON b.business_id = r.business_id
        WHERE b.categories IS NOT NULL AND b.categories != ''
        GROUP BY b.city, 
            CASE 
                WHEN b.categories IS NOT NULL AND b.categories != '' THEN
                    CASE 
                        WHEN POSITION(',' IN b.categories) > 0 THEN
                            TRIM(SUBSTRING(b.categories, 1, POSITION(',' IN b.categories) - 1))
                        ELSE
                            TRIM(b.categories)
                    END
                ELSE 'Unknown'
            END
        HAVING COUNT(r.review_id) >= 20
        ORDER BY total_reviews DESC, avg_review_rating DESC
        LIMIT 100
        "#,
    ).await?;

    println!("📊 Category analysis across cities:");
    let category_analysis_results = category_analysis_df.clone().collect().await?;
    print_batches(&category_analysis_results)?;

    // Save category analysis
    category_analysis_df.write_parquet("/app/data/processed/category_analysis.parquet", DataFrameWriteOptions::new(), None).await?;

    // 4. Top cities by business performance
    println!("📊 Analyzing top performing cities...");
    let top_cities_df = ctx.sql(
        r#"
        SELECT
            b.city,
            COUNT(DISTINCT b.business_id) AS total_businesses,
            COUNT(r.review_id) AS total_reviews,
            AVG(b.stars) AS avg_business_rating,
            AVG(r.stars) AS avg_review_rating,
            COUNT(DISTINCT CASE WHEN b.stars >= 4.5 THEN b.business_id END) AS excellent_count,
            COUNT(DISTINCT CASE WHEN b.stars >= 4.0 THEN b.business_id END) AS very_good_count,
            ROUND(COUNT(DISTINCT CASE WHEN b.stars >= 4.0 THEN b.business_id END) * 100.0 / COUNT(DISTINCT b.business_id), 2) AS high_rated_percentage
        FROM business b
        JOIN review r ON b.business_id = r.business_id
        GROUP BY b.city
        HAVING COUNT(r.review_id) >= 50
        ORDER BY avg_business_rating DESC, total_reviews DESC
        LIMIT 20
        "#,
    ).await?;

    println!("📊 Top performing cities:");
    let top_cities_results = top_cities_df.clone().collect().await?;
    print_batches(&top_cities_results)?;

    // Save top cities analysis
    top_cities_df.write_parquet("/app/data/processed/top_performing_cities.parquet", DataFrameWriteOptions::new(), None).await?;

    // 5. Business performance by rating tiers
    println!("📊 Analyzing business performance by rating tiers...");
    let rating_tiers_df = ctx.sql(
        r#"
        SELECT
            b.city,
            CASE 
                WHEN b.stars >= 4.5 THEN 'Excellent (4.5+)'
                WHEN b.stars >= 4.0 THEN 'Very Good (4.0-4.4)'
                WHEN b.stars >= 3.5 THEN 'Good (3.5-3.9)'
                WHEN b.stars >= 3.0 THEN 'Average (3.0-3.4)'
                ELSE 'Below Average (<3.0)'
            END as rating_tier,
            COUNT(DISTINCT b.business_id) AS business_count,
            COUNT(r.review_id) AS total_reviews,
            AVG(r.stars) AS avg_review_rating,
            AVG(b.review_count) AS avg_review_count
        FROM business b
        JOIN review r ON b.business_id = r.business_id
        GROUP BY b.city, 
            CASE 
                WHEN b.stars >= 4.5 THEN 'Excellent (4.5+)'
                WHEN b.stars >= 4.0 THEN 'Very Good (4.0-4.4)'
                WHEN b.stars >= 3.5 THEN 'Good (3.5-3.9)'
                WHEN b.stars >= 3.0 THEN 'Average (3.0-3.4)'
                ELSE 'Below Average (<3.0)'
            END
        HAVING COUNT(r.review_id) >= 10
        ORDER BY b.city, 
            CASE rating_tier
                WHEN 'Excellent (4.5+)' THEN 1
                WHEN 'Very Good (4.0-4.4)' THEN 2
                WHEN 'Good (3.5-3.9)' THEN 3
                WHEN 'Average (3.0-3.4)' THEN 4
                ELSE 5
            END
        "#,
    ).await?;

    println!("📊 Business performance by rating tiers:");
    let rating_tiers_results = rating_tiers_df.clone().collect().await?;
    print_batches(&rating_tiers_results)?;

    // Save rating tiers analysis
    rating_tiers_df.write_parquet("/app/data/processed/rating_tiers_analysis.parquet", DataFrameWriteOptions::new(), None).await?;

    // ===== ENHANCED USER ANALYSIS SECTION =====

    // 1. User Engagement Analysis - Top reviewers and elite users
    println!("📊 Analyzing user engagement patterns...");
    let user_engagement_df = ctx.sql(
        r#"
        SELECT
            u.user_id,
            u.name,
            u.review_count,
            u.average_stars as avg_rating,
            u.useful as useful_votes,
            u.funny as funny_votes,
            u.cool as cool_votes,
            u.fans,
            CASE 
                WHEN u.elite IS NOT NULL AND u.elite != '' THEN 'Elite'
                ELSE 'Regular'
            END as elite_status,
            CASE 
                WHEN u.review_count >= 1000 THEN 'Power User'
                WHEN u.review_count >= 500 THEN 'Active User'
                WHEN u.review_count >= 100 THEN 'Regular User'
                ELSE 'Occasional User'
            END as user_category
        FROM user u
        WHERE u.review_count > 50
        ORDER BY u.review_count DESC, u.average_stars DESC
        LIMIT 50
        "#,
    ).await?;

    println!("📊 Top user engagement analysis:");
    let user_engagement_results = user_engagement_df.clone().collect().await?;
    print_batches(&user_engagement_results)?;

    // Save user engagement analysis
    user_engagement_df.write_parquet("/app/data/processed/user_engagement.parquet", DataFrameWriteOptions::new(), None).await?;

    // 2. User-Business Interaction Analysis
    println!("📊 Analyzing user-business interactions...");
    let user_business_df = ctx.sql(
        r#"
        SELECT
            u.user_id,
            u.name as user_name,
            u.average_stars as user_avg_rating,
            COUNT(r.review_id) as total_reviews_written,
            COUNT(DISTINCT r.business_id) as unique_businesses_reviewed,
            AVG(r.stars) as avg_review_rating_given,
            AVG(b.stars) as avg_business_rating_reviewed,
            COUNT(DISTINCT b.city) as cities_reviewed,
            MIN(r.date) as first_review_date,
            MAX(r.date) as last_review_date,
            COUNT(r.review_id) as interaction_count,
            AVG(r.stars) as avg_rating,
            CASE 
                WHEN COUNT(r.review_id) >= 100 THEN 'High Activity'
                WHEN COUNT(r.review_id) >= 50 THEN 'Medium Activity'
                ELSE 'Low Activity'
            END as interaction_type
        FROM user u
        JOIN review r ON u.user_id = r.user_id
        JOIN business b ON r.business_id = b.business_id
        WHERE u.review_count > 20
        GROUP BY u.user_id, u.name, u.average_stars
        ORDER BY total_reviews_written DESC, avg_review_rating_given DESC
        LIMIT 30
        "#,
    ).await?;

    println!("📊 User-business interaction analysis:");
    let user_business_results = user_business_df.clone().collect().await?;
    print_batches(&user_business_results)?;

    // Save user-business interaction analysis
    user_business_df.write_parquet("/app/data/processed/user_business_interactions.parquet", DataFrameWriteOptions::new(), None).await?;

    // 3. User Sentiment Analysis
    println!("📊 Analyzing user sentiment patterns...");
    let user_sentiment_df = ctx.sql(
        r#"
        SELECT
            u.user_id,
            u.name,
            u.average_stars as user_avg_rating,
            COUNT(r.review_id) as total_reviews,
            AVG(r.stars) as avg_rating_given,
            STDDEV(r.stars) as rating_consistency,
            COUNT(CASE WHEN r.stars >= 4 THEN 1 END) as positive_reviews,
            COUNT(CASE WHEN r.stars <= 2 THEN 1 END) as negative_reviews,
            COUNT(CASE WHEN r.stars = 3 THEN 1 END) as neutral_reviews,
            ROUND(COUNT(CASE WHEN r.stars >= 4 THEN 1 END) * 100.0 / COUNT(r.review_id), 2) as positive_review_percentage,
            ROUND(COUNT(CASE WHEN r.stars <= 2 THEN 1 END) * 100.0 / COUNT(r.review_id), 2) as negative_review_percentage,
            CASE 
                WHEN AVG(r.stars) >= 4.0 THEN 'Positive'
                WHEN AVG(r.stars) <= 2.0 THEN 'Negative'
                ELSE 'Neutral'
            END as sentiment
        FROM user u
        JOIN review r ON u.user_id = r.user_id
        WHERE u.review_count > 30
        GROUP BY u.user_id, u.name, u.average_stars
        ORDER BY total_reviews DESC, avg_rating_given DESC
        LIMIT 25
        "#,
    ).await?;

    println!("📊 User sentiment analysis:");
    let user_sentiment_results = user_sentiment_df.clone().collect().await?;
    print_batches(&user_sentiment_results)?;

    // Save user sentiment analysis
    user_sentiment_df.write_parquet("/app/data/processed/user_sentiment.parquet", DataFrameWriteOptions::new(), None).await?;

    // 4. User Compliment Analysis
    println!("📊 Analyzing user compliment patterns...");
    let user_compliment_df = ctx.sql(
        r#"
        SELECT
            u.user_id,
            u.name as user_name,
            u.review_count,
            u.average_stars,
            u.compliment_hot,
            u.compliment_more,
            u.compliment_profile,
            u.compliment_cute,
            u.compliment_list,
            u.compliment_note,
            u.compliment_plain,
            u.compliment_cool,
            u.compliment_funny,
            u.compliment_writer,
            u.compliment_photos,
            (u.compliment_hot + u.compliment_more + u.compliment_profile + u.compliment_cute + 
             u.compliment_list + u.compliment_note + u.compliment_plain + u.compliment_cool + 
             u.compliment_funny + u.compliment_writer + u.compliment_photos) as total_compliments,
            CASE 
                WHEN u.compliment_hot > 0 THEN 'Hot'
                WHEN u.compliment_cool > 0 THEN 'Cool'
                WHEN u.compliment_funny > 0 THEN 'Funny'
                WHEN u.compliment_writer > 0 THEN 'Writer'
                WHEN u.compliment_photos > 0 THEN 'Photos'
                ELSE 'Other'
            END as primary_compliment_type,
            CASE 
                WHEN u.compliment_hot > 0 THEN 'Hot'
                WHEN u.compliment_cool > 0 THEN 'Cool'
                WHEN u.compliment_funny > 0 THEN 'Funny'
                WHEN u.compliment_writer > 0 THEN 'Writer'
                WHEN u.compliment_photos > 0 THEN 'Photos'
                ELSE 'Other'
            END as compliment_type,
            (u.compliment_hot + u.compliment_more + u.compliment_profile + u.compliment_cute + 
             u.compliment_list + u.compliment_note + u.compliment_plain + u.compliment_cool + 
             u.compliment_funny + u.compliment_writer + u.compliment_photos) as compliment_count
        FROM user u
        WHERE (u.compliment_hot + u.compliment_more + u.compliment_profile + u.compliment_cute + 
               u.compliment_list + u.compliment_note + u.compliment_plain + u.compliment_cool + 
               u.compliment_funny + u.compliment_writer + u.compliment_photos) > 0
        ORDER BY total_compliments DESC, u.review_count DESC
        LIMIT 30
        "#,
    ).await?;

    println!("📊 User compliment analysis:");
    let user_compliment_results = user_compliment_df.clone().collect().await?;
    print_batches(&user_compliment_results)?;

    // Save user compliment analysis
    user_compliment_df.write_parquet("/app/data/processed/user_compliments.parquet", DataFrameWriteOptions::new(), None).await?;

    // 5. User Activity Timeline Analysis
    println!("📊 Analyzing user activity timeline patterns...");
    let user_timeline_df = ctx.sql(
        r#"
        SELECT
            u.user_id,
            u.name,
            u.yelping_since,
            u.review_count as user_total_reviews,
            u.average_stars,
            COUNT(r.review_id) as reviews_in_dataset,
            MIN(r.date) as earliest_review_date,
            MAX(r.date) as latest_review_date,
            ROUND(AVG(r.stars), 2) as avg_rating_in_dataset,
            COUNT(DISTINCT EXTRACT(YEAR FROM r.date)) as active_years,
            MIN(r.date) as activity_date,
            COUNT(r.review_id) as review_count,
            AVG(r.stars) as avg_rating,
            1 as user_count
        FROM user u
        JOIN review r ON u.user_id = r.user_id
        WHERE u.review_count > 10
        GROUP BY u.user_id, u.name, u.yelping_since, u.review_count, u.average_stars
        ORDER BY reviews_in_dataset DESC, avg_rating_in_dataset DESC
        LIMIT 25
        "#,
    ).await?;

    println!("📊 User activity timeline analysis:");
    let user_timeline_results = user_timeline_df.clone().collect().await?;
    print_batches(&user_timeline_results)?;

    // Save user timeline analysis
    user_timeline_df.write_parquet("/app/data/processed/user_activity_timeline.parquet", DataFrameWriteOptions::new(), None).await?;

    // 6. Elite User Analysis
    println!("📊 Analyzing elite user characteristics...");
    let elite_user_df = ctx.sql(
        r#"
        SELECT
            u.user_id,
            u.name,
            u.review_count,
            u.average_stars as avg_rating,
            u.useful as useful_votes,
            u.funny as funny_votes,
            u.cool as cool_votes,
            u.fans,
            u.elite,
            COUNT(r.review_id) as reviews_in_dataset,
            AVG(r.stars) as avg_rating_given,
            COUNT(DISTINCT b.city) as cities_reviewed,
            COUNT(DISTINCT b.business_id) as businesses_reviewed
        FROM user u
        LEFT JOIN review r ON u.user_id = r.user_id
        LEFT JOIN business b ON r.business_id = b.business_id
        WHERE u.elite IS NOT NULL AND u.elite != ''
        GROUP BY u.user_id, u.name, u.review_count, u.average_stars, u.useful, u.funny, u.cool, u.fans, u.elite
        ORDER BY u.review_count DESC, u.average_stars DESC
        LIMIT 20
        "#,
    ).await?;

    println!("📊 Elite user analysis:");
    let elite_user_results = elite_user_df.clone().collect().await?;
    print_batches(&elite_user_results)?;

    // Save elite user analysis
    elite_user_df.write_parquet("/app/data/processed/elite_users.parquet", DataFrameWriteOptions::new(), None).await?;

    println!("✅ All enhanced analyses completed and data saved successfully!");

    Ok(())
}

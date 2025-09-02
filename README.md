# Modern Multimodal Data Processing: Formula 1 Analytics Pipeline

*A comprehensive guide to querying multimodal F1 data using Parquet, DuckDB, LanceDB, Daft, and Databricks*

## Table of Contents
- [Introduction](#introduction)
- [Understanding Data Evolution](#understanding-data-evolution)
- [Parquet Files: The Foundation](#parquet-files-the-foundation)
- [DuckDB: Analytics Made Simple](#duckdb-analytics-made-simple)
- [LanceDB: Vector Search and Multimodal Data](#lancedb-vector-search-and-multimodal-data)
- [Daft: Multimodal Data Processing](#daft-multimodal-data-processing)
- [Databricks: Enterprise Scale](#databricks-enterprise-scale)
- [Building a Complete Pipeline](#building-a-complete-pipeline)
- [Performance Optimization](#performance-optimization)
- [Real-World Use Cases](#real-world-use-cases)

## Introduction

The data landscape has evolved dramatically, and Formula 1 provides a perfect example of this evolution. We've moved from simple race results in CSV files to sophisticated multimodal data processing pipelines that handle telemetry data, driver photos, circuit layouts, team videos, and real-time analytics. This tutorial explores the modern data stack using the **Formula 1 Grand Prix Winners Dataset (1950–2025)** from Kaggle.

### What You'll Learn
- How to work with F1 race data in Parquet files for efficient storage
- Using DuckDB for fast analytical queries on historical race results
- Implementing vector search with LanceDB for driver and team similarity
- Processing multimodal F1 data (images, videos, telemetry) with Daft
- Scaling F1 analytics with Databricks
- Building production-ready motorsports data pipelines

## Understanding Data Evolution

### Traditional Tabular Workloads
Traditional F1 analytics focused on structured race results:
```sql
-- Race winner analysis example
SELECT 
    d.forename || ' ' || d.surname as driver_name,
    c.name as constructor,
    COUNT(*) as wins
FROM results r
JOIN drivers d ON r.driverId = d.driverId
JOIN constructors c ON r.constructorId = c.constructorId
WHERE r.positionOrder = 1 
    AND r.raceId IN (
        SELECT raceId FROM races WHERE year >= 2020
    )
GROUP BY d.driverId, c.constructorId
ORDER BY wins DESC;
```

### Modern Multimodal F1 Workloads
Today's F1 applications require processing diverse data types:
- Circuit layout images and aerial photos for track analysis
- Driver helmet cam videos for performance analysis
- Team radio audio for sentiment analysis
- Car telemetry data for performance optimization
- Fan social media content for engagement metrics

## Parquet Files: The Foundation

Parquet has become essential for F1 analytics, replacing CSV for historical race data storage:

### Why Parquet Over CSV for F1 Data?

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# CSV limitations with F1 data
f1_results_csv = pd.read_csv('f1_race_results.csv')  # Slow, no schema, large size

# Parquet benefits for F1 analytics
f1_results_parquet = pd.read_parquet('f1_race_results.parquet')  # Fast, typed, compressed
```

### Key Advantages for F1 Data:
1. **Columnar Storage**: Better compression for lap times and telemetry
2. **Schema Evolution**: Add new columns as F1 regulations change
3. **Predicate Pushdown**: Skip irrelevant seasons/circuits
4. **Compression**: Handle massive telemetry datasets efficiently

### Working with F1 Parquet Data

```python
# Creating F1 Parquet files from Kaggle dataset
f1_data = {
    'race_id': range(1, 1001),
    'driver_name': [f'Driver_{i}' for i in range(1, 1001)],
    'constructor': ['Mercedes', 'Ferrari', 'Red Bull'] * 334,
    'circuit_name': ['Monaco', 'Silverstone', 'Monza'] * 334,
    'position': [1, 2, 3] * 334,
    'points': [25, 18, 15] * 334,
    'fastest_lap_time': ['1:23.456', '1:24.123', '1:24.789'] * 334,
    'season': [2020 + (i // 50) for i in range(1000)]
}

df = pd.DataFrame(f1_data)
df.to_parquet('f1_race_results.parquet')

# Reading with F1-specific filters
recent_winners = pd.read_parquet(
    'f1_race_results.parquet',
    filters=[('season', '>=', 2020), ('position', '==', 1)]
)
```

## DuckDB: Analytics Made Simple

DuckDB excels at processing F1 race data with complex analytical queries.

### Getting Started with F1 Analytics

```python
import duckdb

# Connect to DuckDB for F1 analytics
conn = duckdb.connect('f1_analytics.duckdb')

# Query F1 Parquet files directly
race_analysis = conn.execute("""
    SELECT 
        circuit_name,
        constructor,
        COUNT(*) as wins,
        AVG(points) as avg_points,
        MIN(fastest_lap_time) as best_lap
    FROM 'f1_race_results.parquet'
    WHERE position = 1 AND season >= 2020
    GROUP BY circuit_name, constructor
    ORDER BY wins DESC
""").fetchall()

print("Top performing constructor-circuit combinations:")
for row in race_analysis[:5]:
    print(f"{row[1]} at {row[0]}: {row[2]} wins, avg {row[3]:.1f} points")
```

### Advanced F1 Analytics Features

#### Object Storage for Season Data
```sql
-- Query historical F1 data from S3
SELECT 
    season,
    COUNT(DISTINCT driver_name) as unique_drivers,
    AVG(points) as avg_points_per_race
FROM 's3://f1-data-lake/seasons/*.parquet'
WHERE season BETWEEN 1950 AND 2025
GROUP BY season
ORDER BY season;
```

#### Apache Iceberg for F1 Transactions
```python
# Iceberg table operations for live F1 data
conn.execute("""
    CREATE TABLE f1_live_results AS
    SELECT * FROM iceberg_scan('s3://f1-live/race-results/')
    WHERE race_date >= CURRENT_DATE - INTERVAL 30 DAYS
""")

# Update with latest race results
conn.execute("""
    INSERT INTO f1_live_results 
    SELECT * FROM 's3://f1-live/latest-race/*.parquet'
""")
```

### Architecture Benefits for F1 Analytics

DuckDB's performance characteristics make it ideal for F1 data:
- **Fast execution**: Process millions of lap times efficiently
- **Memory efficiency**: Handle large telemetry datasets
- **Easy deployment**: Perfect for F1 team laptops and edge computing
- **ACID transactions**: Reliable updates during live races

## LanceDB: Vector Search and Multimodal Data

LanceDB enables similarity search across F1's multimodal content - from driver photos to circuit layouts.

### Installation and F1 Data Setup

```python
import lancedb
import numpy as np

# Create F1-focused LanceDB
db = lancedb.connect("./f1-vector-db")

# Sample F1 multimodal data with embeddings
f1_multimodal_data = [
    {
        "id": 1,
        "content_type": "driver_photo",
        "filename": "hamilton_portrait.jpg",
        "driver_name": "Lewis Hamilton",
        "constructor": "Mercedes",
        "season": 2024,
        "image_embedding": np.random.random(768).tolist(),
        "metadata": {
            "category": "driver", 
            "championships": 7,
            "nationality": "British"
        }
    },
    {
        "id": 2,
        "content_type": "circuit_layout", 
        "filename": "monaco_circuit.jpg",
        "circuit_name": "Circuit de Monaco",
        "country": "Monaco",
        "image_embedding": np.random.random(768).tolist(),
        "metadata": {
            "category": "circuit",
            "length_km": 3.337,
            "corners": 19,
            "difficulty": "high"
        }
    },
    {
        "id": 3,
        "content_type": "team_car",
        "filename": "ferrari_sf24.jpg", 
        "constructor": "Ferrari",
        "season": 2024,
        "image_embedding": np.random.random(768).tolist(),
        "metadata": {
            "category": "car",
            "engine": "Ferrari",
            "chassis": "SF-24"
        }
    }
]

# Create multimodal F1 table
f1_table = db.create_table("f1_multimodal", f1_multimodal_data)
```

### F1 Vector Search Operations

```python
# Find similar drivers by photo
hamilton_query = np.random.random(768).tolist()

similar_drivers = f1_table.search(hamilton_query)\
    .where("content_type = 'driver_photo'")\
    .limit(5)\
    .to_list()

print("Drivers similar to Hamilton (by photo):")
for driver in similar_drivers:
    print(f"- {driver['driver_name']} ({driver['constructor']})")

# Find circuits similar to Monaco
monaco_query = np.random.random(768).tolist()

similar_circuits = f1_table.search(monaco_query)\
    .where("content_type = 'circuit_layout'")\
    .where("metadata.difficulty = 'high'")\
    .limit(3)\
    .to_list()

print("\nCircuits similar to Monaco:")
for circuit in similar_circuits:
    print(f"- {circuit['circuit_name']} ({circuit['metadata']['corners']} corners)")
```

### F1 Use Cases for Vector Search
- **Driver similarity**: Find drivers with similar racing styles
- **Circuit analysis**: Compare track layouts and characteristics
- **Car design**: Identify similar aerodynamic packages
- **Performance clustering**: Group similar race performances
- **Fan engagement**: Content-based recommendations

## Daft: Multimodal Data Processing

Daft serves as the compute layer for processing F1's diverse data types.

### Installation and F1 Setup

```bash
pip install getdaft
```

### Basic F1 Operations

```python
import daft

# Read F1 race results
f1_df = daft.read_parquet("f1_race_results.parquet")

# Process driver helmet images
f1_with_images = f1_df.with_column(
    "driver_image_data", 
    daft.col("driver_photo_path").apply(
        lambda x: load_f1_image(x), 
        return_dtype=daft.DataType.python()
    )
)

# Extract visual features from driver photos
f1_processed = f1_with_images.with_column(
    "driver_visual_features",
    daft.col("driver_image_data").apply(
        extract_driver_features, 
        return_dtype=daft.DataType.tensor(daft.DataType.float32())
    )
)

# Show processed data
f1_processed.select(["driver_name", "constructor", "driver_visual_features"]).show()
```

### Advanced F1 Multimodal Operations

```python
# F1 video processing pipeline for race highlights
f1_videos = daft.read_parquet("f1_race_videos.parquet")

# Extract key moments from race videos
race_moments = f1_videos.with_column(
    "key_frames",
    daft.col("race_video_path").apply(extract_race_highlights)
)

# Generate embeddings for race moments
race_embeddings = race_moments.with_column(
    "moment_embeddings", 
    daft.col("key_frames").apply(generate_f1_video_embeddings)
)

# Analyze overtaking maneuvers
overtaking_analysis = race_embeddings.with_column(
    "overtaking_score",
    daft.col("moment_embeddings").apply(detect_overtaking_moves)
).where(daft.col("overtaking_score") > 0.8)

# Save to LanceDB for similarity search
overtaking_analysis.write_lance("f1_overtaking_moments")
```

### F1 Telemetry Processing

```python
# Process real-time F1 telemetry data
telemetry_df = daft.read_parquet("f1_telemetry_*.parquet")

# Calculate performance metrics
performance_df = telemetry_df.with_columns([
    daft.col("speed_kph").apply(calculate_speed_zones).alias("speed_zone"),
    daft.col("throttle_percent").apply(analyze_throttle_usage).alias("throttle_efficiency"),
    daft.col("brake_pressure").apply(detect_braking_points).alias("braking_analysis")
])

# Aggregate by driver and circuit
driver_performance = performance_df.groupby(["driver_id", "circuit_id"]).agg([
    daft.col("speed_zone").mean().alias("avg_speed_zone"),
    daft.col("throttle_efficiency").mean().alias("avg_throttle_eff"),
    daft.col("lap_time_ms").min().alias("best_lap_time")
])
```

### Key Daft Features for F1 Data
- **Lazy evaluation**: Efficient processing of season-long datasets
- **Multimodal support**: Native handling of videos, telemetry, images
- **Distributed compute**: Scale across F1 team's compute clusters
- **Integration**: Seamless workflow with other F1 analytics tools

## Databricks: Enterprise Scale

Databricks provides the enterprise platform for F1 teams and organizations processing large-scale racing data.

### ACID Transactions for F1 Data

Databricks implements ACID properties for reliable F1 data operations:

```sql
-- Atomic F1 race result updates
BEGIN TRANSACTION;
    INSERT INTO race_results 
    SELECT * FROM latest_race_data 
    WHERE race_id = (SELECT MAX(race_id) FROM races);
    
    UPDATE driver_standings 
    SET points = points + new_race_points,
        position = calculate_new_position(points + new_race_points)
    WHERE season = 2025;
    
    UPDATE constructor_standings
    SET points = (SELECT SUM(points) FROM driver_standings 
                  WHERE constructor_id = constructor_standings.constructor_id);
COMMIT;
```

### Delta Lake Integration for F1 Analytics

```python
from delta import *
from pyspark.sql import SparkSession

# Configure Spark for F1 Delta Lake
spark = configure_spark_with_delta_pip(SparkSession.builder).getOrCreate()

# Read F1 race results Delta table
f1_results = spark.read.format("delta").load("/f1-data-lake/race-results")

# Time travel to compare seasons
f1_2023 = spark.read.format("delta")\
    .option("timestampAsOf", "2023-12-31")\
    .load("/f1-data-lake/race-results")

f1_2024 = spark.read.format("delta")\
    .option("timestampAsOf", "2024-12-31")\
    .load("/f1-data-lake/race-results")

# Compare constructor performance across seasons
season_comparison = f1_2024.join(f1_2023, ["constructor_id"], "outer")\
    .select(
        "constructor_name",
        (f1_2024.points - f1_2023.points).alias("points_improvement")
    ).orderBy("points_improvement", ascending=False)
```

### Advanced F1 Features

#### Composable Table Formats for Racing Data
Modern table formats provide essential capabilities for F1:
- **Schema evolution**: Add new telemetry sensors safely
- **Time travel**: Analyze historical performance trends
- **ACID transactions**: Reliable updates during live races
- **File skipping**: Query only relevant races/seasons

#### Performance Optimizations for F1 Queries
```sql
-- Z-ordering for better F1 query performance
OPTIMIZE f1_race_results ZORDER BY (season, circuit_id, driver_id);

-- Optimize for common F1 analytics patterns
OPTIMIZE f1_lap_times ZORDER BY (race_id, driver_id, lap_number);

-- Vacuum old race data
VACUUM f1_race_results RETAIN 168 HOURS;
```

## Building a Complete F1 Pipeline

Let's build an end-to-end multimodal F1 data pipeline:

### Step 1: F1 Data Ingestion

```python
import daft
import lancedb
import duckdb

# Ingest multimodal F1 data
def ingest_f1_data(kaggle_f1_path):
    # Read F1 race results from Kaggle dataset
    races_df = daft.read_csv(f"{kaggle_f1_path}/races.csv")
    results_df = daft.read_csv(f"{kaggle_f1_path}/results.csv")
    drivers_df = daft.read_csv(f"{kaggle_f1_path}/drivers.csv")
    constructors_df = daft.read_csv(f"{kaggle_f1_path}/constructors.csv")
    circuits_df = daft.read_csv(f"{kaggle_f1_path}/circuits.csv")
    
    # Join for comprehensive race data
    f1_complete = results_df\
        .join(races_df, on="raceId")\
        .join(drivers_df, on="driverId")\
        .join(constructors_df, on="constructorId")\
        .join(circuits_df, on="circuitId")
    
    # Process driver photos (if available)
    f1_with_driver_embeddings = f1_complete.with_column(
        "driver_photo_embedding",
        daft.col("driver_photo_path").apply(generate_driver_photo_embedding)
    )
    
    # Process circuit layout images
    f1_with_circuit_embeddings = f1_with_driver_embeddings.with_column(
        "circuit_layout_embedding", 
        daft.col("circuit_image_path").apply(generate_circuit_embedding)
    )
    
    return f1_with_circuit_embeddings

# Process the F1 data
processed_f1_df = ingest_f1_data("kaggle_f1_dataset/")
```

### Step 2: F1 Storage and Indexing

```python
# Save F1 analytics data to Parquet
processed_f1_df.write_parquet("f1_complete_dataset.parquet")

# Index F1 vectors in LanceDB for similarity search
db = lancedb.connect("./f1_vector_db")

# Create driver similarity index
driver_data = processed_f1_df.select([
    "driverId", "driver_name", "nationality", "constructor_name",
    "driver_photo_embedding", "total_points", "wins"
]).to_pyarrow_table()

driver_table = db.create_table("f1_drivers", driver_data)

# Create circuit similarity index  
circuit_data = processed_f1_df.select([
    "circuitId", "circuit_name", "country", "circuit_layout_embedding",
    "length", "corners", "avg_lap_time"
]).distinct().to_pyarrow_table()

circuit_table = db.create_table("f1_circuits", circuit_data)
```

### Step 3: F1 Analytics with DuckDB

```python
conn = duckdb.connect("f1_analytics.duckdb")

# Championship analysis
championship_analysis = conn.execute("""
    WITH driver_seasons AS (
        SELECT 
            driver_name,
            constructor_name,
            season,
            SUM(points) as season_points,
            COUNT(*) as races_entered,
            SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) as wins
        FROM 'f1_complete_dataset.parquet'
        WHERE season >= 2020
        GROUP BY driver_name, constructor_name, season
    ),
    season_rankings AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY season ORDER BY season_points DESC) as championship_position
        FROM driver_seasons
    )
    SELECT 
        driver_name,
        constructor_name,
        COUNT(*) as seasons_competed,
        SUM(wins) as total_wins,
        AVG(season_points) as avg_season_points,
        COUNT(CASE WHEN championship_position = 1 THEN 1 END) as championships_won
    FROM season_rankings
    GROUP BY driver_name, constructor_name
    HAVING seasons_competed >= 2
    ORDER BY championships_won DESC, total_wins DESC
""").fetchall()

print("F1 Driver Championship Analysis (2020-2025):")
for row in championship_analysis[:10]:
    print(f"{row[0]} ({row[1]}): {row[5]} championships, {row[3]} wins")
```

### Step 4: F1 Vector Search and Recommendations

```python
# Find drivers similar to Lewis Hamilton
def find_similar_drivers(driver_name, limit=5):
    # Get Hamilton's embedding
    hamilton_data = driver_table.search("Lewis Hamilton")\
        .where("driver_name = 'Lewis Hamilton'")\
        .limit(1)\
        .to_pandas()
    
    if len(hamilton_data) > 0:
        hamilton_embedding = hamilton_data.iloc[0]['driver_photo_embedding']
        
        similar_drivers = driver_table.search(hamilton_embedding)\
            .where("driver_name != 'Lewis Hamilton'")\
            .limit(limit)\
            .to_pandas()
        
        return similar_drivers[['driver_name', 'constructor_name', 'nationality', 'wins']]
    
    return None

# Find circuits similar to Monaco
def find_similar_circuits(circuit_name="Circuit de Monaco", limit=5):
    circuit_data = circuit_table.search("Circuit de Monaco")\
        .where(f"circuit_name = '{circuit_name}'")\
        .limit(1)\
        .to_pandas()
    
    if len(circuit_data) > 0:
        circuit_embedding = circuit_data.iloc[0]['circuit_layout_embedding']
        
        similar_circuits = circuit_table.search(circuit_embedding)\
            .where(f"circuit_name != '{circuit_name}'")\
            .limit(limit)\
            .to_pandas()
        
        return similar_circuits[['circuit_name', 'country', 'length', 'corners']]
    
    return None

# Example usage
print("Drivers similar to Lewis Hamilton:")
similar_drivers = find_similar_drivers("Lewis Hamilton")
if similar_drivers is not None:
    print(similar_drivers)

print("\nCircuits similar to Monaco:")
similar_circuits = find_similar_circuits()
if similar_circuits is not None:
    print(similar_circuits)
```

## Performance Optimization

### File Skipping Strategies for F1 Data

1. **Partition by season**: Organize F1 data by frequently queried time periods
2. **Use appropriate file sizes**: 100MB-1GB per season/circuit combination  
3. **Implement predicate pushdown**: Filter at the storage level for specific seasons

```python
# Efficient F1 data filtering
recent_f1_seasons = daft.read_parquet("f1_historical_data.parquet")\
    .where(daft.col("season") >= 2020)\
    .where(daft.col("constructor_name").is_in(["Mercedes", "Ferrari", "Red Bull"]))

# Lazy evaluation for championship calculations
championship_df = recent_f1_seasons.select([
    "driver_name", "constructor_name", "season", "points"
]).with_column(
    "championship_potential", 
    calculate_championship_probability(daft.col("points"))
)
```

### Query Optimization for F1 Analytics

```python
# Memory-efficient F1 lap time analysis
def analyze_f1_lap_times(season_data_path, batch_size=1000):
    """Process F1 lap times in memory-efficient batches"""
    lap_times_df = daft.read_parquet(season_data_path)
    
    for batch in lap_times_df.iter_partitions(batch_size):
        # Calculate sector times and performance metrics
        processed_batch = batch.with_columns([
            daft.col("sector1_time").apply(analyze_sector_performance).alias("s1_analysis"),
            daft.col("sector2_time").apply(analyze_sector_performance).alias("s2_analysis"), 
            daft.col("sector3_time").apply(analyze_sector_performance).alias("s3_analysis")
        ])
        
        yield processed_batch

# Circuit-specific performance analysis
def analyze_circuit_performance(circuit_name):
    return daft.read_parquet("f1_complete_dataset.parquet")\
        .where(daft.col("circuit_name") == circuit_name)\
        .groupby(["driver_name", "constructor_name"])\
        .agg([
            daft.col("fastest_lap_time").min().alias("best_lap"),
            daft.col("points").sum().alias("total_points"),
            daft.col("position").mean().alias("avg_finish_position")
        ])\
        .orderBy("total_points", desc=True)
```

## Real-World F1 Use Cases

### 1. F1 Performance Prediction System

```python
# Build F1 race performance predictor
def predict_f1_race_performance(driver_id, circuit_id, season=2025):
    # Get driver's historical performance at this circuit
    driver_history = conn.execute("""
        SELECT 
            AVG(points) as avg_points,
            AVG(position) as avg_position,
            MIN(fastest_lap_time) as personal_best,
            COUNT(*) as races_at_circuit
        FROM f1_complete_dataset
        WHERE driver_id = ? AND circuit_id = ?
        AND season >= 2020
    """, [driver_id, circuit_id]).fetchone()
    
    # Get circuit-specific performance patterns
    circuit_patterns = conn.execute("""
        SELECT 
            constructor_name,
            AVG(points) as constructor_avg_points,
            COUNT(CASE WHEN position = 1 THEN 1 END) as wins
        FROM f1_complete_dataset  
        WHERE circuit_id = ? AND season >= 2020
        GROUP BY constructor_name
        ORDER BY constructor_avg_points DESC
    """, [circuit_id]).fetchall()
    
    # Use vector similarity for style-based predictions
    driver_embedding = get_driver_performance_embedding(driver_id)
    similar_performances = driver_table.search(driver_embedding)\
        .where(f"circuit_id = {circuit_id}")\
        .limit(10)\
        .to_list()
    
    return {
        'predicted_points': calculate_prediction(driver_history, circuit_patterns, similar_performances),
        'confidence': calculate_confidence_score(driver_history[3]),  # races_at_circuit
        'similar_drivers': [p['driver_name'] for p in similar_performances[:3]]
    }
```

### 2. Multimodal F1 Content Search Engine

```python
def search_f1_content(query_text=None, query_image=None, content_type=None):
    """Search across all F1 multimodal content"""
    results = []
    
    if query_text:
        # Text-based search (e.g., "Monaco qualifying lap records")
        text_embedding = generate_text_embedding(query_text)
        text_results = f1_table.search(text_embedding, vector_column="text_embedding")\
            .limit(10)
        
        if content_type:
            text_results = text_results.where(f"content_type = '{content_type}'")
            
        results.extend(text_results.to_list())
    
    if query_image:
        # Image-based search (e.g., upload circuit photo to find similar tracks)
        image_embedding = generate_image_embedding(query_image) 
        image_results = f1_table.search(image_embedding, vector_column="image_embedding")\
            .limit(10)
            
        if content_type:
            image_results = image_results.where(f"content_type = '{content_type}'")
            
        results.extend(image_results.to_list())
    
    return rank_f1_results(results)

# Example usage
monaco_results = search_f1_content(
    query_text="street circuit with tight corners and elevation changes",
    content_type="circuit_layout"
)

print("Circuits similar to Monaco:")
for result in monaco_results[:5]:
    print(f"- {result['circuit_name']}: {result['metadata']['corners']} corners")
```

### 3. F1 Dataset Preparation for Machine Learning

```python
def prepare_f1_ml_dataset(source_data_path):
    """Prepare F1 data for machine learning models"""
    # Process with Daft
    f1_df = daft.read_parquet(source_data_path)
    
    # Clean and filter for quality F1 data
    f1_clean = f1_df.where(daft.col("status") == "Finished")\
        .where(daft.col("laps_completed") >= 0.9 * daft.col("total_laps"))\
        .where(daft.col("season") >= 2010)  # Modern F1 era
    
    # Feature engineering for F1 performance prediction
    f1_features = f1_clean.with_columns([
        # Driver experience features
        daft.col("races_entered").apply(calculate_experience_score).alias("experience_score"),
        
        # Circuit familiarity
        daft.col("previous_races_at_circuit").apply(calculate_familiarity).alias("circuit_familiarity"),
        
        # Constructor performance features  
        daft.col("constructor_season_points").apply(normalize_constructor_strength).alias("constructor_strength"),
        
        # Weather and track condition embeddings
        daft.col("race_conditions").apply(encode_race_conditions).alias("conditions_embedding")
    ])
    
    # Generate embeddings for similarity-based features
    f1_embedded = f1_features.with_column(
        "performance_embedding",
        daft.col("lap_times_sequence").apply(generate_performance_embedding)
    )
    
    # Deduplicate similar race scenarios using vector similarity
    deduplicated_f1 = deduplicate_by_race_similarity(f1_embedded)
    
    # Save in format suitable for ML training
    deduplicated_f1.write_parquet("f1_ml_training_data.parquet")
    
    return deduplicated_f1
```

### 4. Real-Time F1 Race Analytics

```python
def real_time_f1_analytics():
    """Process live F1 race data during Grand Prix weekends"""
    
    # Stream live telemetry data
    live_telemetry = daft.read_streaming("f1_live_telemetry")\
        .with_column(
            "performance_metrics",
            daft.col("telemetry_data").apply(calculate_real_time_performance)
        )
    
    # Update driver positions and gap analysis
    position_updates = live_telemetry.groupby(["driver_id", "lap_number"])\
        .agg([
            daft.col("lap_time").min().alias("lap_time"),
            daft.col("sector_times").apply(lambda x: x).alias("sector_analysis"),
            daft.col("tire_compound").apply(lambda x: x).alias("tire_strategy")
        ])
    
    # Push updates to live dashboard
    position_updates.write_streaming("f1_live_dashboard")
    
    # Store in vector database for post-race analysis
    position_updates.write_lance("f1_race_moments")

# Strategy optimization during races
def optimize_f1_strategy(driver_id, current_lap, weather_conditions):
    """Real-time F1 strategy optimization"""
    
    # Get similar race scenarios from vector database
    current_conditions_embedding = encode_race_state(
        driver_id, current_lap, weather_conditions
    )
    
    similar_scenarios = f1_table.search(current_conditions_embedding)\
        .where("content_type = 'race_scenario'")\
        .limit(20)\
        .to_list()
    
    # Analyze optimal strategies from similar situations
    strategy_recommendations = []
    for scenario in similar_scenarios:
        if scenario['metadata']['outcome_success_rate'] > 0.7:
            strategy_recommendations.append({
                'strategy': scenario['metadata']['strategy_used'],
                'success_rate': scenario['metadata']['outcome_success_rate'],
                'reference_race': scenario['metadata']['race_reference']
            })
    
    return sorted(strategy_recommendations, key=lambda x: x['success_rate'], reverse=True)
```

## Best Practices for F1 Data

### F1 Data Architecture
1. **Separate storage and compute**: Use object storage for historical F1 data, compute engines for race analysis
2. **Implement proper data governance**: Track lineage of race results, driver data quality, and access controls for team data
3. **Version your F1 data**: Use tools like DVC or Delta Lake for versioning race results and regulation changes
4. **Monitor performance**: Track query performance during live races and resource usage

### F1 Code Organization
```python
# F1 analytics project structure
f1_multimodal_pipeline/
├── data_ingestion/
│   ├── kaggle_parsers.py      # Parse Kaggle F1 dataset
│   ├── live_data_parsers.py   # Parse live race feeds
│   └── data_validators.py     # Validate race results
├── processing/
│   ├── driver_embeddings.py   # Driver photo/performance embeddings
│   ├── circuit_embeddings.py  # Circuit layout embeddings
│   ├── telemetry_processing.py # Real-time telemetry analysis
│   └── race_transformers.py   # Race result transformations
├── storage/
│   ├── f1_parquet_ops.py     # F1-specific Parquet operations
│   └── f1_vector_ops.py      # F1 vector database operations
├── analytics/
│   ├── championship_queries.py # Championship and standings analysis
│   ├── performance_analysis.py # Driver/constructor performance
│   └── race_visualizations.py  # F1 data visualizations
└── ml_models/
    ├── race_prediction.py     # Race outcome prediction
    ├── strategy_optimization.py # Pit stop and tire strategy
    └── driver_performance.py  # Driver performance modeling
```

### Error Handling for F1 Data

```python
def robust_f1_processing_pipeline(f1_data_path):
    """Robust F1 data processing with comprehensive error handling"""
    try:
        # Process F1 data with proper error handling
        f1_df = daft.read_parquet(f1_data_path)
        
        # Validate F1 data quality
        race_count = f1_df.count().compute()
        if race_count == 0:
            raise ValueError("Empty F1 dataset - no races found")
        
        # Validate data integrity for F1 specifics
        valid_seasons = f1_df.where(
            (daft.col("season") >= 1950) & 
            (daft.col("season") <= 2025)
        )
        
        invalid_count = race_count - valid_seasons.count().compute()
        if invalid_count > race_count * 0.1:  # More than 10% invalid
            raise ValueError(f"Too many invalid F1 season entries: {invalid_count}")
        
        # Process with F1-specific error recovery
        processed_f1 = valid_seasons.with_column(
            "processed_race_data",
            daft.col("raw_race_data").apply(safe_f1_process_function)
        ).where(daft.col("processed_race_data").is_not_null())
        
        return processed_f1
        
    except Exception as e:
        logger.error(f"F1 data pipeline failed: {e}")
        # Implement F1-specific fallback strategy
        return handle_f1_processing_failure(f1_data_path, e)

def safe_f1_process_function(race_data):
    """Safely process individual F1 race records"""
    try:
        # Validate essential F1 fields
        required_fields = ['driver_name', 'constructor', 'circuit_name', 'season']
        if not all(field in race_data for field in required_fields):
            return None
            
        # Process F1-specific calculations
        race_data['points_normalized'] = normalize_f1_points_by_era(
            race_data.get('points', 0), 
            race_data.get('season', 2025)
        )
        
        race_data['performance_score'] = calculate_f1_performance_score(race_data)
        
        return race_data
        
    except Exception as e:
        logger.warning(f"Failed to process F1 race record: {e}")
        return None
```

## Conclusion

The modern data stack enables powerful multimodal F1 data processing pipelines that transform traditional race analytics into comprehensive motorsports intelligence systems. By combining:

- **Parquet** for efficient storage of historical F1 race data from the Kaggle dataset
- **DuckDB** for fast championship and performance analytics  
- **LanceDB** for driver similarity and circuit comparison operations
- **Daft** for processing multimodal F1 content (driver photos, circuit layouts, race videos)
- **Databricks** for enterprise-scale F1 team analytics

You can build robust, scalable systems that handle the complexity of modern Formula 1 data applications, from historical analysis of 75 years of racing to real-time strategy optimization during live Grand Prix events.

### F1-Specific Next Steps

1. **Download the dataset**: Get the Formula 1 Grand Prix Winners Dataset (1950–2025) from [Kaggle](https://www.kaggle.com/datasets/julianbloise/winners-formula-1-1950-to-2025)
2. **Experiment**: Try the examples with the actual F1 CSV files
3. **Optimize**: Profile and tune for F1-specific query patterns (championship analysis, circuit performance)
4. **Scale**: Gradually add live telemetry data and real-time race feeds
5. **Monitor**: Implement observability for race weekend analytics loads
6. **Extend**: Add predictive modeling for race outcomes and strategy optimization

### F1 Analytics Resources

- [Formula 1 Dataset on Kaggle](https://www.kaggle.com/datasets/julianbloise/winners-formula-1-1950-to-2025)
- [Daft Documentation](https://docs.daft.ai/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Databricks Documentation](https://docs.databricks.com/)
- [F1 Technical Regulations](https://www.fia.com/regulation/category/110) - For understanding data context

### Sample F1 Queries to Get Started

```sql
-- Most successful drivers by decade
WITH decades AS (
    SELECT 
        driver_name,
        constructor_name,
        FLOOR(season/10)*10 as decade,
        SUM(points) as decade_points,
        COUNT(CASE WHEN position = 1 THEN 1 END) as decade_wins
    FROM f1_complete_dataset
    GROUP BY driver_name, constructor_name, decade
)
SELECT decade, driver_name, constructor_name, decade_points, decade_wins
FROM decades 
WHERE decade_points = (
    SELECT MAX(decade_points) 
    FROM decades d2 
    WHERE d2.decade = decades.decade
)
ORDER BY decade;

-- Circuit difficulty analysis
SELECT 
    circuit_name,
    country,
    AVG(CASE WHEN status != 'Finished' THEN 1.0 ELSE 0.0 END) as dnf_rate,
    AVG(position) as avg_finishing_position,
    COUNT(CASE WHEN fastest_lap = 1 THEN 1 END) as fastest_laps_set
FROM f1_complete_dataset
WHERE season >= 2020
GROUP BY circuit_name, country
ORDER BY dnf_rate DESC;
```

---

*This tutorial provides a foundation for building modern multimodal F1 data pipelines using real Formula 1 racing data. The motorsports analytics landscape evolves rapidly with new regulations and technologies, so stay updated with the latest developments in each tool and F1 data sources.*

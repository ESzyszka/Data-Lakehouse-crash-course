## Conclusion

The modern data stack enables powerful multimodal data processing pipelines that bridge traditional analytics and AI workloads. Using the **Formula 1 Grand Prix Winners Dataset (1950–2025)** as our example, we've demonstrated how to combine:

- **Parquet** for efficient storage of historical F1 race data
- **DuckDB** for fast championship and era analysis
- **LanceDB** for driver similarity and race characteristic searches
- **Daft** for multimodal processing of race videos, driver photos, and telemetry  
- **Databricks** for enterprise-scale F1 analytics

This architecture allows you to build sophisticated F1 analytics systems that can:
- Find drivers with similar racing styles and performance patterns
- Search through decades of race footage for specific moments
- Analyze championship battles across different eras
- Prepare rich F1 datasets for training specialized language models
- Scale from historical analysis to real-time race prediction

### Key Achievements from this Tutorial

1. **Real-world application**: Used actual Kaggle F1 dataset showing practical implementation
2. **End-to-end pipeline**: Complete workflow from data ingestion to AI-ready embeddings
3. **Performance optimization**: Efficient processing of 75+ years of F1 historical data
4. **Multimodal capabilities**: Integration of race videos, driver photos, and structured data
5. **Enterprise readiness**: Robust error handling and scalable architecture patterns

### Next Steps

1. **Download the dataset**: Get the [Formula 1 Grand Prix Winners Dataset](https://www.kaggle.com/datasets/julianbloise/winners-formula-1-1950-to-2025) from Kaggle
2. **Start small**: Begin with basic Parquet operations on recent F1 seasons (2020-2025)
3. **Add embeddings**: Implement driver performance and race characteristic embeddings
4. **Build vector search**: Create similarity search for drivers and races
5. **Scale gradually**: Expand to full historical dataset and add video/image processing
6. **Monitor and optimize**: Profile queries and optimize for your specific F1 analysis use cases

### Practical F1 Use Cases to Explore

- **Championship predictor**: Use historical patterns to predict season outcomes
- **Driver career analyzer**: Compare### Step 4: F1 Vector Search

```python
def find_similar_drivers(driver_name, limit=5):
    """Find drivers with similar performance characteristics"""
    
    # Get the driver's embedding# Modern Multimodal Data Processing: From Parquet to AI Workloads

*A comprehensive guide to querying multimodal data using Parquet, DuckDB, LanceDB, Daft, and Databricks*

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

The data landscape has evolved dramatically. We've moved from simple CSV files to sophisticated multimodal data processing pipelines that can handle text, images, video, and more. This tutorial explores the modern data stack and how to leverage it for AI and analytics workloads.

### What You'll Learn
- How to work with Parquet files for efficient data storage
- Using DuckDB for fast analytical queries
- Implementing vector search with LanceDB
- Processing multimodal data with Daft
- Scaling with Databricks
- Building production-ready data pipelines

## Understanding Data Evolution

### Traditional Tabular Workloads
Traditional analytics focused on structured data:
```sql
-- Formula 1 race analysis example
SELECT driver, COUNT(*) as wins
FROM f1_winners 
WHERE year >= 2020
GROUP BY driver
ORDER BY wins DESC;
```

### Modern Multimodal Workloads
Today's applications require processing diverse data types in F1 analytics:
- **Race videos** for computer vision analysis (overtaking detection, crashes)
- **Radio communications** for speech processing and sentiment analysis
- **Telemetry data** combined with driver photos for performance analysis
- **Historical race embeddings** for similarity search and predictions

## Parquet Files: The Foundation

Parquet has become the de facto standard for analytical workloads, replacing CSV for several key reasons:

### Why Parquet Over CSV?

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Download F1 dataset from Kaggle first
# kaggle datasets download -d julianbloise/winners-formula-1-1950-to-2025

# CSV limitations - slow loading of F1 data
df_csv = pd.read_csv('formula-1-winners.csv')  # Slow, no schema, large size

# Parquet benefits - efficient F1 data storage
df_parquet = pd.read_parquet('f1_winners.parquet')  # Fast, typed, compressed
```

### Key Advantages:
1. **Columnar Storage**: Better compression and query performance
2. **Schema Evolution**: Built-in type system
3. **Predicate Pushdown**: Skip irrelevant data
4. **Compression**: Significantly smaller file sizes

### Working with Parquet

```python
# Load the F1 dataset and convert to Parquet
import pandas as pd
import numpy as np

# Read the Kaggle F1 dataset
df_f1 = pd.read_csv('formula-1-winners.csv')

# Add multimodal columns for our examples
df_f1['driver_photo'] = df_f1['Driver'].apply(lambda x: f'photos/{x.replace(" ", "_").lower()}.jpg')
df_f1['race_video'] = df_f1.apply(lambda x: f'videos/{x["Year"]}_{x["Grand Prix"].replace(" ", "_")}.mp4', axis=1)
df_f1['driver_embedding'] = [np.random.random(768).tolist() for _ in range(len(df_f1))]

# Save as Parquet
df_f1.to_parquet('f1_multimodal.parquet')

# Reading with filters - find all wins by Lewis Hamilton
hamilton_wins = pd.read_parquet(
    'f1_multimodal.parquet',
    filters=[('Driver', '==', 'Lewis Hamilton')]
)
print(f"Lewis Hamilton has {len(hamilton_wins)} wins in the dataset")
```

## DuckDB: Analytics Made Simple

DuckDB provides a lightweight, embedded analytical database that excels at processing Parquet files.

### Getting Started

```python
import duckdb

# Connect to DuckDB (in-memory or persistent)
conn = duckdb.connect('f1_analytics.duckdb')

# Query F1 Parquet files directly
result = conn.execute("""
    SELECT Driver, Team, COUNT(*) as wins
    FROM 'f1_multimodal.parquet'
    WHERE Year >= 2000
    GROUP BY Driver, Team
    ORDER BY wins DESC
    LIMIT 10
""").fetchall()

print("Top F1 winners since 2000:")
for driver, team, wins in result:
    print(f"{driver} ({team}): {wins} wins")
```

### Advanced Features

#### Object Storage Integration
```sql
-- Query F1 race data directly from S3
SELECT Driver, "Grand Prix", Year, Team
FROM 's3://f1-data/races/*.parquet'
WHERE Year >= 2020 AND Team = 'Mercedes'
ORDER BY Year, "Grand Prix";
```

#### Apache Iceberg Support
DuckDB integrates with Iceberg for transactional semantics on F1 data:

```python
# Iceberg table operations for F1 race results
conn.execute("""
    CREATE TABLE f1_iceberg_races AS
    SELECT * FROM iceberg_scan('s3://f1-lake/iceberg-races/')
    WHERE Year >= 2020
""")

# Time travel to see historical race results
historical_results = conn.execute("""
    SELECT Driver, Team, "Grand Prix"
    FROM f1_iceberg_races
    FOR SYSTEM_TIME AS OF '2023-01-01'
    WHERE Driver = 'Max Verstappen'
""").fetchall()
```

### Architecture Benefits

DuckDB's C++ implementation provides:
- **Fast execution**: Vectorized processing
- **Memory efficiency**: Optimized for analytical workloads  
- **Easy deployment**: Single binary, no server required
- **ACID transactions**: Reliable data operations

## LanceDB: Vector Search and Multimodal Data

LanceDB specializes in vector storage and similarity search, essential for AI applications.

### Installation and Setup

```python
import lancedb
import numpy as np
import pandas as pd

# Create/connect to LanceDB for F1 data
db = lancedb.connect("./f1-lance-data")

# Load F1 dataset and create embeddings
df_f1 = pd.read_parquet('f1_multimodal.parquet')

# Create sample multimodal F1 data with embeddings
f1_embeddings = []
for _, row in df_f1.head(100).iterrows():  # Using first 100 records for example
    f1_embeddings.append({
        "race_id": f"{row['Year']}_{row['Grand Prix'].replace(' ', '_')}",
        "driver": row['Driver'],
        "team": row['Team'], 
        "grand_prix": row['Grand Prix'],
        "year": row['Year'],
        "driver_photo": row['driver_photo'],
        "race_video": row['race_video'],
        "driver_embedding": np.random.random(768).tolist(),  # Simulated driver performance embedding
        "race_embedding": np.random.random(768).tolist(),    # Simulated race characteristics embedding
        "metadata": {
            "constructor": row['Team'],
            "era": "modern" if row['Year'] >= 2000 else "classic",
            "decade": f"{row['Year']//10*10}s"
        }
    })

# Create LanceDB table
table = db.create_table("f1_multimodal_embeddings", f1_embeddings)
```

### Vector Search Operations

```python
# Find similar F1 drivers based on performance embeddings
sample_driver_embedding = f1_embeddings[0]["driver_embedding"]

# Search for similar driving styles/performance
similar_drivers = table.search(sample_driver_embedding, vector_column="driver_embedding")\
    .limit(5)\
    .to_list()

print("Drivers with similar performance characteristics:")
for result in similar_drivers:
    print(f"- {result['driver']} ({result['team']}) - {result['year']}")

# Find similar race characteristics
sample_race_embedding = f1_embeddings[0]["race_embedding"] 

similar_races = table.search(sample_race_embedding, vector_column="race_embedding")\
    .where("metadata.era = 'modern'")\
    .limit(5)\
    .to_list()

print("\nRaces with similar characteristics:")
for result in similar_races:
    print(f"- {result['grand_prix']} {result['year']} (Winner: {result['driver']})")
```

### Use Cases
- **Driver similarity analysis**: Find drivers with similar racing styles and performance patterns
- **Race video search**: Content-based retrieval of similar race moments and overtaking maneuvers  
- **Historical race analysis**: Semantic search through decades of F1 commentary and reports
- **Team strategy recommendations**: Similar race conditions and strategic decisions

## Daft: Multimodal Data Processing

Daft serves as the compute layer bridging data and AI workloads.

### Installation

```bash
pip install getdaft
```

### Basic Operations

```python
import daft

# Read F1 multimodal data
df = daft.read_parquet("f1_multimodal.parquet")

# Load driver photos for computer vision analysis
def load_driver_image(photo_path):
    """Simulate loading driver photos for facial recognition/analysis"""
    # In practice, you'd load actual images
    return f"image_data_for_{photo_path}"

df_with_images = df.with_column(
    "driver_image_data", 
    df["driver_photo"].apply(load_driver_image, return_dtype=daft.DataType.string())
)

# Extract facial features for driver recognition
def extract_driver_features(image_data):
    """Extract facial features from driver photos"""
    # In practice, use a model like FaceNet or similar
    return np.random.random(512).tolist()  # Simulated facial embedding

df_processed = df_with_images.with_column(
    "facial_features",
    df_with_images["driver_image_data"].apply(
        extract_driver_features, 
        return_dtype=daft.DataType.tensor(daft.DataType.float32())
    )
)
```

### Advanced Multimodal Operations

```python
# F1 race video processing pipeline
def extract_race_frames(video_path):
    """Extract key frames from F1 race videos"""
    # In practice, extract frames at key moments (starts, overtakes, crashes)
    return [f"frame_{i}.jpg" for i in range(10)]

def generate_race_embeddings(frames):
    """Generate embeddings from race video frames"""
    # In practice, use a video analysis model
    return np.random.random(768).tolist()

# Process race videos
race_videos_df = df.select(["race_video", "Driver", "Grand Prix", "Year"])

# Extract key frames from race footage
frames_df = race_videos_df.with_column(
    "key_frames",
    race_videos_df["race_video"].apply(extract_race_frames)
)

# Generate embeddings for race analysis
embeddings_df = frames_df.with_column(
    "race_video_embedding", 
    frames_df["key_frames"].apply(generate_race_embeddings)
)

# Save processed F1 video data to LanceDB
embeddings_df.write_lance("f1_video_embeddings")
```

### Key Features
- **Lazy evaluation**: Efficient processing of large F1 historical datasets
- **Multimodal support**: Native handling of race videos, driver photos, telemetry data
- **Distributed compute**: Scale across multiple machines for processing decades of F1 data
- **Integration**: Works seamlessly with other tools in the F1 analytics stack

## Databricks: Enterprise Scale

Databricks provides the enterprise platform for large-scale multimodal data processing.

### ACID Transactions

Databricks implements ACID properties for reliable data operations:

```sql
-- Atomic F1 operations
BEGIN TRANSACTION;
    INSERT INTO f1_driver_embeddings SELECT * FROM new_driver_analysis;
    UPDATE f1_driver_profiles SET last_updated = current_timestamp();
    INSERT INTO f1_race_predictions SELECT * FROM latest_predictions;
COMMIT;
```

### Delta Lake Integration

```python
from delta import *
from pyspark.sql import SparkSession

# Configure Spark for Delta Lake
spark = configure_spark_with_delta_pip(SparkSession.builder).getOrCreate()

# Read F1 Delta table
f1_results_df = spark.read.format("delta").load("/path/to/f1-results-delta")

# Time travel to see historical F1 standings
historical_f1 = spark.read.format("delta")\
    .option("timestampAsOf", "2020-12-31")\
    .load("/path/to/f1-results-delta")

# Analyze championship battles over time
championship_evolution = historical_f1.filter(
    historical_f1.Driver.isin(["Lewis Hamilton", "Max Verstappen"])
).groupBy("Driver", "Year").count().orderBy("Year")
```

### Advanced Features

#### Composable Table Formats
Modern table formats provide:
- **Schema evolution**: Add/modify columns safely
- **Time travel**: Query historical data
- **ACID transactions**: Reliable concurrent operations
- **File skipping**: Query only relevant files

#### Performance Optimizations
```sql
-- Z-ordering for better file skipping on F1 queries
OPTIMIZE f1_race_results ZORDER BY (year, driver, constructor);

-- Partition by decade for efficient historical analysis
CREATE TABLE f1_partitioned
USING DELTA
PARTITIONED BY (decade)
AS SELECT *, (year DIV 10) * 10 as decade FROM f1_race_results;

-- Vacuum old files
VACUUM f1_race_results RETAIN 168 HOURS;
```

## Building a Complete Pipeline

Let's build an end-to-end F1 multimodal data pipeline:

### Step 1: F1 Data Ingestion

```python
import daft
import lancedb
import duckdb
import pandas as pd
import numpy as np

def ingest_f1_data(kaggle_csv_path):
    """Ingest and enrich F1 data with multimodal components"""
    
    # Read Kaggle F1 dataset
    df = pd.read_csv(kaggle_csv_path)
    
    # Convert to Daft for multimodal processing
    daft_df = daft.from_pandas(df)
    
    # Add multimodal paths
    daft_df = daft_df.with_columns([
        daft.col("Driver").apply(
            lambda x: f"photos/drivers/{x.replace(' ', '_').lower()}.jpg"
        ).alias("driver_photo"),
        
        daft.col("Grand Prix").apply(
            lambda gp, year=daft.col("Year"): f"videos/races/{year}_{gp.replace(' ', '_')}.mp4"
        ).alias("race_video"),
        
        daft.col("Team").apply(
            lambda team: f"audio/radio/{team.replace(' ', '_')}_radio.wav"
        ).alias("team_radio")
    ])
    
    # Generate embeddings (in practice, use actual ML models)
    def generate_driver_embedding(driver_name):
        """Generate driver performance embedding"""
        # This would use actual driver statistics, racing style analysis, etc.
        np.random.seed(hash(driver_name) % 2147483647)
        return np.random.random(768).tolist()
    
    def generate_race_embedding(grand_prix, year):
        """Generate race characteristics embedding"""
        # This would analyze track layout, weather, historical data, etc.
        np.random.seed(hash(f"{grand_prix}_{year}") % 2147483647)
        return np.random.random(768).tolist()
    
    # Add embeddings
    daft_df = daft_df.with_columns([
        daft.col("Driver").apply(generate_driver_embedding).alias("driver_embedding"),
        daft.col("Grand Prix").apply(
            lambda gp, year=daft.col("Year"): generate_race_embedding(gp, year)
        ).alias("race_embedding")
    ])
    
    return daft_df

# Process the F1 data
f1_processed = ingest_f1_data("formula-1-winners.csv")
```

### Step 2: Storage and Indexing

```python
# Save F1 data to Parquet for analytical queries
f1_processed.write_parquet("f1_multimodal_processed.parquet")

# Create LanceDB vector index for similarity search
db = lancedb.connect("./f1_vector_db")

# Convert to format suitable for LanceDB
f1_pandas = f1_processed.to_pandas()
lance_data = []

for _, row in f1_pandas.iterrows():
    lance_data.append({
        "race_id": f"{row['Year']}_{row['Grand Prix'].replace(' ', '_')}",
        "driver": row['Driver'],
        "constructor": row['Team'],
        "grand_prix": row['Grand Prix'],
        "year": int(row['Year']),
        "driver_embedding": row['driver_embedding'],
        "race_embedding": row['race_embedding'],
        "driver_photo": row['driver_photo'],
        "race_video": row['race_video'],
        "metadata": {
            "era": "modern" if row['Year'] >= 2000 else "classic",
            "decade": f"{int(row['Year'])//10*10}s",
            "constructor": row['Team']
        }
    })

# Create vector search table
f1_table = db.create_table("f1_multimodal_search", lance_data)
print(f"Indexed {len(lance_data)} F1 race records for vector search")
```

### Step 3: F1 Analytics with DuckDB

```python
conn = duckdb.connect("f1_analytics.duckdb")

# Championship analysis
championship_analysis = conn.execute("""
    SELECT 
        driver,
        constructor,
        COUNT(*) as total_wins,
        MIN(year) as first_win,
        MAX(year) as last_win,
        MAX(year) - MIN(year) as career_span
    FROM 'f1_multimodal_processed.parquet'
    GROUP BY driver, constructor
    HAVING total_wins >= 5
    ORDER BY total_wins DESC
""").fetchall()

print("F1 Champions Analysis (5+ wins):")
for driver, constructor, wins, first, last, span in championship_analysis:
    print(f"{driver} ({constructor}): {wins} wins ({first}-{last}, {span} year span)")

# Era comparison
era_stats = conn.execute("""
    SELECT 
        CASE 
            WHEN year < 1980 THEN 'Classic Era (pre-1980)'
            WHEN year < 2000 THEN 'Modern Era (1980-1999)' 
            WHEN year < 2014 THEN 'V8 Era (2000-2013)'
            ELSE 'Hybrid Era (2014+)'
        END as era,
        COUNT(*) as races,
        COUNT(DISTINCT driver) as unique_winners,
        COUNT(DISTINCT constructor) as unique_constructors
    FROM 'f1_multimodal_processed.parquet'
    GROUP BY 1
    ORDER BY MIN(year)
""").fetchall()

print("\nF1 Era Analysis:")
for era, races, winners, constructors in era_stats:
    print(f"{era}: {races} races, {winners} unique winners, {constructors} constructors")
```

### Step 4: F1 Vector Search

```python
def find_similar_drivers(driver_name, limit=5):
    """Find drivers with similar performance characteristics"""
    
    # Get the driver's embedding
    driver_record = f1_table.search()\
        .where(f"driver = '{driver_name}'")\
        .limit(1)\
        .to_list()
    
    if not driver_record:
        print(f"Driver {driver_name} not found")
        return []
    
    driver_embedding = driver_record[0]['driver_embedding']
    
    # Find similar drivers
    similar = f1_table.search(driver_embedding, vector_column="driver_embedding")\
        .where(f"driver != '{driver_name}'")\
        .limit(limit)\
        .to_list()
    
    return similar

def find_similar_races(grand_prix, year, limit=5):
    """Find races with similar characteristics"""
    
    # Get the race embedding
    race_record = f1_table.search()\
        .where(f"grand_prix = '{grand_prix}' AND year = {year}")\
        .limit(1)\
        .to_list()
    
    if not race_record:
        print(f"Race {grand_prix} {year} not found")
        return []
    
    race_embedding = race_record[0]['race_embedding']
    
    # Find similar races
    similar = f1_table.search(race_embedding, vector_column="race_embedding")\
        .where(f"NOT (grand_prix = '{grand_prix}' AND year = {year})")\
        .limit(limit)\
        .to_list()
    
    return similar

# Example usage
print("Finding drivers similar to Lewis Hamilton:")
similar_to_hamilton = find_similar_drivers("Lewis Hamilton", limit=5)
for driver in similar_to_hamilton:
    print(f"- {driver['driver']} ({driver['constructor']}, {driver['year']})")

print("\nFinding races similar to Monaco 2008:")
similar_races = find_similar_races("Monaco Grand Prix", 2008, limit=3)
for race in similar_races:
    print(f"- {race['grand_prix']} {race['year']} (Winner: {race['driver']})")
```

## Performance Optimization

### File Skipping Strategies

1. **Partition F1 data by era**: Organize by decades or regulatory periods
2. **Use appropriate file sizes**: 100MB-1GB per file for optimal F1 historical data
3. **Implement predicate pushdown**: Filter at the storage level for year/driver queries

### Query Optimization

```python
# Efficient F1 data filtering
modern_era_wins = daft.read_parquet("f1_multimodal_processed.parquet")\
    .where(daft.col("Year") >= 2000)\
    .where(daft.col("Driver").isin(["Lewis Hamilton", "Michael Schumacher", "Sebastian Vettel"]))

# Lazy evaluation for driver analysis
driver_analysis = modern_era_wins.select(["Driver", "Team", "Grand Prix", "Year"])\
    .with_column("wins_per_team", 
                 daft.col("Driver").apply(lambda d: compute_team_wins(d)))

# Process championship battles efficiently
championship_data = modern_era_wins.groupby(["Driver", "Year"])\
    .agg([daft.col("Grand Prix").count().alias("wins_per_season")])
```

### Memory Management

```python
# Process large F1 historical datasets in batches
def process_f1_decades(file_path, batch_size=1000):
    """Process F1 data decade by decade to manage memory"""
    df = daft.read_parquet(file_path)
    
    # Group by decade for efficient processing
    decades = [1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020]
    
    for decade in decades:
        decade_data = df.where(
            (daft.col("Year") >= decade) & 
            (daft.col("Year") < decade + 10)
        )
        
        # Process decade data
        processed_decade = process_f1_decade_batch(decade_data)
        yield f"Processed {decade}s F1 data", processed_decade

def process_f1_decade_batch(decade_df):
    """Process a single decade of F1 data"""
    return decade_df.with_columns([
        decade_df["Driver"].apply(analyze_driver_performance).alias("performance_score"),
        decade_df["Team"].apply(analyze_constructor_dominance).alias("team_strength")
    ])

# Example usage
for decade_info, processed_data in process_f1_decades("f1_multimodal_processed.parquet"):
    print(decade_info)
    # Save or further process each decade
```

## Real-World Use Cases

### 1. F1 Driver Recommendation System

```python
def build_f1_driver_recommendations(target_driver):
    """Build recommendations for similar F1 drivers based on performance"""
    
    # Get target driver's race history and performance
    driver_history = conn.execute("""
        SELECT year, constructor, grand_prix, 
               ROW_NUMBER() OVER (PARTITION BY year ORDER BY year) as race_order
        FROM f1_multimodal_processed 
        WHERE driver = ?
        ORDER BY year DESC
        LIMIT 50
    """, [target_driver]).fetchall()
    
    if not driver_history:
        return f"No data found for {target_driver}"
    
    # Get driver's performance embedding
    driver_records = f1_table.search()\
        .where(f"driver = '{target_driver}'")\
        .limit(10)\
        .to_list()
    
    # Find similar drivers based on performance patterns
    recommendations = []
    for record in driver_records:
        driver_embedding = record['driver_embedding']
        similar_drivers = f1_table.search(driver_embedding, vector_column="driver_embedding")\
            .where(f"driver != '{target_driver}'")\
            .limit(3)\
            .to_list()
        recommendations.extend(similar_drivers)
    
    # Deduplicate and rank recommendations
    unique_drivers = {}
    for rec in recommendations:
        driver_name = rec['driver']
        if driver_name not in unique_drivers:
            unique_drivers[driver_name] = {
                'driver': driver_name,
                'constructor': rec['constructor'],
                'years': [rec['year']],
                'count': 1
            }
        else:
            unique_drivers[driver_name]['years'].append(rec['year'])
            unique_drivers[driver_name]['count'] += 1
    
    # Sort by frequency of similarity
    sorted_recommendations = sorted(
        unique_drivers.values(), 
        key=lambda x: x['count'], 
        reverse=True
    )[:5]
    
    return sorted_recommendations

# Example usage
hamilton_similar = build_f1_driver_recommendations("Lewis Hamilton")
print(f"Drivers similar to Lewis Hamilton:")
for rec in hamilton_similar:
    year_range = f"{min(rec['years'])}-{max(rec['years'])}"
    print(f"- {rec['driver']} ({rec['constructor']}) - Active: {year_range}")
```

### 2. F1 Multimodal Search Engine

```python
def f1_multimodal_search(query_text=None, driver_name=None, era_filter=None):
    """Search F1 data using text, driver performance, or era characteristics"""
    
    results = []
    
    if query_text:
        # Text-based search through race descriptions/metadata
        # In practice, you'd generate text embedding from query
        text_embedding = generate_text_embedding(query_text)
        text_results = f1_table.search(text_embedding, vector_column="race_embedding")\
            .limit(10)\
            .to_list()
        results.extend(text_results)
    
    if driver_name:
        # Driver performance-based search
        driver_records = f1_table.search()\
            .where(f"driver = '{driver_name}'")\
            .limit(5)\
            .to_list()
        
        if driver_records:
            driver_embedding = driver_records[0]['driver_embedding']
            similar_performances = f1_table.search(driver_embedding, vector_column="driver_embedding")\
                .limit(10)\
                .to_list()
            results.extend(similar_performances)
    
    if era_filter:
        # Era-based filtering
        era_results = f1_table.search()\
            .where(f"metadata.era = '{era_filter}'")\
            .limit(20)\
            .to_list()
        results.extend(era_results)
    
    # Merge and rank results
    return merge_and_rank_f1_results(results)

def merge_and_rank_f1_results(results):
    """Merge and rank F1 search results"""
    unique_results = {}
    
    for result in results:
        race_id = result['race_id']
        if race_id not in unique_results:
            unique_results[race_id] = result
            unique_results[race_id]['relevance_score'] = 1
        else:
            unique_results[race_id]['relevance_score'] += 1
    
    # Sort by relevance
    ranked_results = sorted(
        unique_results.values(),
        key=lambda x: x['relevance_score'],
        reverse=True
    )
    
    return ranked_results[:10]

# Example searches
print("Search 1: Monaco races")
monaco_results = f1_multimodal_search(query_text="Monaco street circuit challenging")
for result in monaco_results[:3]:
    print(f"- {result['grand_prix']} {result['year']} (Winner: {result['driver']})")

print("\nSearch 2: Similar to Michael Schumacher's performance")
schumacher_similar = f1_multimodal_search(driver_name="Michael Schumacher")
for result in schumacher_similar[:3]:
    print(f"- {result['driver']} at {result['grand_prix']} {result['year']}")

print("\nSearch 3: Modern era races")
modern_results = f1_multimodal_search(era_filter="modern")
for result in modern_results[:3]:
    print(f"- {result['grand_prix']} {result['year']} ({result['constructor']})")
```

### 3. F1 Dataset Preparation for LLMs

```python
def prepare_f1_llm_dataset(source_data):
    """Prepare F1 dataset for training language models on racing knowledge"""
    
    # Process F1 data with Daft
    df = daft.read_parquet(source_data)
    
    # Create rich text descriptions for LLM training
    def create_race_description(row):
        return f"""In {row['Year']}, {row['Driver']} driving for {row['Team']} won the {row['Grand Prix']}. This victory was part of the {row['Year']} Formula 1 World Championship season. {row['Driver']} demonstrated exceptional skill and racecraft to claim victory at this prestigious event."""
    
    df_with_descriptions = df.with_column(
        "race_description",
        df.apply(create_race_description, return_dtype=daft.DataType.string())
    )
    
    # Quality filtering for LLM training
    df_quality = df_with_descriptions.where(
        (daft.col("Year") >= 1970) &  # Focus on modern era with better documentation
        (daft.col("Driver").str.len() > 3) &  # Filter out incomplete names
        (daft.col("Team").str.len() > 2)  # Filter out incomplete team names
    )
    
    # Generate embeddings for deduplication
    def generate_content_embedding(description):
        """Generate embedding for content deduplication"""
        # In practice, use a proper text embedding model
        return np.random.random(384).tolist()
    
    df_embedded = df_quality.with_column(
        "content_embedding",
        df_quality["race_description"].apply(generate_content_embedding)
    )
    
    # Create training examples with various formats
    def create_training_examples(row):
        """Create multiple training formats from F1 data"""
        examples = []
        
        # Q&A format
        examples.append({
            "instruction": f"Who won the {row['Grand Prix']} in {row['Year']}?",
            "response": f"{row['Driver']} won the {row['Grand Prix']} in {row['Year']}, driving for {row['Team']}.",
            "category": "race_winner"
        })
        
        # Historical context format
        examples.append({
            "instruction": f"Tell me about {row['Driver']}'s victory at the {row['Year']} {row['Grand Prix']}.",
            "response": row['race_description'],
            "category": "race_description"
        })
        
        # Championship context
        examples.append({
            "instruction": f"What team did {row['Driver']} drive for in {row['Year']}?",
            "response": f"In {row['Year']}, {row['Driver']} drove for {row['Team']}.",
            "category": "driver_team"
        })
        
        return examples
    
    # Generate training examples
    training_examples = df_embedded.with_column(
        "training_examples",
        df_embedded.apply(create_training_examples, return_dtype=daft.DataType.python())
    )
    
    # Deduplicate using vector similarity (simplified for example)
    # In practice, you'd implement proper semantic deduplication
    deduplicated_df = training_examples.distinct()
    
    # Save in format suitable for LLM training
    deduplicated_df.select([
        "Year", "Driver", "Team", "Grand Prix", 
        "race_description", "training_examples"
    ]).write_parquet("f1_llm_training_data.parquet")
    
    # Create JSONL format for popular LLM training frameworks
    training_data = []
    for _, row in deduplicated_df.to_pandas().iterrows():
        for example in row['training_examples']:
            training_data.append({
                "instruction": example["instruction"],
                "input": "",  # F1 questions usually don't need additional input
                "output": example["response"],
                "metadata": {
                    "year": row["Year"],
                    "driver": row["Driver"],
                    "constructor": row["Team"],
                    "grand_prix": row["Grand Prix"],
                    "category": example["category"]
                }
            })
    
    # Save as JSONL for training
    import json
    with open("f1_llm_training.jsonl", "w") as f:
        for item in training_data:
            f.write(json.dumps(item) + "\n")
    
    print(f"Created {len(training_data)} training examples from F1 historical data")
    print("Training data categories:")
    categories = {}
    for item in training_data:
        cat = item["metadata"]["category"]
        categories[cat] = categories.get(cat, 0) + 1
    
    for cat, count in categories.items():
        print(f"  {cat}: {count} examples")
    
    return deduplicated_df

# Prepare F1 dataset for LLM training
f1_llm_data = prepare_f1_llm_dataset("f1_multimodal_processed.parquet")
```

## Best Practices

### Data Architecture
1. **Separate storage and compute**: Use object storage for F1 historical data, compute engines for race analysis
2. **Implement proper data governance**: Track lineage of race results, data quality, and access patterns
3. **Version your data**: Use tools like DVC or Delta Lake for versioning F1 datasets as new seasons are added
4. **Monitor performance**: Track query performance for championship analysis and historical comparisons

### Code Organization
```python
# F1 Analytics Project Structure
f1_multimodal_pipeline/
├── data_ingestion/
│   ├── kaggle_parser.py       # Parse Kaggle F1 dataset
│   ├── race_validators.py     # Validate race result data
│   └── season_processors.py   # Process by F1 season
├── processing/
│   ├── driver_embeddings.py   # Generate driver performance embeddings
│   ├── race_transformers.py   # Transform race characteristics
│   └── team_analysis.py       # Constructor/team analysis
├── storage/
│   ├── f1_parquet_ops.py     # F1-specific Parquet operations
│   ├── vector_ops.py         # Vector operations for similarity
│   └── historical_archive.py # Historical data management
└── analytics/
    ├── championship_queries.py # Championship and standings analysis
    ├── era_comparisons.py     # Cross-era performance comparisons
    └── race_visualizations.py # F1 data visualizations
```

### Error Handling
```python
def robust_f1_processing_pipeline(kaggle_csv_path):
    """Robust F1 data processing with comprehensive error handling"""
    
    try:
        # Validate Kaggle dataset
        if not os.path.exists(kaggle_csv_path):
            raise FileNotFoundError(f"F1 dataset not found at {kaggle_csv_path}")
        
        # Read and validate F1 data
        df = pd.read_csv(kaggle_csv_path)
        
        # Validate required F1 columns
        required_columns = ['Year', 'Grand Prix', 'Driver', 'Team']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required F1 columns: {missing_columns}")
        
        # Data quality checks
        if df.empty:
            raise ValueError("F1 dataset is empty")
        
        if df['Year'].min() < 1950:
            logger.warning("F1 data contains races before 1950 - may be invalid")
        
        # Process with error recovery for each driver
        def safe_driver_analysis(driver_name):
            try:
                return analyze_driver_performance(driver_name)
            except Exception as e:
                logger.error(f"Failed to analyze driver {driver_name}: {e}")
                return None  # Return None for failed analysis
        
        # Convert to Daft and process safely
        daft_df = daft.from_pandas(df)
        processed_df = daft_df.with_column(
            "performance_analysis",
            daft_df["Driver"].apply(safe_driver_analysis)
        ).where(daft.col("performance_analysis").is_not_null())  # Filter out failed analyses
        
        # Verify we have sufficient data after processing
        processed_count = processed_df.count().compute()
        if processed_count == 0:
            raise ValueError("All F1 driver analyses failed - check data quality")
        
        original_count = len(df)
        success_rate = processed_count / original_count
        if success_rate < 0.8:
            logger.warning(f"Low success rate: {success_rate:.2%} of F1 records processed successfully")
        
        return processed_df
        
    except FileNotFoundError as e:
        logger.error(f"F1 dataset file error: {e}")
        return handle_missing_f1_data()
    
    except ValueError as e:
        logger.error(f"F1 data validation error: {e}")
        return handle_invalid_f1_data(kaggle_csv_path, e)
    
    except Exception as e:
        logger.error(f"F1 pipeline failed unexpectedly: {e}")
        return handle_f1_processing_failure(kaggle_csv_path, e)

def handle_missing_f1_data():
    """Fallback strategy when F1 dataset is missing"""
    logger.info("Attempting to download F1 dataset from Kaggle...")
    # In practice, implement automatic download or use backup dataset
    return create_minimal_f1_dataset()

def handle_invalid_f1_data(csv_path, error):
    """Handle invalid F1 data by cleaning and retrying"""
    logger.info(f"Attempting to clean F1 data due to: {error}")
    
    try:
        # Try to clean the data
        df = pd.read_csv(csv_path, encoding='utf-8', errors='ignore')
        df = df.dropna(subset=['Driver', 'Team', 'Grand Prix'])
        
        # Save cleaned version
        cleaned_path = csv_path.replace('.csv', '_cleaned.csv')
        df.to_csv(cleaned_path, index=False)
        
        return robust_f1_processing_pipeline(cleaned_path)
    except Exception as cleanup_error:
        logger.error(f"F1 data cleanup also failed: {cleanup_error}")
        return create_minimal_f1_dataset()

def create_minimal_f1_dataset():
    """Create a minimal F1 dataset for testing/fallback"""
    minimal_data = {
        'Year': [2023, 2023, 2023],
        'Grand Prix': ['Monaco Grand Prix', 'British Grand Prix', 'Italian Grand Prix'],
        'Driver': ['Max Verstappen', 'Lewis Hamilton', 'Charles Leclerc'],
        'Team': ['Red Bull Racing', 'Mercedes', 'Ferrari']
    }
    return daft.from_pandas(pd.DataFrame(minimal_data))

# Usage with comprehensive error handling
try:
    f1_processed = robust_f1_processing_pipeline("formula-1-winners.csv")
    print(f"Successfully processed F1 data: {f1_processed.count().compute()} records")
except Exception as e:
    print(f"F1 processing completely failed: {e}")
```

## Conclusion

The modern data stack enables powerful multimodal data processing pipelines that bridge traditional analytics and AI workloads. By combining:

- **Parquet** for efficient data storage
- **DuckDB** for fast analytics
- **LanceDB** for vector operations
- **Daft** for multimodal processing  
- **Databricks** for enterprise scale

You can build robust, scalable systems that handle the complexity of modern data applications.

### Next Steps

1. **Experiment**: Try the examples with your own data
2. **Optimize**: Profile and tune your specific workloads
3. **Scale**: Gradually increase data size and complexity
4. **Monitor**: Implement proper observability and alerting
5. **Iterate**: Continuously improve based on usage patterns

### Resources

- [Daft Documentation](https://docs.daft.ai/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Databricks Documentation](https://docs.databricks.com/)

---

*This tutorial provides a foundation for building modern multimodal data pipelines. The landscape evolves rapidly, so stay updated with the latest developments in each tool.*

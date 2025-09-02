# Modern Multimodal Data Processing: From Parquet to AI Workloads

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
-- Clickstream analysis example
SELECT user_id, COUNT(*) as clicks
FROM clickstream_data 
WHERE date >= '2024-01-01'
GROUP BY user_id;
```

### Modern Multimodal Workloads
Today's applications require processing diverse data types:
- Images and videos for computer vision
- Text documents for NLP
- Audio files for speech processing
- Embeddings for similarity search

## Parquet Files: The Foundation

Parquet has become the de facto standard for analytical workloads, replacing CSV for several key reasons:

### Why Parquet Over CSV?

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# CSV limitations
df_csv = pd.read_csv('large_dataset.csv')  # Slow, no schema, large size

# Parquet benefits
df_parquet = pd.read_parquet('large_dataset.parquet')  # Fast, typed, compressed
```

### Key Advantages:
1. **Columnar Storage**: Better compression and query performance
2. **Schema Evolution**: Built-in type system
3. **Predicate Pushdown**: Skip irrelevant data
4. **Compression**: Significantly smaller file sizes

### Working with Parquet

```python
# Creating Parquet files
data = {
    'id': range(1000),
    'filename': [f'image_{i}.jpg' for i in range(1000)],
    'embedding': [[0.1, 0.2, 0.3] for _ in range(1000)]
}
df = pd.DataFrame(data)
df.to_parquet('multimodal_data.parquet')

# Reading with filters
filtered_df = pd.read_parquet(
    'multimodal_data.parquet',
    filters=[('id', '>', 500)]
)
```

## DuckDB: Analytics Made Simple

DuckDB provides a lightweight, embedded analytical database that excels at processing Parquet files.

### Getting Started

```python
import duckdb

# Connect to DuckDB (in-memory or persistent)
conn = duckdb.connect('analytics.duckdb')

# Query Parquet files directly
result = conn.execute("""
    SELECT id, filename, COUNT(*)
    FROM 'multimodal_data.parquet'
    WHERE id > 100
    GROUP BY id, filename
    ORDER BY COUNT(*) DESC
""").fetchall()
```

### Advanced Features

#### Object Storage Integration
```sql
-- Query files directly from S3
SELECT * FROM 's3://bucket/data/*.parquet'
WHERE created_date >= '2024-01-01';
```

#### Apache Iceberg Support
DuckDB integrates with Iceberg for transactional semantics:

```python
# Iceberg table operations
conn.execute("""
    CREATE TABLE iceberg_table AS
    SELECT * FROM iceberg_scan('s3://bucket/iceberg-table/')
""")
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

# Create/connect to LanceDB
db = lancedb.connect("./lance-data")

# Sample multimodal data
data = [
    {
        "id": 1,
        "filename": "image1.jpg",
        "vector": np.random.random(768).tolist(),
        "metadata": {"category": "person", "confidence": 0.95}
    },
    {
        "id": 2, 
        "filename": "video1.mp4",
        "vector": np.random.random(768).tolist(),
        "metadata": {"category": "landscape", "confidence": 0.87}
    }
]

# Create table
table = db.create_table("multimodal_embeddings", data)
```

### Vector Search Operations

```python
# Similarity search
query_vector = np.random.random(768).tolist()

results = table.search(query_vector).limit(10).to_list()
print(f"Found {len(results)} similar items")

# Filtered vector search
filtered_results = table.search(query_vector)\
    .where("metadata.confidence > 0.9")\
    .limit(5)\
    .to_list()
```

### Use Cases
- **Video similarity**: Find similar video content
- **Image search**: Content-based image retrieval  
- **Document search**: Semantic text search
- **Recommendation systems**: User/item similarity

## Daft: Multimodal Data Processing

Daft serves as the compute layer bridging data and AI workloads.

### Installation

```bash
pip install getdaft
```

### Basic Operations

```python
import daft

# Read multimodal data
df = daft.read_parquet("multimodal_data.parquet")

# Process images
df_with_images = df.with_column(
    "image_data", 
    df["filename"].apply(lambda x: load_image(x), return_dtype=daft.DataType.python())
)

# Extract features
df_processed = df_with_images.with_column(
    "features",
    df_with_images["image_data"].apply(extract_features, return_dtype=daft.DataType.tensor(daft.DataType.float32()))
)
```

### Advanced Multimodal Operations

```python
# Video processing pipeline
video_df = daft.read_parquet("videos.parquet")

# Extract frames
frames_df = video_df.with_column(
    "frames",
    video_df["video_path"].apply(extract_frames)
)

# Generate embeddings
embeddings_df = frames_df.with_column(
    "embeddings", 
    frames_df["frames"].apply(generate_video_embeddings)
)

# Save to LanceDB
embeddings_df.write_lance("video_embeddings")
```

### Key Features
- **Lazy evaluation**: Efficient processing of large datasets
- **Multimodal support**: Native handling of images, video, text
- **Distributed compute**: Scale across multiple machines
- **Integration**: Works seamlessly with other tools in the stack

## Databricks: Enterprise Scale

Databricks provides the enterprise platform for large-scale multimodal data processing.

### ACID Transactions

Databricks implements ACID properties for reliable data operations:

```sql
-- Atomic operations
BEGIN TRANSACTION;
    INSERT INTO user_embeddings SELECT * FROM new_embeddings;
    UPDATE user_profiles SET last_updated = current_timestamp();
COMMIT;
```

### Delta Lake Integration

```python
from delta import *
from pyspark.sql import SparkSession

# Configure Spark for Delta Lake
spark = configure_spark_with_delta_pip(SparkSession.builder).getOrCreate()

# Read Delta table
df = spark.read.format("delta").load("/path/to/delta-table")

# Time travel
historical_df = spark.read.format("delta")\
    .option("timestampAsOf", "2024-01-01")\
    .load("/path/to/delta-table")
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
-- Z-ordering for better file skipping
OPTIMIZE table_name ZORDER BY (user_id, timestamp);

-- Vacuum old files
VACUUM table_name RETAIN 168 HOURS;
```

## Building a Complete Pipeline

Let's build an end-to-end multimodal data pipeline:

### Step 1: Data Ingestion

```python
import daft
import lancedb
import duckdb

# Ingest multimodal data
def ingest_data(source_path):
    # Read raw data with Daft
    df = daft.read_json(f"{source_path}/*.json")
    
    # Process images
    df = df.with_column(
        "image_embedding",
        df["image_path"].apply(generate_image_embedding)
    )
    
    # Process text
    df = df.with_column(
        "text_embedding", 
        df["description"].apply(generate_text_embedding)
    )
    
    return df

# Process the data
processed_df = ingest_data("raw_data/")
```

### Step 2: Storage and Indexing

```python
# Save to Parquet for analytical queries
processed_df.write_parquet("processed_data.parquet")

# Index vectors in LanceDB
db = lancedb.connect("./vector_db")
lance_data = processed_df.select([
    "id", "image_embedding", "text_embedding", "metadata"
]).to_pyarrow_table()

table = db.create_table("multimodal_index", lance_data)
```

### Step 3: Analytics with DuckDB

```python
conn = duckdb.connect("analytics.duckdb")

# Analytical queries
results = conn.execute("""
    SELECT 
        category,
        COUNT(*) as count,
        AVG(confidence_score) as avg_confidence
    FROM 'processed_data.parquet'
    GROUP BY category
    ORDER BY count DESC
""").fetchall()
```

### Step 4: Vector Search

```python
# Find similar content
def find_similar(query_embedding, limit=10):
    return table.search(query_embedding)\
        .limit(limit)\
        .to_pandas()

# Example usage
similar_items = find_similar(sample_embedding)
```

## Performance Optimization

### File Skipping Strategies

1. **Partition your data**: Organize by frequently queried columns
2. **Use appropriate file sizes**: 100MB-1GB per file
3. **Implement predicate pushdown**: Filter at the storage level

### Query Optimization

```python
# Efficient filtering
df_filtered = daft.read_parquet("data.parquet")\
    .where(daft.col("timestamp") >= "2024-01-01")\
    .where(daft.col("category") == "important")

# Lazy evaluation
df_processed = df_filtered.select(["id", "embedding"])\
    .with_column("similarity", compute_similarity(daft.col("embedding")))
```

### Memory Management

```python
# Process in batches
def process_large_dataset(file_path, batch_size=1000):
    df = daft.read_parquet(file_path)
    
    for batch in df.iter_partitions(batch_size):
        processed_batch = process_batch(batch)
        yield processed_batch
```

## Real-World Use Cases

### 1. Content Recommendation System

```python
# Build a recommendation system
def build_recommendations(user_id):
    # Get user's interaction history
    user_history = conn.execute("""
        SELECT item_id, interaction_type, timestamp
        FROM user_interactions 
        WHERE user_id = ?
        ORDER BY timestamp DESC
        LIMIT 50
    """, [user_id]).fetchall()
    
    # Get item embeddings
    item_embeddings = [get_embedding(item[0]) for item in user_history]
    
    # Find similar items
    recommendations = []
    for embedding in item_embeddings:
        similar = table.search(embedding).limit(5).to_list()
        recommendations.extend(similar)
    
    return deduplicate_recommendations(recommendations)
```

### 2. Multimodal Search Engine

```python
def multimodal_search(query_text=None, query_image=None):
    results = []
    
    if query_text:
        text_embedding = generate_text_embedding(query_text)
        text_results = table.search(text_embedding, vector_column="text_embedding")
        results.extend(text_results.to_list())
    
    if query_image:
        image_embedding = generate_image_embedding(query_image)
        image_results = table.search(image_embedding, vector_column="image_embedding")
        results.extend(image_results.to_list())
    
    return merge_and_rank_results(results)
```

### 3. Dataset Preparation for LLMs

```python
def prepare_llm_dataset(source_data):
    # Process with Daft
    df = daft.read_parquet(source_data)
    
    # Clean and filter
    df_clean = df.where(daft.col("quality_score") > 0.8)\
        .where(daft.col("text_length") > 100)
    
    # Generate embeddings for deduplication
    df_embedded = df_clean.with_column(
        "embedding",
        df_clean["text"].apply(generate_embedding)
    )
    
    # Deduplicate using vector similarity
    deduplicated_df = deduplicate_by_similarity(df_embedded)
    
    # Save in format suitable for training
    deduplicated_df.write_parquet("llm_training_data.parquet")
    
    return deduplicated_df
```

## Best Practices

### Data Architecture
1. **Separate storage and compute**: Use object storage for data, compute engines for processing
2. **Implement proper data governance**: Track lineage, quality, and access
3. **Version your data**: Use tools like DVC or Delta Lake for versioning
4. **Monitor performance**: Track query performance and resource usage

### Code Organization
```python
# Project structure
multimodal_pipeline/
├── data_ingestion/
│   ├── parsers.py
│   └── validators.py
├── processing/
│   ├── embeddings.py
│   └── transformers.py
├── storage/
│   ├── parquet_ops.py
│   └── vector_ops.py
└── analytics/
    ├── queries.py
    └── visualizations.py
```

### Error Handling
```python
def robust_processing_pipeline(data_path):
    try:
        # Process data with proper error handling
        df = daft.read_parquet(data_path)
        
        # Validate data quality
        if df.count().compute() == 0:
            raise ValueError("Empty dataset")
        
        # Process with error recovery
        processed_df = df.with_column(
            "processed",
            df["raw_data"].apply(safe_process_function)
        ).where(daft.col("processed").is_not_null())
        
        return processed_df
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        # Implement fallback strategy
        return handle_processing_failure(data_path, e)
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

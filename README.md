# Mutual Friend Recommender

A PySpark-based implementation of a friend recommendation system using MapReduce-style logic. The model analyzes a social network graph and suggests new friends based on the number of mutual friends shared.

## Features
- Built using Apache Spark RDDs
- Efficiently computes mutual friend counts
- Recommends top 10 non-friends for each user
- Designed for large-scale social graph datasets

## Dataset
- Source: [LiveJournal Social Network](https://an-ml.s3.us-west-1.amazonaws.com/soc-LiveJournal1Adj.txt)
- Format:  
  ```
  UserID<TAB>UserID1,UserID2,...
  ```

## How It Works
1. Parse the graph into `(user, friends)` pairs.
2. Generate all potential friend-of-friend connections.
3. Count mutual friends between users not already connected.
4. Recommend top 10 based on mutual count.

## Requirements
- Python 3.x
- PySpark

## Running the Code
Use this inside a Databricks notebook or Spark cluster:
```python
data = sc.textFile("path/to/soc-LiveJournal1Adj.txt")
# run mutual friend logic...
```

## Output Example
```
UserID    RecommendedUser1,RecommendedUser2,... up to 10
```

## License
MIT License

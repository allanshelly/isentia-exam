## News Ingest Pipeline

#Overview

This challenge will showcase the candidate&#39;s ability to integrate external APIs, manage
dependencies, utilise cloud services (AWS Kinesis), and containerise an application using
Docker. The task is to create a robust Python script that periodically fetches news articles from
a public API and streams the structured data into an AWS Kinesis stream.
Scenario: Aurora Analytics
Aurora Analytics, a fast-growing media intelligence firm, relies heavily on real-time news and
market sentiment data. The ability to quickly integrate new data sources is critical to the
company’s success.  
NewsAPI.org, a premium global news aggregator, has just provided Aurora Analytics with API
access to their comprehensive feed. This is a critical opportunity to enrich Aurora&#39;s analytical
models with high-fidelity, high-volume news data.  
The immediate need is to build a modern, scalable, and resilient ingestion service. This service
must securely connect to the NewsAPI feed, reliably process the incoming articles, and stream
the raw, structured data onto a dedicated AWS Kinesis Data Stream for downstream processing
by Aurora&#39;s machine learning and sentiment analysis engines. The content is a real-time
stream, therefore the system should regularly pull content from the API and optimise to reduce
latency when delivering to clients.  
# Core Requirements  
1. API Integration: Fetch news data from the public Everything API from NewsAPI.
2. Data Processing: Structure and validate the retrieved data.
3. Kinesis Integration: Write the processed data to a configured AWS Kinesis Data Stream.
4. Containerization: Package the application and its dependencies using Docker.
   
# Technical Requirements
The ingested article data written to Kinesis should be a JSON object containing at least the
following fields (after being extracted and cleaned from the API response):  
● article_id (Unique identifier)  
● source_name  
● title  
● content  
● Url  
● author  
● published_at  
● ingested_at  

# Deliverables
The candidate should submit the URL of a public Git repository (e.g., GitHub, GitLab) containing
at a minimum the following:
1. Python code - The main application logic
2. Dockerfile - An executable Docker container
3. README.md

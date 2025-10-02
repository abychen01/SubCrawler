# Reddit Data Pipeline Project

[![GitHub Repo](https://img.shields.io/badge/GitHub-Repo-blue?logo=github)](https://github.com/abychen01/currency_exchange_analysis)
[![Python Version](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/downloads/)
[![Spark Version](https://img.shields.io/badge/Spark-3.x-orange)](https://spark.apache.org/)

## Overview
This project implements an ETL pipeline using Azure Synapse Analytics (or a similar Spark environment) to scrape Reddit data based on configurable subreddits and keywords. Data is ingested from Reddit, processed through Bronze (raw), Silver (transformed), and Gold (enriched with sentiment) layers using Delta tables. The processed data is synced to a SQL Server database for external use (e.g., Power BI reporting), and email notifications are sent via Mailjet for initial loads or updates. Configurations for subreddits, keywords, and recipient emails are fetched from CSV files stored in Google Drive (exported from Google Sheets). There are three sheets/CSVs: subreddits, keywords, and email. The pipeline supports any number of subreddits and keywords added to these sheets, searching all specified subreddits for all keywords. When all three sheets are added (i.e., first complete setup), it performs a full search and sends an initial email. Subsequent runs detect updates (new posts/comments or config changes) and send notifications accordingly.

> **Note:** Email notifications may be delivered to the Spam or Junk folder depending on your mail provider. Please check these folders if you do not see the alert in your inbox.
>
> 

<img width="1414" alt="image" src="https://github.com/abychen01/no_name_project/blob/436dcde12cb6e0ec02b7825592c6e5414ae6666d/ETL.png" />
(The screenshot shows the pipeline flow: Notebook (Bronze) → Notebook (Silver) → Notebook (Gold) → Notebook (SQL Server sync) → Notebook (Email).

## Project Details

- **Data Source**: Reddit via PRAW API, filtered by subreddits and keywords from sheets/CSVs.
- **Configurations**: 
  - `subreddits.csv` (from sheet): List of subreddits to search.  [link](https://docs.google.com/spreadsheets/d/12pCopaJID0R5iqkc0Lsb4pKmDYaOKlg_3WARdO0SxEM/edit?usp=sharing)
    
    <img width="1000" alt="image" src="https://github.com/abychen01/no_name_project/blob/4555c283ce45b07640780f1b01fa315e6a2904a6/subreddits%20sheet.png" />
  - `keywords.csv` (from sheet): List of keywords for filtering posts and comments.  [link](https://docs.google.com/spreadsheets/d/1mT0LXCjuUCIPrglGPMCpD9dWZ-tLn-aQ19z0xLb2mjU/edit?usp=sharing)
  
    <img width="1000" alt="image" src="https://github.com/abychen01/no_name_project/blob/4555c283ce45b07640780f1b01fa315e6a2904a6/keywords%20sheet.png" />
  - `email.csv` (from sheet): Recipient email for notifications.   [link](https://docs.google.com/spreadsheets/d/1Z_Roi1VfweDt8VglA7tMI4qLgQj1KSD8rlMrDlJiPpU/edit?usp=sharing)
    
    <img width="1000" alt="image" src="https://github.com/abychen01/no_name_project/blob/4555c283ce45b07640780f1b01fa315e6a2904a6/email%20sheet.png" />
- **Processing**: Ingest raw data, clean/transform (e.g., deduplication, timestamps, URLs), add sentiment analysis using TextBlob.
- **Storage**: Delta tables in lakehouses (Bronze, Silver, Gold layers).
- **Sync**: Export to SQL Server database.
- **Notifications**: Emails with summaries of new posts/comments and a Power BI link, sent on first run (when all three sheets are present) or updates.
- **Tools**: PySpark, PRAW, TextBlob, Google Drive API, pyodbc, Mailjet.

The pipeline detects changes in sheets/CSVs and Reddit data for incremental processing. When all three sheets are added/updated, it performs a full search across all subreddits and keywords, sending an initial email. Subsequent runs check for new content and notify accordingly.

## Notebooks

The pipeline consists of five Jupyter notebooks (`.ipynb` files) run sequentially.

### 1. Bronze Nb.ipynb (Bronze Layer: Data Ingestion)
   - **Purpose**: Fetches configurations from Google Drive CSVs (from sheets) and ingests raw Reddit data.
   - **Key Steps**:
     - Downloads `subreddits.csv`, `keywords.csv`, and `email.csv` from Google Drive using service account credentials.
     - Compares downloaded CSVs with existing lakehouse versions to detect changes (sets flags like `same_sub`, `same_key`).
     - Authenticates with Reddit via PRAW using stored credentials.
     - Scrapes submissions (posts) and comments (direct keyword matches and indirect via post replies) from all specified subreddits, filtering by all keywords.
     - Handles incremental updates if configurations are unchanged; otherwise, performs full refresh.
     - Saves raw data to Delta tables: `submissions`, `comments_direct`, `comments_indirect`.
   - **Inputs**: Google Drive folder ID, service account JSON, Reddit credentials.
   - **Outputs**: Bronze lakehouse tables with raw data.
   - **Dependencies**: `praw`, `googleapiclient`, `delta.tables`.
   - **Notes**: Supports any number of subreddits/keywords from sheets. Includes rate-limiting sleeps and schema definitions.

### 2. Silver Nb-.ipynb (Silver Layer: Data Transformation)
   - **Purpose**: Cleans and transforms raw data from Bronze.
   - **Key Steps**:
     - Reads Bronze tables: `comments_direct`, `comments_indirect`, `submissions`.
     - Unions comments, deduplicates, filters out deleted content.
     - Generates Reddit URLs for posts and comments.
     - Converts Unix timestamps to UTC and EST.
     - Saves to Silver Delta tables: `comments`, `submissions`.
   - **Inputs**: Bronze table names (parameters: `b_comments_direct`, etc.).
   - **Outputs**: Silver lakehouse tables.
   - **Dependencies**: PySpark timestamp functions.
   - **Notes**: Overwrites schema as needed.

### 3. Gold Nb-.ipynb (Gold Layer: Data Enrichment)
   - **Purpose**: Adds analytics to Silver data.
   - **Key Steps**:
     - Reads Silver tables: `comments`, `submissions`.
     - Computes sentiment scores using TextBlob on comment bodies and post titles.
     - Adds columns: `comment_sentiment`, `title_sentiment`.
     - Saves to Gold Delta tables: `comments`, `submissions`.
   - **Inputs**: Silver table names (parameters: `s_comments`, etc.).
   - **Outputs**: Gold lakehouse tables.
   - **Dependencies**: `textblob` UDF.
   - **Notes**: Overwrites existing tables.

### 4. SQL Server sync.ipynb (Data Sync to SQL Server)
   - **Purpose**: Syncs Gold data to SQL Server.
   - **Key Steps**:
     - Loads credentials from Delta table.
     - Connects via pyodbc/JDBC; creates database/tables if needed.
     - Maps Spark types to SQL (e.g., string to NVARCHAR).
     - Writes data to SQL tables (`submissions`, `comments`) in overwrite mode.
   - **Inputs**: Gold data (via Spark reads).
   - **Outputs**: SQL Server tables in `no_name_project` database.
   - **Dependencies**: `pyodbc`, JDBC driver.
   - **Notes**: Handles schema creation; originally for AWS RDS.

### 5. email.ipynb (Notification Layer)
   - **Purpose**: Sends update notifications.
   - **Key Steps**:
     - Loads Mailjet credentials and recipient from CSVs/Delta.
     - Uses Delta history to detect new posts/comments.
     - Formats HTML email with counts, new content lists, and Power BI link.
     - Sends via Mailjet on first run (when configurations are new/complete) or updates.
   - **Inputs**: Flags like `same_sub`, `same_key`; tables `submissions`, `comments_indirect`.
   - **Outputs**: Email to recipient.
   - **Dependencies**: `mailjet_rest`, Delta history.
   - **Notes**: Triggers based on changes; handles initial vs. update scenarios, including when all three sheets are first added.

## Setup and Running

1. **Prerequisites**:
   - Spark environment with lakehouses (Bronze_LH, etc.).
   - Libraries: `praw`, `googleapiclient`, `textblob`, `mailjet_rest`, `pyodbc`.
   - Credentials: Delta tables for Reddit/Mailjet; service account JSON for Drive.
   - Google Drive: CSVs exported from sheets for subreddits, keywords, email (add any number to scale).
   - SQL Server: Accessible instance.

2. **Configuration**:
   - Update Drive IDs and parameters.
   - Add subreddits/keywords to sheets for dynamic searching.

3. **Running**:
   - Use pipeline to run notebooks in order.
   - First run with all sheets/CSVs: Full search and email.
   - Later runs: Incremental updates and notifications.

## Code Comments

Each notebook includes detailed comments explaining the purpose of each code cell, enhancing readability and maintainability.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for suggestions or improvements.

## License

This project does not use a specific open-source license and is intended for educational and non-commercial purposes only.

## Contact
- Repo Owner: [abychen01](https://github.com/abychen01)
- For questions: Open an issue.

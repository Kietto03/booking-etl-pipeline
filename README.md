Project Name: Automated Hotel Data ETL Pipeline
1. Introduction
This project is an end-to-end Data Engineering pipeline designed to monitor hotel availability and pricing in real-time. It automates the extraction of data from Booking.com, transforms raw data, and loads it into a Data Warehouse (PostgreSQL) with historical tracking capabilities.

Goal: To track room availability changes (Available vs. Booked) and price fluctuations over time for data analysis.

2. System Architecture
(Chèn ảnh sơ đồ hệ thống tại đây - Bạn có thể chụp màn hình sơ đồ Mermaid tôi vẽ ở bước trước)

3. Tech Stack
Orchestration: Apache Airflow 2.x

Language: Python 3.12 (Pandas, SQLAlchemy)

Web Scraping: Playwright (Handling dynamic content & anti-bot mechanisms)

Database: PostgreSQL

Concepts: ETL, SCD (Slowly Changing Dimensions), Data Warehousing, Logging.

4. Key Features
Automated Scheduling: The pipeline runs daily at 08:30 AM via Airflow Scheduler.

Anti-Bot Crawling: Uses Playwright with masked User-Agents and dynamic delays to bypass Booking.com's security.

Slowly Changing Dimensions (SCD): Implemented logic to track history:

Updates last_available_date if the room is still available.

Marks status as NOT (Booked) if the room disappears from the scan.

Performance Optimization:

Blocked media resources (images/fonts) to speed up crawling by 3x.

Used SQL Stored Procedures for bulk data processing instead of row-by-row Python loops.

Operational Logging: Custom logging table (etl_logs) to track the number of inserts, updates, and failures for each run.

5. Database Schema
booking_staging: Raw data from daily scans (Truncated daily).

booking_master: Historical data with first_seen, last_available_date, and status.

etl_logs: Audit trail for monitoring pipeline health.

6. How to Run
Setup Environment:

Bash
pip install -r requirements.txt
playwright install chromium
Database Init:
Run scripts in sql/ folder to create tables and procedures.

Airflow Config:
Update airflow.cfg to point to the project dags folder.

Run:

Bash
airflow standalone

# Description
Project designed to pull in energy grid based data and store it in a postgres relational database for analysis. 

# Phase 1 - NYISO data
Use requests library to pull in data from NYISO in the form of CSVs and store in the database.  
Use basic grids to view data. Expand functionality of grids.

# Phase 2 - Celery background processing
Implement Celery library to move requests to background processes. Build 'cron' jobs for more up to date processing.

# Phase 3 - Graphing library
Implement graphing library to frontend (in core pvrd_framework) and add screens to module UI.
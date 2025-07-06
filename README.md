Designed and implemented a complete data cleaning pipeline using SQL to prepare a raw dataset of global company layoffs for analysis.

Removed duplicate records using ROW_NUMBER() with PARTITION BY on key fields such as company, industry, and date.

Standardized data entries by trimming whitespace and correcting inconsistent values (e.g., industry and country names).

Converted text-based date fields to proper DATE format to enable time-based trend analysis.

Handled missing values by replacing blank entries with NULL and imputing industry fields using matched company and location data.

Deleted records lacking both layoff counts and percentages, ensuring dataset integrity.

Created analytical queries to summarize layoffs by company, country, industry, and time (year/month), including rolling totals using window functions.

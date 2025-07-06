-- Data Cleaning



-- 1. Remove Duplicates
-- 2. Standardize thr Data
-- 3. Null Values or blank values
-- 4. Remove any columns


-- View all data in the layoffs table
SELECT *
FROM layoffs;

-- Create a copy of the original layoffs table structure
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;
-- Copy data from layoffs into the new staging table
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Check contents of the staging table
SELECT *
FROM layoffs;
-- 1.  Remove Duplicates
-- Identify potential duplicate rows using ROW_NUMBER for deduplication
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Remove Duplicates using CTE
 WITH duplicate_cte AS(
   SELECT *,
   ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
   country, funds_raised_millions ) AS row_num
   FROM layoffs_staging
   )
   SELECT *
FROM duplicate_cte
WHERE row_num < 2;   


-- Create a cleaned version of the data with additional row_num column
  CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Insert deduplicated data into the new staging2 table
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
   country, funds_raised_millions ) AS row_num
   FROM layoffs_staging;
   
-- Delete rows that are duplicates (row_num > 1)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

  -- 2. Standardizing data
  -- Standardize company name by trimming whitespace
  SELECT company, TRIM(company)
  FROM layoffs_staging2;

UPDATE layoffs_staging2
  SET company = TRIM(company);
   
  SELECT  DISTINCT industry
  FROM layoffs_staging2
  ;
  -- Fix inconsistent industry name for 'Crypto'
  UPDATE layoffs_staging2
  SET industry = 'Crypto'
  WHERE industry LIKE 'Crypto';

   -- Standardize country names by removing trailing periods
   SELECT  DISTINCT country, TRIM(TRAILING '.' FROM country)
  FROM layoffs_staging2
   ORDER BY 1;
   
   UPDATE layoffs_staging2
   SET country = TRIM(TRAILING '.' FROM country)
   WHERE country LIKE 'United States%';
   
   SELECT `date`layoffs_staging2
   FROM layoffs_staging2;
   
   -- Convert the date format from text to DATE type
   UPDATE layoffs_staging2
   SET `date` =  STR_TO_DATE (`date`, '%m/%d/%Y' );
   
   ALTER TABLE layoffs_staging2
   MODIFY COLUMN `date` DATE;
   
   
   -- 3. Null Values or blank values
   SELECT *
   FROM layoffs_staging2
   WHERE total_laid_off IS NULL
   AND percentage_laid_off IS NULL;
   
   UPDATE layoffs_staging2 
   SET industry = null
   WHERE industry = '';
   
   -- Find rows with missing industry info
   SELECT *
   from layoffs_staging2
   WHERE industry IS NULL
   OR industry = '';
   
   
   SELECT *
   from layoffs_staging2
   WHERE company = 'Airbnb';
   
   -- Impute missing industry values using matching company + location
SELECT * 
FROM layoffs_staging2 t1
join layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '');

UPDATE layoffs_staging2 t1
join layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;
   
-- 4. Remove any columns

     SELECT *
     FROM layoffs_staging2
     WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
   
-- 4. Remove rows with NULL values in both total_laid_off and percentage_laid_off
      DELETE
      FROM layoffs_staging2
      WHERE total_laid_off IS NULL
      AND percentage_laid_off IS NULL;
   
   SELECT *
   FROM layoffs_staging2;
   
-- Drop the helper column after cleaning
   ALTER TABLE layoffs_staging2
   DROP COLUMN row_num;
   
-- Final cleaned dataset
   SELECT * 
   FROM layoffs_staging2;

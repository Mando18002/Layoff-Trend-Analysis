-- Data Cleaning
-- 1) Remove Duplicates
-- 2) Standardize the Data
-- 3) Null or Blank Values
-- 4) Remove Columns or Rows if Needed

-- REMOVING DUPLICATES
SELECT * 
FROM practice1.layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM practice1.layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM practice1.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM practice1.layoffs_staging
WHERE company = "Casper";

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
FROM practice1.layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM practice1.layoffs_staging;

SELECT * 
FROM practice1.layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM practice1.layoffs_staging2
WHERE row_num > 1;

-- STANDARDIZING DATA
SELECT company
FROM practice1.layoffs_staging2;

UPDATE practice1.layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT INDUSTRY
FROM practice1.layoffs_staging2
ORDER BY INDUSTRY ASC;

SELECT *
FROM practice1.layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE practice1.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM practice1.layoffs_staging2;

UPDATE practice1.layoffs_staging2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE "United States%";

SELECT DISTINCT country
FROM practice1.layoffs_staging2
ORDER BY country ASC;

SELECT `date`,
STR_TO_DATE(`date`, "%m/%d/%Y")
FROM practice1.layoffs_staging2;

UPDATE practice1.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

SELECT `date`
FROM practice1.layoffs_staging2;

ALTER TABLE practice1.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- BLANKS & NULLS
SELECT *
FROM practice1.layoffs_staging2
WHERE industry IS NULL
OR industry = "";

SELECT *
FROM practice1.layoffs_staging2
WHERE company = "Airbnb";

SELECT t1.industry, t2.industry
FROM practice1.layoffs_staging2 t1
JOIN practice1.layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = "")
AND t2.industry IS NOT NULL AND t2.industry <> "";

UPDATE practice1.layoffs_staging2 t1
JOIN practice1.layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = "")
AND t2.industry IS NOT NULL AND t2.industry <> "";

-- REMOVING COLUMNS AND ROWS THAT DONT MAKE SENSE TO INCLUDE
SELECT *
FROM practice1.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM practice1.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE practice1.layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM practice1.layoffs_staging2;

-- EXPLORATORY DATA ANALYSIS (EDA PROCESS)
SELECT * 
FROM practice1.layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM practice1.layoffs_staging2;

SELECT company, total_laid_off
FROM practice1.layoffs_staging2
WHERE total_laid_off = (SELECT MAX(total_laid_off) FROM practice1.layoffs_staging2);

SELECT company, SUM(total_laid_off) AS total_layoffs
FROM practice1.layoffs_staging2
GROUP BY company
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_layoffs DESC;

SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM practice1.layoffs_staging2
GROUP BY industry
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_layoffs DESC;

SELECT MIN(`date`), MAX(`date`)
FROM practice1.layoffs_staging2;

SELECT YEAR(`date`), SUM(total_laid_off) AS time_lay
FROM practice1.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY time_lay DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_layoffs
FROM practice1.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

WITH Rolling_Total AS
(SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM practice1.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS Rolling_Total
FROM Rolling_Total;

SELECT company, SUM(total_laid_off) AS total_layoffs
FROM practice1.layoffs_staging2
GROUP BY company
ORDER BY total_layoffs DESC;    

SELECT company, YEAR(`date`), SUM(total_laid_off) AS total_layoffs
FROM practice1.layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY total_layoffs DESC;

WITH Company_Year (company, years, total_laid_off) AS
(SELECT company, YEAR(`date`), SUM(total_laid_off) AS total_layoffs
FROM practice1.layoffs_staging2
GROUP BY company, YEAR(`date`)), Company_Year_Rank AS(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;







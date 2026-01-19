-- Exploratory Data Analysis

SELECT * 
FROM layoffs_staging2;

SELECT max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
where percentage_laid_off = 1
order by 9 desc
;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
order by 2 desc
;

select min(`date`), max(`date`) 
from layoffs_staging2;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
;

SELECT * 
FROM layoffs_staging2
;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY year(`date`)
ORDER BY 1 DESC
;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc
;


select substring(`date`,1,7) as `Month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `Month`
order by 1
;


with Rolling_Total as
(
select substring(`date`,1,7) as `Month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `Month`
order by 1
)
select `Month`,total_off, sum(total_off) over(order by `Month` ) as rolling_total
from Rolling_Total;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
order by 2 desc
;

SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
order by 3 desc
;

with Company_Year (company, Years, total_laid_off)as
(
SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
), company_Year_Rank as
(select *, dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from Company_Year
where years is not null
)
select *
from company_Year_Rank
where Ranking <= 5
;


































































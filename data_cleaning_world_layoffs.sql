select *from layoffs;

SET SQL_SAFE_UPDATES = 0;
-- 1.Remove Duplicates
-- 2. Standardize the data 
-- 3. Null Values or Blank Values 
-- 4. Remove any columns 

-- create table layoffs_staging 
-- like layoffs;
select *from layoffs_staging;
insert into layoffs_staging select * from layoffs;
select *from layoffs_staging;


select *,
row_number()
OVER(
partition by company, location ,industry, total_laid_off, percentage_laid_off, `date`, stage , country, funds_raised_millions)  as row_num 
from layoffs_staging;


with duplicates_cte as 
(
select *,
row_number()
OVER(
partition by company, location ,industry, total_laid_off, percentage_laid_off, `date`, stage , country, funds_raised_millions) as row_num
from layoffs_staging
) 
select * from duplicates_cte where row_num>1;

select * from layoffs_staging where company ='Casper';


with duplicates_cte as 
(
select *,
row_number()
OVER(
partition by company, location ,industry, total_laid_off, percentage_laid_off, `date`, stage , country, funds_raised_millions) as row_num
from layoffs_staging
) 
delete from duplicates_cte where row_num>1; # we can not delete like this 





CREATE TABLE `layoffs_staging3` (
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



select * from layoffs_staging3;


insert into layoffs_staging3
select *,
row_number()
OVER(
partition by company, location ,industry, total_laid_off, percentage_laid_off, `date`, stage , country, funds_raised_millions) as row_num
from layoffs_staging;



select * from layoffs_staging3 where row_num>1;


DELETE  from layoffs_staging3 where row_num>1;

select * from layoffs_staging3 where row_num>1;

select * from layoffs_staging3 ;


-- standardizing data 
select  (Trim(company)) from layoffs_staging3;


update layoffs_staging3
set company=trim(company);

select  company from layoffs_staging3;


select distinct(industry) from layoffs_staging3
order by industry;


select distinct(industry) from layoffs_staging3
order by industry;

select *from layoffs_staging3 where industry like 'Crypto%';


update layoffs_staging3
set industry='Crypto'
where industry like 'Crypto%';


select distinct(location) from layoffs_staging3
order by location ;

select distinct(country) from layoffs_staging3
order by country;


select distinct(country), trim(trailing '.' from country) as country_stand
from layoffs_staging3 order by country;


update layoffs_staging3
set country=trim(trailing '.' from country)
where country like 'United States%';






select `date`, 
str_to_date(`date`, '%m/%d/%Y')
 from layoffs_staging3;
 
 
 update  layoffs_staging3
 set `date`= str_to_date(`date`, '%m/%d/%Y');


alter table layoffs_staging3 modify column `date` DATE ;


-- working with non values 


select * from layoffs_staging3 where total_laid_off is null and percentage_laid_off is null;



select * from layoffs_staging3 where industry is null or industry ='';


select * from layoffs_staging3 where company ='Airbnb';


update layoffs_staging3 set industry=NULL 
where industry ='';


select t1.industry, t2.industry from 
layoffs_staging2 as t1 
join layoffs_staging3 t2
  on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;



update layoffs_staging3 as t1 
join layoffs_staging3 as t2 on 
t1.company =t2.company 
set t1.industry =t2.industry 
where (t1.industry is null )
and t2.industry is not null;


select * from layoffs_staging3 where total_laid_off is null and percentage_laid_off is null;

delete from layoffs_staging3 where total_laid_off is null and percentage_laid_off is null;

select * from layoffs_staging3;

alter table layoffs_staging3
drop column row_num;

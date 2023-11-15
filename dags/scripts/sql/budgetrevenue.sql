BEGIN;


INSERT INTO movies.budgetrevenue
SELECT * FROM movies.stage_budgetrevenue

DROP TABLE IF EXISTS movies.stage_budgetrevenue;

END;
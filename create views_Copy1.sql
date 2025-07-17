-------------------------
-- CREATE VIEW CALENDER --
----------------------
CREATE VIEW gold.calander
AS 
SELECT
 *
 FROM
    OPENROWSET(
        BULK 'https://companystorages.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
        FORMAT = 'PARQUET'
    ) as calender

    -------------------------
-- CREATE VIEW Customers --
----------------------
CREATE VIEW gold.customers
AS 
SELECT
 *
 FROM
    OPENROWSET(
        BULK 'https://companystorages.dfs.core.windows.net/silver/AdventureWorks_Customers/',
        FORMAT = 'PARQUET'
    ) as customers

    -------------------------
-- CREATE VIEW PRODUCTS --
----------------------
CREATE VIEW gold.products
AS 
SELECT
 *
 FROM
    OPENROWSET(
        BULK 'https://companystorages.dfs.core.windows.net/silver/AdventureWorks_Products/',
        FORMAT = 'PARQUET'
    ) as Products

    -------------------------
-- CREATE VIEW RETUENS --
----------------------
CREATE VIEW gold.returns
AS 
SELECT
 *
 FROM
    OPENROWSET(
        BULK 'https://companystorages.dfs.core.windows.net/silver/AdventureWorks_Returns/',
        FORMAT = 'PARQUET'
    ) as Retur

      -------------------------
-- CREATE VIEW SUBCAT --
----------------------
CREATE VIEW gold.subcat
AS 
SELECT
 *
 FROM
    OPENROWSET(
        BULK 'https://companystorages.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
        FORMAT = 'PARQUET'
    ) as SubCat

    df_ret.write.format("parquet")\
    .mode("append")\
        .save("abfss://silver@companystorages.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

         -------------------------
-- CREATE VIEW SUBCAT --
----------------------
CREATE VIEW gold.territories
AS 
SELECT
 *
 FROM
    OPENROWSET(
        BULK 'https://companystorages.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
        FORMAT = 'PARQUET'
    ) as terr

   
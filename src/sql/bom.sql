CREATE DATABASE BomDB;

USE BomDB;
GO

/*===========================================================
    Start Store Procedure ==> Loading Transformtions
===========================================================*/
CREATE OR ALTER PROCEDURE dbo.usp_LoadProductionData
AS
BEGIN
    SET NOCOUNT ON;

    /*===========================================================
      1. Ensure Staging Table Exists
    ===========================================================*/
    IF OBJECT_ID('dbo.stg_ProductionData') IS NULL
    BEGIN
        CREATE TABLE dbo.stg_ProductionData (
            year INT,
            month INT,
            produced_material NVARCHAR(50),
            produced_material_production_type NVARCHAR(50),
            produced_material_release_type NVARCHAR(10),
            produced_material_quantity DECIMAL(10,2),
            component_material NVARCHAR(50),
            component_material_production_type NVARCHAR(50),
            component_material_release_type NVARCHAR(10),
            component_material_quantity DECIMAL(10,2),
            plant_id NVARCHAR(50)
        );
    END;

    /*===========================================================
      2. Bulk Load Data into Staging
    ===========================================================*/
    BULK INSERT dbo.stg_ProductionData
    FROM '/var/opt/mssql/import/production_data.csv' -- path in docker container env
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK,
        KEEPNULLS
    );

    /*===========================================================
      3. Ensure Production Table Exists
    ===========================================================*/
    IF OBJECT_ID('dbo.ProductionData') IS NULL
    BEGIN
        CREATE TABLE dbo.ProductionData (
            plant_id NVARCHAR(50),
            year INT,
            produced_material NVARCHAR(50),
            produced_material_production_type NVARCHAR(50),
            produced_material_release_type NVARCHAR(10),
            produced_material_quantity DECIMAL(10,2),
            component_material NVARCHAR(50),
            component_material_production_type NVARCHAR(50),
            component_material_release_type NVARCHAR(10),
            component_material_quantity DECIMAL(10,2)
        );
    END;

    /*===========================================================
      4. Transform and Insert Cleaned Data
    ===========================================================*/
    WITH cleaned_data AS (
        SELECT 
            plant_id,
            year,
            produced_material,
            produced_material_production_type,
            produced_material_release_type,
            SUM(produced_material_quantity) AS produced_material_quantity,
            component_material,
            ISNULL(component_material_production_type, '_') AS component_material_production_type,
            component_material_release_type,
            SUM(component_material_quantity) AS component_material_quantity
        FROM dbo.stg_ProductionData
        GROUP BY 
            plant_id, year, produced_material, produced_material_production_type,
            produced_material_release_type, component_material, component_material_production_type,
            component_material_release_type
    )
    INSERT INTO dbo.ProductionData (
        plant_id,
        year,
        produced_material,
        produced_material_production_type,
        produced_material_release_type,
        produced_material_quantity,
        component_material,
        component_material_production_type,
        component_material_release_type,
        component_material_quantity
    )
    SELECT 
        plant_id,
        year,
        produced_material,
        produced_material_production_type,
        produced_material_release_type,
        produced_material_quantity,
        component_material,
        component_material_production_type,
        component_material_release_type,
        component_material_quantity
    FROM cleaned_data;

END;
GO
/*===========================================================
    END Store Procedure 
===========================================================*/



SELECT * FROM dbo.ProductionData
GO

/*===========================================================
    Start VIEW  
===========================================================*/
CREATE OR ALTER VIEW dbo.vw_BillOfMaterials
AS
WITH recursive_cte AS (
    -- Anchor query
    SELECT 
        plant_id,
        year,
        produced_material AS fin_material_id,
        produced_material_production_type AS fin_material_production_type,
        produced_material_release_type AS fin_material_release_type,
        produced_material_quantity AS fin_production_quantity,

        produced_material AS prod_material_id,
        produced_material_production_type AS prod_material_production_type,
        produced_material_release_type AS prod_material_release_type,
        produced_material_quantity AS prod_material_production_quantity,

        component_material AS component_id,
        component_material_production_type,
        component_material_release_type,
        component_material_quantity AS component_consumption_quantity
    FROM dbo.ProductionData
    WHERE produced_material_release_type = 'FIN'

    UNION ALL

    -- Recursive query
    SELECT 
        pr.plant_id,
        pr.year,
        rc.fin_material_id,
        rc.fin_material_production_type,
        rc.fin_material_release_type,
        rc.fin_production_quantity,
        pr.produced_material AS prod_material_id,
        pr.produced_material_production_type AS prod_material_production_type,
        pr.produced_material_release_type AS prod_material_release_type,
        pr.produced_material_quantity AS prod_material_production_quantity,
        pr.component_material AS component_id,
        pr.component_material_production_type,
        pr.component_material_release_type,
        pr.component_material_quantity AS component_consumption_quantity
    FROM dbo.ProductionData pr
    JOIN recursive_cte rc 
        ON pr.produced_material = rc.component_id
    WHERE rc.component_material_release_type IN ('FIN', 'PROD')
)
SELECT * FROM recursive_cte;
GO

/*===========================================================
    END VIEW  
===========================================================*/
SELECT * FROM dbo.vw_BillOfMaterials;
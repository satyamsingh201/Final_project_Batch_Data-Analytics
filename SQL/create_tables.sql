
COMMENT#-> IT IS A SOURCE CODE OF TABLE WHICH WE CREATE IN sql DATABASE.

DROP TABLE IF EXISTS source_car_data;
CREATE TABLE source_car_data (
    Branch_ID      VARCHAR(200),
    Dealer_ID      VARCHAR(200),
    Model_ID       VARCHAR(200),
    Revenue        BIGINT,
    Units_Sold     BIGINT,
    Date_ID        VARCHAR(200),
    Day            INT,
    Month          INT,
    Year           INT,
    BranchName     VARCHAR(200),
    DealerName     VARCHAR(200),
    Product_Name   VARCHAR(200)
);

COMMENT#->IT IS SELECT STATMENT FOR SELECT TO SEE ALL THE DATA.

SELECT * FROM source_car_data;

SELECT MIN(Date_ID) FROM source_car_data;

COMMENT#-> IT IS A WATER  TABLE FOR UPDATE LASTLOAD  FROM THE LASTEST DATA
CREATE TABLE water_table
(
    Last_load  VARCHAR(200),
);


INSERT INTO water_table(Last_load)
VALUES('DT0000')

SELECT * FROM water_table;

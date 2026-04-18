
CREATE PROCEDURE Update_water_table
  @Last_load VARCHAR(200)

AS
BEGIN

   BEGIN TRANSACTION
    UPDATE water_table
    SET Last_load=@Last_load
    COMMIT TRANSACTION
END;

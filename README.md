# easysnowflake

Follow the script given for snowflake for warehouse, database, schema and tables creations.
Also, storage integration needs to be created under the name "easygo_api".
Take the given storage integration script and replace the azure tetent id as per your azure account settings.
Once integration is done, then describe the same to accept the permission to integrate with azure.
In Azure storage account ensure the snowflake service principle has storage blob data reader and storage blob data contributor access setup.

------ STORAGE INTEGRATION AND EXTERNAL STAGE-------------

CREATE OR REPLACE STORAGE INTEGRATION easygo_api
  TYPE = EXTERNAL_STAGE
  ENABLED = TRUE
  STORAGE_PROVIDER = 'AZURE'
  AZURE_TENANT_ID = <tenant id from your azure active directory settings> 
  STORAGE_ALLOWED_LOCATIONS = ('*');

---- CREATE STAGE------

create or replace stage stage_bronze.blob_store_stage
STORAGE_INTEGRATION = easygo_api
url = <raw container storage account's url>; --URL will be replaced based on the storage integration account

---- COPY INTO STAGE -----

copy into easy_stage.stage_bronze.daily_results 
from '<raw container storage account's url>'
storage_integration = easygo_api
file_format = (
type = 'JSON' 
) 

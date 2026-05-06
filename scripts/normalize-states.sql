BEGIN;
-- Normalize FIPS codes to state abbreviations
UPDATE environmental_sites SET state = 'AL' WHERE state = '01';
UPDATE environmental_sites SET state = 'AK' WHERE state = '02';
UPDATE environmental_sites SET state = 'AZ' WHERE state = '04';
UPDATE environmental_sites SET state = 'AR' WHERE state = '05';
UPDATE environmental_sites SET state = 'CA' WHERE state = '06';
UPDATE environmental_sites SET state = 'CO' WHERE state = '08';
UPDATE environmental_sites SET state = 'CT' WHERE state = '09';
UPDATE environmental_sites SET state = 'DE' WHERE state = '10';
UPDATE environmental_sites SET state = 'DC' WHERE state = '11';
UPDATE environmental_sites SET state = 'GA' WHERE state = '13';
UPDATE environmental_sites SET state = 'ID' WHERE state = '16';
UPDATE environmental_sites SET state = 'IL' WHERE state = '17';
UPDATE environmental_sites SET state = 'IA' WHERE state = '19';
UPDATE environmental_sites SET state = 'KY' WHERE state = '21';
UPDATE environmental_sites SET state = 'ME' WHERE state = '23';
UPDATE environmental_sites SET state = 'MA' WHERE state = '25';
UPDATE environmental_sites SET state = 'MI' WHERE state = '26';
UPDATE environmental_sites SET state = 'MS' WHERE state = '28';
UPDATE environmental_sites SET state = 'MO' WHERE state = '29';
UPDATE environmental_sites SET state = 'MT' WHERE state = '30';
UPDATE environmental_sites SET state = 'NE' WHERE state = '31';
UPDATE environmental_sites SET state = 'NV' WHERE state = '32';
UPDATE environmental_sites SET state = 'NJ' WHERE state = '34';
UPDATE environmental_sites SET state = 'NM' WHERE state = '35';
UPDATE environmental_sites SET state = 'NY' WHERE state = '36';
UPDATE environmental_sites SET state = 'ND' WHERE state = '38';
UPDATE environmental_sites SET state = 'OH' WHERE state = '39';
UPDATE environmental_sites SET state = 'OK' WHERE state = '40';
UPDATE environmental_sites SET state = 'OR' WHERE state = '41';
UPDATE environmental_sites SET state = 'PA' WHERE state = '42';
UPDATE environmental_sites SET state = 'RI' WHERE state = '44';
UPDATE environmental_sites SET state = 'SD' WHERE state = '46';
UPDATE environmental_sites SET state = 'TX' WHERE state = '48';
UPDATE environmental_sites SET state = 'UT' WHERE state = '49';
UPDATE environmental_sites SET state = 'VT' WHERE state = '50';
UPDATE environmental_sites SET state = 'VA' WHERE state = '51';
UPDATE environmental_sites SET state = 'WA' WHERE state = '53';
UPDATE environmental_sites SET state = 'WV' WHERE state = '54';
-- Fix case variants
UPDATE environmental_sites SET state = 'TX' WHERE state IN ('tx', 'Tx');
-- Remove invalid state codes
UPDATE environmental_sites SET state = NULL 
WHERE state IS NOT NULL 
  AND state NOT IN ('AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY');
COMMIT;

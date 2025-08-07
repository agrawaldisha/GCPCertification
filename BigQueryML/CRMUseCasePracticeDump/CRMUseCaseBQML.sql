-- SELECT * FROM `gcpdataengineering-467713.CustomerReviewGenAI.cutomer_reviews` LIMIT 1000

LOAD DATA OVERWRITE CustomerReviewGenAI.customer_reviews
(customer_review_id INT64, customer_id INT64, location_id INT64, review_datetime DATETIME, review_text STRING, social_media_source STRING, social_media_handle STRING)
FROM FILES (
  format = 'CSV',
  uris = ['gs://coffeeonwheels/customer_reviews.csv']);

-- The result is the review_images object table is added to the CustomerReviewGenAI dataset and loaded with the uri (the cloud storage location) of each audio review in the sample dataset.
-- This Creates a object table in BQ to store unstructured data 
CREATE OR REPLACE EXTERNAL TABLE
  `CustomerReviewGenAI.review_images`
WITH CONNECTION `us.gemini_conn`
OPTIONS (
  object_metadata = 'SIMPLE',
  uris = ['gs://coffeeonwheels/images/*']
  );

-- BigQuery ML statement used to create or update a remote machine learning model i
CREATE OR REPLACE MODEL `CustomerReviewGenAI.gemini_2_0_flash`
REMOTE WITH CONNECTION `us.gemini_conn`
OPTIONS (endpoint = 'gemini-2.0-flash')

-- Prmopt for Keyword Extraction  
-- This query takes customer reviews from the customer_reviews table, constructs prompts for the gemini_2_0_flash model to identify keywords within each review. The results are then stored in a new table customer_reviews_keywords.

CREATE OR REPLACE TABLE
`CustomerReviewGenAI.customer_reviews_keywords` AS (
SELECT ml_generate_text_llm_result, social_media_source, review_text, customer_id, location_id, review_datetime
FROM
ML.GENERATE_TEXT(
MODEL `CustomerReviewGenAI.gemini_2_0_flash`,
(
   SELECT social_media_source, customer_id, location_id, review_text, review_datetime, CONCAT(
      'For each review, provide keywords from the review. Answer in JSON format with one key: keywords. Keywords should be a list.',
      review_text) AS prompt
   FROM `CustomerReviewGenAI.customer_reviews`
),
STRUCT(
   0.2 AS temperature, TRUE AS flatten_json_output)));

--Renaming table in bigquery 
-- CREATE OR REPLACE TABLE `CustomerReviewGenAI.customer_reviews` AS
-- SELECT * FROM `CustomerReviewGenAI.customer_reviews_cleaned`;

-- Renaming columns in table
-- CREATE OR REPLACE TABLE `CustomerReviewGenAI.customer_reviews_cleaned` AS
-- SELECT
--   int64_field_0 AS customer_id,
--   int64_field_1 AS location_id,
--   int64_field_2 AS truck_id,
--   timestamp_field_3 AS review_datetime,
--   string_field_4 AS review_text,
--   string_field_5 AS social_media_source,
--   string_field_6 AS user_handle
-- FROM `CustomerReviewGenAI.customer_reviews`;

-- Analyze the customer reviews for positive and negative sentiment
--This query takes customer reviews from the customer_reviews table, constructs prompts for the gemini_2_0_flash model to classify the sentiment of each review. The results are then stored in a new table customer_reviews_analysis, so that you may use it later for further analysis.

CREATE OR REPLACE TABLE
`CustomerReviewGenAI.customer_reviews_analysis` AS (
SELECT ml_generate_text_llm_result, social_media_source, review_text, customer_id, location_id, review_datetime
FROM
ML.GENERATE_TEXT(
MODEL `CustomerReviewGenAI.gemini_2_0_flash`,
(
   SELECT social_media_source, customer_id, location_id, review_text, review_datetime, CONCAT(
      'Classify the sentiment of the following text as positive or negative.',
      review_text, "In your response don't include the sentiment explanation. Remove all extraneous information from your response, it should be a boolean response either positive or negative.") AS prompt
   FROM `CustomerReviewGenAI.customer_reviews`
),
STRUCT(
   0.2 AS temperature, TRUE AS flatten_json_output)));


--Create a view to sanitize the records
--The query creates the view, cleaned_data_view and includes the sentiment results

CREATE OR REPLACE VIEW CustomerReviewGenAI.cleaned_data_view AS
SELECT
REPLACE(REPLACE(REPLACE(LOWER(ml_generate_text_llm_result), '.', ''), ' ', ''), '\n', '') AS sentiment,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(social_media_source, r'Google(\+|\sReviews|\sLocal|\sMy\sBusiness|\sreviews|\sMaps)?',
      'Google'), 'YELP', 'Yelp'), r'SocialMedia1?', 'Social Media') AS social_media_source,
review_text,
customer_id,
location_id,
review_datetime
FROM
CustomerReviewGenAI.customer_reviews_analysis;

SELECT
  *
FROM
  `gcpdataengineering-467713.CustomerReviewGenAI.cleaned_data_view`;
  
--Create a report of positive and negative review counts

SELECT sentiment, COUNT(*) AS count
FROM `CustomerReviewGenAI.cleaned_data_view`
WHERE sentiment IN ('positive', 'negative')
GROUP BY sentiment;

-- Create a count of positive and negative reviews by social media source

SELECT sentiment, social_media_source, COUNT(*) AS count
FROM `CustomerReviewGenAI.cleaned_data_view`
WHERE sentiment IN ('positive') OR sentiment IN ('negative')
GROUP BY sentiment, social_media_source
ORDER BY sentiment, count;    


-- Respond to customer reviews
-- Note: Refer to zero-shot vs. few-shot prompts within the Google AI for Developers documentation for more information.
-- This table will contain the original review data along with the generated marketing suggestions, allowing you to easily analyze and act upon them.
CREATE OR REPLACE TABLE
`CustomerReviewGenAI.customer_reviews_marketing` AS (
SELECT ml_generate_text_llm_result, social_media_source, review_text, customer_id, location_id, review_datetime
FROM
ML.GENERATE_TEXT(
MODEL `CustomerReviewGenAI.gemini_2_0_flash`,
(
   SELECT social_media_source, customer_id, location_id, review_text, review_datetime, CONCAT(
      'You are a marketing representative. How could we incentivise this customer with this positive review? Provide a single response, and should be simple and concise, do not include emojis. Answer in JSON format with one key: marketing. Marketing should be a string.', review_text) AS prompt
   FROM `CustomerReviewGenAI.customer_reviews`
   WHERE customer_id = 5576
),
STRUCT(
   0.2 AS temperature, TRUE AS flatten_json_output)));

SELECT * FROM `CustomerReviewGenAI.customer_reviews_marketing`


-- Make easier to read 
CREATE OR REPLACE TABLE
`CustomerReviewGenAI.customer_reviews_marketing_formatted` AS (
SELECT
   review_text,
   JSON_QUERY(RTRIM(LTRIM(results.ml_generate_text_llm_result, " ```json"), "```"), "$.marketing") AS marketing,
   social_media_source, customer_id, location_id, review_datetime
FROM
   `CustomerReviewGenAI.customer_reviews_marketing` results )

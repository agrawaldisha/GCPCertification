-- This query uses the LOAD DATA statement to load the customer_reviews.csv file from Cloud Storage to a BigQuery table with the given column names and data types.


LOAD DATA OVERWRITE gemini_demo.customer_reviews
(customer_review_id INT64, customer_id INT64, location_id INT64, review_datetime DATETIME, review_text STRING, social_media_source STRING, social_media_handle STRING)
FROM FILES (
  format = 'CSV',
  uris = ['gs://qwiklabs-gcp-01-99af910bdb92-bucket/gsp1246/customer_reviews.csv']);

--  The result is the review_images object table is added to the gemini_demo dataset and loaded with the uri (the cloud storage location) of each audio review in the sample dataset.

CREATE OR REPLACE EXTERNAL TABLE
  `gemini_demo.review_images`
WITH CONNECTION `us.gemini_conn`
OPTIONS (
  object_metadata = 'SIMPLE',
  uris = ['gs://qwiklabs-gcp-01-99af910bdb92-bucket/gsp1246/images/*']
  );

-- The result is the gemini_2_0_flash model is created and you see it added to the gemini_demo dataset, in the models section.

CREATE OR REPLACE MODEL `gemini_demo.gemini_2_0_flash`
REMOTE WITH CONNECTION `us.gemini_conn`
OPTIONS (endpoint = 'gemini-2.0-flash')

-- This query takes customer reviews from the customer_reviews table, constructs prompts for the gemini_2_0_flash model to identify keywords within each review. The results are then stored in a new table customer_reviews_keywords.

CREATE OR REPLACE TABLE
`gemini_demo.customer_reviews_keywords` AS (
SELECT ml_generate_text_llm_result, social_media_source, review_text, customer_id, location_id, review_datetime
FROM
ML.GENERATE_TEXT(
MODEL `gemini_demo.gemini_2_0_flash`,
(
   SELECT social_media_source, customer_id, location_id, review_text, review_datetime, CONCAT(
      'For each review, provide keywords from the review. Answer in JSON format with one key: keywords. Keywords should be a list.',
      review_text) AS prompt
   FROM `gemini_demo.customer_reviews`
),
STRUCT(
   0.2 AS temperature, TRUE AS flatten_json_output)));

SELECT * FROM `gemini_demo.customer_reviews_keywords`

-- Analyze the customer reviews for positive and negative sentiment
--This query takes customer reviews from the customer_reviews table, constructs prompts for the gemini_2_0_flash model to classify the sentiment of each review. The results are then stored in a new table customer_reviews_analysis, so that you may use it later for further analysis.
CREATE OR REPLACE TABLE
`gemini_demo.customer_reviews_analysis` AS (
SELECT ml_generate_text_llm_result, social_media_source, review_text, customer_id, location_id, review_datetime
FROM
ML.GENERATE_TEXT(
MODEL `gemini_demo.gemini_2_0_flash`,
(
   SELECT social_media_source, customer_id, location_id, review_text, review_datetime, CONCAT(
      'Classify the sentiment of the following text as positive or negative.',
      review_text, "In your response don't include the sentiment explanation. Remove all extraneous information from your response, it should be a boolean response either positive or negative.") AS prompt
   FROM `gemini_demo.customer_reviews`
),
STRUCT(
   0.2 AS temperature, TRUE AS flatten_json_output)));

--The result is rows customer_reviews_analysis table with the ml_generate_text_llm_result column containing the sentiment analysis, with the social_media_source, review_text, customer_id, location_id and review_datetime columns included

SELECT * FROM `gemini_demo.customer_reviews_analysis`
ORDER BY review_datetime

--Create a view to sanitize the records
--The query creates the view, cleaned_data_view and includes the sentiment results
CREATE OR REPLACE VIEW gemini_demo.cleaned_data_view AS
SELECT
REPLACE(REPLACE(REPLACE(LOWER(ml_generate_text_llm_result), '.', ''), ' ', ''), '\n', '') AS sentiment,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(social_media_source, r'Google(\+|\sReviews|\sLocal|\sMy\sBusiness|\sreviews|\sMaps)?',
      'Google'), 'YELP', 'Yelp'), r'SocialMedia1?', 'Social Media') AS social_media_source,
review_text,
customer_id,
location_id,
review_datetime
FROM
gemini_demo.customer_reviews_analysis;

--Create a report of positive and negative review counts

SELECT sentiment, COUNT(*) AS count
FROM `gemini_demo.cleaned_data_view`
WHERE sentiment IN ('positive', 'negative')
GROUP BY sentiment;

-- Create a count of positive and negative reviews by social media source

SELECT sentiment, social_media_source, COUNT(*) AS count
FROM `gemini_demo.cleaned_data_view`
WHERE sentiment IN ('positive') OR sentiment IN ('negative')
GROUP BY sentiment, social_media_source
ORDER BY sentiment, count;    


-- Respond to customer reviews
-- Note: Refer to zero-shot vs. few-shot prompts within the Google AI for Developers documentation for more information.
-- This table will contain the original review data along with the generated marketing suggestions, allowing you to easily analyze and act upon them.
CREATE OR REPLACE TABLE
`gemini_demo.customer_reviews_marketing` AS (
SELECT ml_generate_text_llm_result, social_media_source, review_text, customer_id, location_id, review_datetime
FROM
ML.GENERATE_TEXT(
MODEL `gemini_demo.gemini_2_0_flash`,
(
   SELECT social_media_source, customer_id, location_id, review_text, review_datetime, CONCAT(
      'You are a marketing representative. How could we incentivise this customer with this positive review? Provide a single response, and should be simple and concise, do not include emojis. Answer in JSON format with one key: marketing. Marketing should be a string.', review_text) AS prompt
   FROM `gemini_demo.customer_reviews`
   WHERE customer_id = 5576
),
STRUCT(
   0.2 AS temperature, TRUE AS flatten_json_output)));

SELECT * FROM `gemini_demo.customer_reviews_marketing`


-- Make easier to read 
CREATE OR REPLACE TABLE
`gemini_demo.customer_reviews_marketing_formatted` AS (
SELECT
   review_text,
   JSON_QUERY(RTRIM(LTRIM(results.ml_generate_text_llm_result, " ```json"), "```"), "$.marketing") AS marketing,
   social_media_source, customer_id, location_id, review_datetime
FROM
   `gemini_demo.customer_reviews_marketing` results )


SELECT * FROM `gemini_demo.customer_reviews_marketing_formatted`


-- This query is designed to automate customer service responses by using Gemini to analyze customer reviews and generate appropriate responses and action plans. It's a powerful example of how Google Cloud can be used to enhance customer service and improve business operations. When the query is run, the result is the customer_reviews_cs_response table is created.

CREATE OR REPLACE TABLE
`gemini_demo.customer_reviews_cs_response` AS (
SELECT ml_generate_text_llm_result, social_media_source, review_text, customer_id, location_id, review_datetime
FROM
ML.GENERATE_TEXT(
MODEL `gemini_demo.gemini_2_0_flash`,
(
   SELECT social_media_source, customer_id, location_id, review_text, review_datetime, CONCAT(
      'How would you respond to this customer review? If the customer says the coffee is weak or burnt, respond stating "thank you for the review we will provide your response to the location that you did not like the coffee and it could be improved." Or if the review states the service is bad, respond to the customer stating, "the location they visited has been notified and we are taking action to improve our service at that location." From the customer reviews provide actions that the location can take to improve. The response and the actions should be simple, and to the point. Do not include any extraneous or special characters in your response. Answer in JSON format with two keys: Response, and Actions. Response should be a string. Actions should be a string.', review_text) AS prompt
   FROM `gemini_demo.customer_reviews`
   WHERE customer_id = 8844
),
STRUCT(
   0.2 AS temperature, TRUE AS flatten_json_output)));


SELECT * FROM `gemini_demo.customer_reviews_cs_response`

--Make Easier to read

CREATE OR REPLACE TABLE
`gemini_demo.customer_reviews_cs_response_formatted` AS (
SELECT
   review_text,
   JSON_QUERY(RTRIM(LTRIM(results.ml_generate_text_llm_result, " ```json"), "```"), "$.Response") AS Response,
   JSON_QUERY(RTRIM(LTRIM(results.ml_generate_text_llm_result, " ```json"), "```"), "$.Actions") AS Actions,
   social_media_source, customer_id, location_id, review_datetime
FROM
   `gemini_demo.customer_reviews_cs_response` results )

SELECT * FROM `gemini_demo.customer_reviews_cs_response_formatted`


--  Prompt Gemini to provide keywords and summaries for each image
-- Analyze the images with Gemini 2.0 Flash model

CREATE OR REPLACE TABLE
`gemini_demo.review_images_results` AS (
SELECT
    uri,
    ml_generate_text_llm_result
FROM
    ML.GENERATE_TEXT( MODEL `gemini_demo.gemini_2_0_flash`,
    TABLE `gemini_demo.review_images`,
    STRUCT( 0.2 AS temperature,
        'For each image, provide a summary of what is happening in the image and keywords from the summary. Answer in JSON format with two keys: summary, keywords. Summary should be a string, keywords should be a list.' AS PROMPT,
        TRUE AS FLATTEN_JSON_OUTPUT)));

SELECT * FROM `gemini_demo.review_images_results`;

CREATE OR REPLACE TABLE
  `gemini_demo.review_images_results_formatted` AS (
  SELECT
    uri,
    JSON_QUERY(RTRIM(LTRIM(results.ml_generate_text_llm_result, " ```json"), "```"), "$.summary") AS summary,
    JSON_QUERY(RTRIM(LTRIM(results.ml_generate_text_llm_result, " ```json"), "```"), "$.keywords") AS keywords
  FROM
    `gemini_demo.review_images_results` results )

SELECT * FROM `gemini_demo.review_images_results_formatted`

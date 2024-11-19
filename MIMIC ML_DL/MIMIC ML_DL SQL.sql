-- These are run in parallel for each query in the following pattern:
-- (Query 1 + Query N)
-- Results are aggregated in Google Colab notebook

-- 1st Query: Get the cohort
WITH icu_cohort AS (
  SELECT DISTINCT
    ie.subject_id,
    ie.hadm_id,
    ie.stay_id,
    ie.intime as icu_intime,
    ie.outtime as icu_outtime,
    DATETIME_DIFF(ie.outtime, ie.intime, HOUR) as icu_length_hours,
    p.gender,
    p.anchor_age as age
  FROM `physionet-data.mimiciv_3_1_icu.icustays` ie
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON ie.subject_id = p.subject_id
  WHERE DATETIME_DIFF(ie.outtime, ie.intime, HOUR) BETWEEN 24 AND 72  -- 1-3 day stays
  AND p.anchor_age BETWEEN 18 AND 80  -- Adult patients
  LIMIT 50
)
SELECT * FROM icu_cohort;

-- 2nd Query: Get medications 
WITH icu_cohort AS (...)  -- Same as above
SELECT 
    m.stay_id,
    m.starttime,
    m.endtime,
    m.itemid,
    CAST(m.amount as STRING) as amount,
    m.amountuom,
    m.rate,
    m.rateuom,
    CAST(m.orderid as STRING) as orderid
FROM `physionet-data.mimiciv_3_1_icu.inputevents` m
WHERE m.stay_id IN (SELECT stay_id FROM icu_cohort);

-- 3rd Query: Get vital signs
WITH icu_cohort AS (...) -- Same as above
SELECT 
    ce.stay_id,
    ce.charttime,
    ce.itemid,
    di.label as vital_name,
    ce.valuenum,
    ce.valueuom
FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
JOIN `physionet-data.mimiciv_3_1_icu.d_items` di
    ON ce.itemid = di.itemid
WHERE ce.stay_id IN (SELECT stay_id FROM icu_cohort)
AND di.category = 'Vital Signs';

-- 4th Query: Get lab results
WITH icu_cohort AS (...) -- Same as above
SELECT 
    ie.stay_id,
    le.charttime,
    le.itemid,
    di.label as lab_name,
    le.valuenum,
    le.valueuom
FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
JOIN `physionet-data.mimiciv_3_1_hosp.d_labitems` di
    ON le.itemid = di.itemid
JOIN icu_cohort ie
    ON le.subject_id = ie.subject_id
WHERE le.charttime BETWEEN ie.icu_intime AND ie.icu_outtime;

-- 5th Query: Get procedures
WITH icu_cohort AS (...) -- Same as above
SELECT 
    stay_id,
    starttime,
    endtime,
    itemid,
    value,
    valueuom,
    location
FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
WHERE stay_id IN (SELECT stay_id FROM icu_cohort);
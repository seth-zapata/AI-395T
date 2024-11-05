WITH lab_measurements AS (
    SELECT 
        le.hadm_id,
        le.charttime,
        CASE
            WHEN le.itemid IN (50912) THEN 'creatinine'
            WHEN le.itemid IN (50811) THEN 'hemoglobin'
            WHEN le.itemid IN (50983) THEN 'sodium'
            WHEN le.itemid IN (50971) THEN 'potassium'
            WHEN le.itemid IN (50802) THEN 'bicarbonate'
            WHEN le.itemid IN (50931) THEN 'glucose'
            WHEN le.itemid IN (51006) THEN 'bun'
            WHEN le.itemid IN (51265) THEN 'platelet'
        END AS lab_type,
        le.valuenum as lab_value
    FROM `physionet-data.mimiciii_clinical.labevents` le
    WHERE le.itemid IN (
        50912,  -- Creatinine
        50811,  -- Hemoglobin
        50983,  -- Sodium
        50971,  -- Potassium
        50802,  -- Bicarbonate
        50931,  -- Glucose
        51006,  -- BUN
        51265   -- Platelet
    )
    AND le.valuenum IS NOT NULL
),
first_day_labs AS (
    SELECT 
        le.hadm_id,
        lab_type,
        AVG(lab_value) as lab_value
    FROM lab_measurements le
    INNER JOIN `physionet-data.mimiciii_clinical.admissions` adm
        ON le.hadm_id = adm.hadm_id
    WHERE TIMESTAMP_ADD(TIMESTAMP(adm.admittime), INTERVAL 24 HOUR) >= TIMESTAMP(le.charttime)
    GROUP BY 1, 2
),
vital_signs AS (
    SELECT 
        ce.hadm_id,
        ce.charttime,
        CASE
            WHEN ce.itemid IN (211,220045) THEN 'heart_rate'
            WHEN ce.itemid IN (51,442,455,6701,220179,220050) THEN 'systolic_bp'
            WHEN ce.itemid IN (615,618,220210,224690) THEN 'respiratory_rate'
        END AS vital_type,
        ce.valuenum as vital_value
    FROM `physionet-data.mimiciii_clinical.chartevents` ce
    WHERE ce.itemid IN (
        211,220045,        -- Heart rate
        51,442,455,6701,220179,220050,  -- Systolic BP
        615,618,220210,224690  -- Respiratory rate
    )
    AND ce.error IS DISTINCT FROM 1
    AND ce.valuenum IS NOT NULL
),
first_day_vitals AS (
    SELECT 
        v.hadm_id,
        vital_type,
        AVG(vital_value) as vital_value
    FROM vital_signs v
    INNER JOIN `physionet-data.mimiciii_clinical.admissions` adm
        ON v.hadm_id = adm.hadm_id
    WHERE TIMESTAMP_ADD(TIMESTAMP(adm.admittime), INTERVAL 24 HOUR) >= TIMESTAMP(v.charttime)
    GROUP BY 1, 2
),
procedures_check AS (
    SELECT 
        hadm_id,
        MAX(CASE WHEN icd9_code LIKE '96.7%' THEN 1 ELSE 0 END) as mechanical_ventilation,
        MAX(CASE WHEN icd9_code LIKE '38.9%' THEN 1 ELSE 0 END) as venous_cath,
        MAX(CASE WHEN icd9_code LIKE '39.95%' THEN 1 ELSE 0 END) as dialysis
    FROM `physionet-data.mimiciii_clinical.procedures_icd`
    GROUP BY 1
)
SELECT 
    adm.hadm_id,
    -- Demographics
    p.gender,
    ROUND(DATETIME_DIFF(adm.admittime, p.dob, YEAR)) as age,
    adm.admission_type,
    adm.admission_location,
    adm.insurance,
    CASE 
        WHEN adm.diagnosis LIKE '%SEPSIS%' THEN 1 
        WHEN adm.diagnosis LIKE '%SEPTIC%' THEN 1
        ELSE 0 
    END as sepsis_diagnosis,
    -- Labs
    MAX(CASE WHEN l.lab_type = 'creatinine' THEN l.lab_value END) as creatinine,
    MAX(CASE WHEN l.lab_type = 'hemoglobin' THEN l.lab_value END) as hemoglobin,
    MAX(CASE WHEN l.lab_type = 'sodium' THEN l.lab_value END) as sodium,
    MAX(CASE WHEN l.lab_type = 'potassium' THEN l.lab_value END) as potassium,
    MAX(CASE WHEN l.lab_type = 'bicarbonate' THEN l.lab_value END) as bicarbonate,
    MAX(CASE WHEN l.lab_type = 'glucose' THEN l.lab_value END) as glucose,
    MAX(CASE WHEN l.lab_type = 'bun' THEN l.lab_value END) as bun,
    MAX(CASE WHEN l.lab_type = 'platelet' THEN l.lab_value END) as platelet,
    -- Vitals
    MAX(CASE WHEN v.vital_type = 'heart_rate' THEN v.vital_value END) as heart_rate,
    MAX(CASE WHEN v.vital_type = 'systolic_bp' THEN v.vital_value END) as systolic_bp,
    MAX(CASE WHEN v.vital_type = 'respiratory_rate' THEN v.vital_value END) as respiratory_rate,
    -- Procedures
    COALESCE(pr.mechanical_ventilation, 0) as mechanical_ventilation,
    COALESCE(pr.venous_cath, 0) as venous_catheterization,
    COALESCE(pr.dialysis, 0) as dialysis,
    -- Outcome
    TIMESTAMP_DIFF(adm.dischtime, adm.admittime, HOUR)/24.0 as length_of_stay_days
FROM `physionet-data.mimiciii_clinical.admissions` adm
INNER JOIN `physionet-data.mimiciii_clinical.patients` p
    ON adm.subject_id = p.subject_id
LEFT JOIN first_day_labs l
    ON adm.hadm_id = l.hadm_id
LEFT JOIN first_day_vitals v
    ON adm.hadm_id = v.hadm_id
LEFT JOIN procedures_check pr
    ON adm.hadm_id = pr.hadm_id
WHERE 
    adm.dischtime IS NOT NULL
    AND adm.admittime IS NOT NULL
    -- Exclude unreasonable LOS
    AND TIMESTAMP_DIFF(adm.dischtime, adm.admittime, HOUR)/24.0 BETWEEN 0 AND 365
GROUP BY 
    adm.hadm_id, p.gender, p.dob,
    adm.admission_type, adm.admission_location, adm.insurance,
    adm.diagnosis, adm.admittime, adm.dischtime,
    pr.mechanical_ventilation, pr.venous_cath, pr.dialysis
LIMIT 1000;
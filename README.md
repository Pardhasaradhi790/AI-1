CREATE TABLE rules_engine (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,

    NonFacFee_condition VARCHAR(50),
    FacFee_condition VARCHAR(50),
    Fee_Compare VARCHAR(20),
    Codeid_condition VARCHAR(50),

    specialtycode VARCHAR(20),

    action_type VARCHAR(50),
    action_value VARCHAR(100),

    pos_list VARCHAR(200),

    description VARCHAR(255),

    -- ✅ NEW COLUMN (Rule Book - human readable)
    rule_book VARCHAR(500),

    comments VARCHAR(500) NULL,

    created_at DATETIME DEFAULT GETDATE()
);

INSERT INTO rules_engine 
(NonFacFee_condition, FacFee_condition, Fee_Compare, Codeid_condition, specialtycode,
 action_type, action_value, pos_list, description, rule_book, comments)
VALUES

-- 1
(NULL, NULL, NULL, 'CPT/HCPCS', '7170',
 'LOAD', 'APPLY_SPECIALTY', NULL,
 'Load specialty',
 'For all CPT/HCPCS codes, assign specialty code 7170',
 NULL),

-- 2
(NULL, NULL, NULL, '99001', '7170',
 'FILTER', 'REMOVE', NULL,
 'Remove code',
 'If code is 99001, remove it from processing',
 NULL),

-- 3
('IN(0,M,NA)', NULL, NULL, 'CPT/HCPCS', '7170',
 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid NonFac',
 'If Non-Facility Fee is 0 or M or NA, do not load the record',
 NULL),

-- 4
(NULL, 'IN(0,M,NA)', NULL, 'CPT/HCPCS', '7170',
 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid Fac',
 'If Facility Fee is 0 or M or NA, do not load the record',
 NULL),

-- 5
('NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', '0996',
 'LOAD', 'LOAD_SPECIALTY_0996', NULL,
 'Diff rates',
 'If Non-Fac Fee and Fac Fee are different, assign specialty code 0996',
 NULL),

-- 6
('HAS_VALUE', 'M_OR_NA', NULL, 'CPT/HCPCS', '7170',
 'LOAD', 'BLANK_POS', NULL,
 'NonFac present',
 'If Non-Fac Fee has value and Fac Fee is M or NA, keep POS blank',
 NULL),

-- 7
('NA', 'HAS_VALUE', NULL, 'CPT/HCPCS', '7170',
 'LOAD', 'LOAD_POS', '19,21,22,23,24,31,34,61,62',
 'Fac only',
 'If only Fac Fee is present, load POS values: 19,21,22,23,24,31,34,61,62',
 NULL),

-- 8
('NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', '7170',
 'LOAD', 'LOAD_POS_WITH_FAC', '19,21,22,23,24,31,34,61,62',
 'Diff rates POS',
 'If Non-Fac and Fac fees are different, load POS list with Fac Fee',
 NULL);


INSERT INTO rules_engine 
(fee_id, flag, NonFacFee_condition, FacFee_condition, Fee_Compare, Codeid_condition, age_rule,
 specialtycode, action_type, action_value, pos_list, description, rule_book, comments)
VALUES

-- 1
('DV00052801','CNM', NULL, NULL, NULL, 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'APPLY_SPECIALTY', NULL,
 'Load specialty',
 'For CNM, assign specialty code 8036',
 NULL),

-- 2
('DV00052801','CNM', NULL, NULL, NULL, '99001', NULL,
 '8036', 'FILTER', 'REMOVE', NULL,
 'Remove code',
 'If code is 99001, remove it',
 NULL),

-- 3
('DV00052801','CNM', 'IN(0,M,NA)', NULL, NULL, 'CPT/HCPCS', NULL,
 '8036', 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid NonFac',
 'If Non-Fac Fee is 0 or M or NA, do not load',
 NULL),

-- 4
('DV00052801','CNM', 'NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', NULL,
 '0996', 'LOAD', 'LOAD_SPECIALTY_0996', NULL,
 'Diff rates',
 'If fees differ, assign specialty 0996 and blank POS',
 NULL),

-- 5
('DV00052801','CNM', 'HAS_VALUE', 'M_OR_NA', NULL, 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'BLANK_POS', NULL,
 'NonFac present',
 'If Non-Fac exists and Fac is NA, keep POS blank',
 NULL),

-- 6
('DV00052801','CNM', NULL, 'IN(0,M,NA)', NULL, 'CPT/HCPCS', NULL,
 '8036', 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid Fac',
 'If Fac Fee is 0 or M or NA, do not load',
 NULL),

-- 7
('DV00052801','CNM', 'NA', 'HAS_VALUE', NULL, 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'LOAD_POS', '19,21,22,23,24,31,34,61,62',
 'Fac only',
 'If only Fac Fee exists, load POS list',
 NULL),

-- 8
('DV00052801','CNM', 'NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'LOAD_POS_WITH_FAC', '19,21,22,23,24,31,34,61,62',
 'Diff rates POS',
 'If fees differ, load POS with Fac Fee',
 NULL),

-- 9
('DV00052801','CNM', NULL, NULL, NULL, 'CPT/HCPCS', 'AGE<=21',
 '8036', 'LOAD', 'ALLOW', NULL,
 'Age rule',
 'Only allow records where age is up to 21',
 NULL);

 SELECT rule_id, fee_id, flag, NonFacFee_condition, FacFee_condition, action_value
FROM rules_engine
WHERE flag = 'CNM';
 

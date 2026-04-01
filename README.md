CREATE TABLE rules_engine (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,

    -- Conditions
    NonFacFee_condition VARCHAR(50),
    FacFee_condition VARCHAR(50),
    Fee_Compare VARCHAR(20),
    Codeid_condition VARCHAR(50),

    -- Context
    specialtycode VARCHAR(20),

    -- Actions
    action_type VARCHAR(50),
    action_value VARCHAR(100),

    -- POS list (important addition)
    pos_list VARCHAR(200),

    -- Description
    description VARCHAR(255),

    -- Comments (as requested)
    comments VARCHAR(500) NULL,

    created_at DATETIME DEFAULT GETDATE()
);




INSERT INTO rules_engine 
(NonFacFee_condition, FacFee_condition, Fee_Compare, Codeid_condition, specialtycode,
 action_type, action_value, pos_list, description, comments)
VALUES

-- 1. Load specialty 7170
(NULL, NULL, NULL, 'CPT/HCPCS', '7170',
 'LOAD', 'APPLY_SPECIALTY', NULL,
 'Load with specialty code 7170', NULL),

-- 2. Remove specific code
(NULL, NULL, NULL, '99001', '7170',
 'FILTER', 'REMOVE', NULL,
 'Remove code 99001', NULL),

-- 3. Invalid NonFacFee
('IN(0,M,NA)', NULL, NULL, 'CPT/HCPCS', '7170',
 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid NonFacFee', NULL),

-- 4. Invalid FacFee
(NULL, 'IN(0,M,NA)', NULL, 'CPT/HCPCS', '7170',
 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid FacFee', NULL),

-- 5. Different rates → load specialty 0996
('NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', '0996',
 'LOAD', 'LOAD_SPECIALTY_0996', NULL,
 'Different rates', NULL),

-- 6. NonFac present, Fac missing → blank POS
('HAS_VALUE', 'M_OR_NA', NULL, 'CPT/HCPCS', '7170',
 'LOAD', 'BLANK_POS', NULL,
 'NonFac present, Fac missing', NULL),

-- 7. Fac only → load POS list
('NA', 'HAS_VALUE', NULL, 'CPT/HCPCS', '7170',
 'LOAD', 'LOAD_POS', '19,21,22,23,24,31,34,61,62',
 'Load only Fac Fee', NULL),

-- 8. Different rates → load POS with Fac Fee
('NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', '7170',
 'LOAD', 'LOAD_POS_WITH_FAC', '19,21,22,23,24,31,34,61,62',
 'Different rates load POS with Fac Fee', NULL);

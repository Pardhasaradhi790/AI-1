ALTER TABLE rules_engine
ADD 
    short_description VARCHAR(255),
    modifier VARCHAR(50);


    UPDATE rules_engine
SET 
    short_description = 'FROM_CLAIM',
    modifier = NULL;


INSERT INTO rules_engine 
(fee_id, flag, NonFacFee_condition, FacFee_condition, Fee_Compare, Codeid_condition, age_rule,
 specialtycode, action_type, action_value, pos_list, description, rule_book, comments,
 short_description, modifier)
VALUES

('DV00052801','CNM', NULL, NULL, NULL, 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'APPLY_SPECIALTY', NULL,
 'Load specialty','For CNM assign specialty 8036', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', NULL, NULL, NULL, '99001', NULL,
 '8036', 'FILTER', 'REMOVE', NULL,
 'Remove code','Remove code 99001', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', 'IN(0,M,NA)', NULL, NULL, 'CPT/HCPCS', NULL,
 '8036', 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid NonFac','Invalid Non-Fac Fee', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', 'NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', NULL,
 '0996', 'LOAD', 'LOAD_SPECIALTY_0996', NULL,
 'Diff rates','Assign 0996 if fees differ', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', 'HAS_VALUE', 'M_OR_NA', NULL, 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'BLANK_POS', NULL,
 'NonFac present','NonFac exists → blank POS', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', NULL, 'IN(0,M,NA)', NULL, 'CPT/HCPCS', NULL,
 '8036', 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid Fac','Invalid Fac Fee', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', 'NA', 'HAS_VALUE', NULL, 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'LOAD_POS', '19,21,22,23,24,31,34,61,62',
 'Fac only','Load POS if only Fac exists', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', 'NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', NULL,
 '8036', 'LOAD', 'LOAD_POS_WITH_FAC', '19,21,22,23,24,31,34,61,62',
 'Diff rates POS','Load POS with Fac Fee', NULL,
 'FROM_CLAIM', NULL),

('DV00052801','CNM', NULL, NULL, NULL, 'CPT/HCPCS', 'AGE<=21',
 '8036', 'LOAD', 'ALLOW', NULL,
 'Age rule','Allow only age <= 21', NULL,
 'FROM_CLAIM', NULL);



 INSERT INTO rules_engine 
(fee_id, flag, NonFacFee_condition, FacFee_condition, Fee_Compare, Codeid_condition, age_rule,
 specialtycode, action_type, action_value, pos_list, description, rule_book, comments,
 short_description, modifier)
VALUES

-- 1. Default specialty
('DV00052801','ORAL', NULL, NULL, NULL, 'CPT/HCPCS', NULL,
 'ORAL', 'LOAD', 'APPLY_SPECIALTY', NULL,
 'Load specialty','Apply ORAL specialty mapping', NULL,
 'FROM_CLAIM', NULL),

-- 2. Remove code 99001
('DV00052801','ORAL', NULL, NULL, NULL, '99001', NULL,
 'ORAL', 'FILTER', 'REMOVE', NULL,
 'Remove code','Remove CPT code 99001', NULL,
 'FROM_CLAIM', NULL),

-- 3. Invalid NonFacFee
('DV00052801','ORAL', 'IN(0,M,NA)', NULL, NULL, 'CPT/HCPCS', NULL,
 'ORAL', 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid NonFac','Skip if Non-Fac Fee is 0/M/NA', NULL,
 'FROM_CLAIM', NULL),

-- 4. Different rates → specialty 0996 + blank POS
('DV00052801','ORAL', 'NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', NULL,
 '0996', 'LOAD', 'LOAD_SPECIALTY_0996', NULL,
 'Diff rates','If fees differ → assign specialty 0996 and blank POS', NULL,
 'FROM_CLAIM', NULL),

-- 5. NonFac present, Fac NA → blank POS
('DV00052801','ORAL', 'HAS_VALUE', 'M_OR_NA', NULL, 'CPT/HCPCS', NULL,
 'ORAL', 'LOAD', 'BLANK_POS', NULL,
 'NonFac present','If NonFac exists and Fac is M/NA → blank POS', NULL,
 'FROM_CLAIM', NULL),

-- 6. Invalid FacFee
('DV00052801','ORAL', NULL, 'IN(0,M,NA)', NULL, 'CPT/HCPCS', NULL,
 'ORAL', 'SKIP', 'DO_NOT_LOAD', NULL,
 'Invalid Fac','Skip if Fac Fee is 0/M/NA', NULL,
 'FROM_CLAIM', NULL),

-- 7. Fac only → POS + specialty 0996
('DV00052801','ORAL', 'NA', 'HAS_VALUE', NULL, 'CPT/HCPCS', NULL,
 '0996', 'LOAD', 'LOAD_POS', '19,21,22,23,24,31,34,61,62',
 'Fac only','If only Fac Fee exists → load POS and specialty 0996', NULL,
 'FROM_CLAIM', NULL),

-- 8. Different rates → POS with Fac Fee
('DV00052801','ORAL', 'NOT_EQUAL', 'NOT_EQUAL', 'NE', 'CPT/HCPCS', NULL,
 'ORAL', 'LOAD', 'LOAD_POS_WITH_FAC', '19,21,22,23,24,31,34,61,62',
 'Diff rates POS','If fees differ → load POS with Fac Fee', NULL,
 'FROM_CLAIM', NULL),

-- 9. Age rule
('DV00052801','ORAL', NULL, NULL, NULL, 'CPT/HCPCS', 'AGE<19_OR_CSHCS',
 'ORAL', 'SKIP', 'DO_NOT_LOAD', NULL,
 'Age rule','Reject if age < 19 or CSHCS-only case', NULL,
 'FROM_CLAIM', NULL);

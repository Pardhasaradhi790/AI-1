CREATE TABLE rules_engine (
    rule_id INT PRIMARY KEY,

    NonFacFee_condition VARCHAR(50),
    FacFee_condition VARCHAR(50),
    Fee_Compare VARCHAR(20),
    Codeid_condition VARCHAR(50),

    action_type VARCHAR(50),
    action_value VARCHAR(100),
    description VARCHAR(255)
);

INSERT INTO rules_engine VALUES
(1, NULL, NULL, NULL, NULL, 'LOAD', 'APPLY_SPECIALTY_7170', 'Load specialty'),

(2, NULL, NULL, NULL, '99001', 'FILTER', 'REMOVE', 'Remove code'),

(3, 'IN(0,M,NA)', NULL, NULL, NULL, 'SKIP', 'DO_NOT_LOAD', 'Invalid NonFac'),

(4, NULL, 'IN(0,M,NA)', NULL, NULL, 'SKIP', 'DO_NOT_LOAD', 'Invalid Fac'),

(5, 'NOT_EQUAL', 'NOT_EQUAL', 'NE', NULL, 'LOAD', 'LOAD_SPECIALTY_0996', 'Diff rates'),

(6, 'HAS_VALUE', 'M_OR_NA', NULL, NULL, 'LOAD', 'BLANK_POS', 'NonFac present'),

(7, 'NA', 'HAS_VALUE', NULL, NULL, 'LOAD', 'LOAD_POS_LIST', 'Fac only'),

(8, 'NOT_EQUAL', 'NOT_EQUAL', 'NE', NULL, 'LOAD', 'LOAD_POS_WITH_FAC', 'Diff rates POS');

SELECT * FROM rules_engine;

# Exercise 3: Data Validation Project

## Scenario

You're building a comprehensive data quality monitoring system for a financial services company. The system must validate data from multiple sources, detect anomalies, ensure regulatory compliance, and provide detailed reporting on data quality metrics. Data quality is critical for regulatory reporting and risk management.

## Business Requirements

1. **Data Sources to Validate**:

   - Transaction data (high volume, real-time)
   - Customer master data (reference data)
   - Market data feeds (external, time-sensitive)
   - Regulatory reporting data (compliance-critical)

2. **Validation Types**:

   - **Schema Validation**: Ensure data conforms to expected structure
   - **Business Rule Validation**: Apply domain-specific validation rules
   - **Data Profiling**: Analyze data characteristics and detect anomalies
   - **Cross-Reference Validation**: Validate data consistency across sources
   - **Temporal Validation**: Check data freshness and sequence

3. **Quality Metrics**:

   - Completeness (missing data percentage)
   - Accuracy (data correctness)
   - Consistency (data uniformity across sources)
   - Validity (data conforms to business rules)
   - Timeliness (data freshness)

4. **Alerting Requirements**:
   - Critical alerts for compliance violations
   - Warning alerts for quality degradation
   - Trend analysis for proactive monitoring
   - Executive dashboards for quality metrics

## Technical Requirements

### Pipeline Structure

Create a DAG named `data_quality_monitoring` with the following structure:

```
load_validation_rules → profile_transaction_data → validate_transactions
                     → profile_customer_data → validate_customers → generate_quality_report
                     → profile_market_data → validate_market_data ↗
                                                                   ↓
                                                            update_quality_dashboard
                                                                   ↓
                                                            send_quality_alerts
```

### Validation Framework

Implement a flexible validation framework that supports:

1. **Rule-Based Validation**:

   - Configurable validation rules stored in database/config
   - Support for SQL-based and Python-based rules
   - Rule versioning and audit trail

2. **Statistical Validation**:

   - Outlier detection using statistical methods
   - Trend analysis and anomaly detection
   - Data distribution analysis

3. **Schema Validation**:
   - JSON Schema validation for structured data
   - Column type and constraint validation
   - Foreign key and referential integrity checks

### Quality Scoring

Implement a comprehensive quality scoring system:

- Overall quality score (0-100)
- Dimension-specific scores (completeness, accuracy, etc.)
- Trend analysis and historical comparison
- Threshold-based alerting

## Implementation Steps

### Step 1: Design validation rule framework

Create a flexible system for defining and managing validation rules:

- Rule definition schema
- Rule storage and retrieval
- Rule execution engine
- Rule result tracking

### Step 2: Implement data profiling

Build comprehensive data profiling capabilities:

- Statistical analysis (min, max, mean, std dev)
- Data type analysis and inference
- Null value analysis
- Unique value analysis
- Pattern detection

### Step 3: Create validation engines

Implement different types of validation:

- Schema validation engine
- Business rule validation engine
- Statistical anomaly detection
- Cross-reference validation

### Step 4: Build quality scoring system

Develop quality metrics calculation:

- Individual rule scoring
- Dimension-level aggregation
- Overall quality score calculation
- Historical trend analysis

### Step 5: Implement alerting system

Create intelligent alerting:

- Threshold-based alerts
- Trend-based alerts
- Alert prioritization and routing
- Alert suppression and escalation

### Step 6: Create quality dashboard

Build monitoring and reporting:

- Real-time quality metrics
- Historical trend analysis
- Drill-down capabilities
- Executive summary reports

## Sample Data and Rules

### Transaction Data Sample

```csv
transaction_id,customer_id,account_id,amount,currency,transaction_date,transaction_type,status
TXN001,CUST001,ACC001,1500.00,USD,2024-01-15 10:30:00,TRANSFER,COMPLETED
TXN002,CUST002,ACC002,-250.00,USD,2024-01-15 11:15:00,WITHDRAWAL,COMPLETED
TXN003,CUST001,ACC001,75.50,USD,2024-01-15 12:00:00,PURCHASE,PENDING
TXN004,,ACC003,10000.00,USD,2024-01-15 13:45:00,DEPOSIT,COMPLETED
TXN005,CUST003,ACC004,999999.99,USD,2024-01-15 14:20:00,TRANSFER,FAILED
```

### Validation Rules Examples

```json
{
  "rules": [
    {
      "rule_id": "TXN_001",
      "name": "Transaction ID Format",
      "description": "Transaction ID must follow TXN### pattern",
      "type": "regex",
      "pattern": "^TXN\\d{3}$",
      "severity": "ERROR",
      "dimension": "validity"
    },
    {
      "rule_id": "TXN_002",
      "name": "Amount Range Check",
      "description": "Transaction amount must be between -10000 and 10000",
      "type": "range",
      "min_value": -10000,
      "max_value": 10000,
      "severity": "WARNING",
      "dimension": "validity"
    },
    {
      "rule_id": "TXN_003",
      "name": "Customer ID Required",
      "description": "Customer ID is required for all transactions",
      "type": "not_null",
      "severity": "ERROR",
      "dimension": "completeness"
    }
  ]
}
```

### Customer Data Sample

```csv
customer_id,first_name,last_name,email,phone,date_of_birth,account_status,risk_rating
CUST001,John,Doe,john.doe@email.com,+1-555-0123,1985-03-15,ACTIVE,LOW
CUST002,Jane,Smith,jane.smith@email.com,+1-555-0124,1990-07-22,ACTIVE,MEDIUM
CUST003,Bob,Johnson,invalid-email,+1-555-0125,2010-01-01,SUSPENDED,HIGH
CUST004,Alice,Brown,alice.brown@email.com,,1975-11-30,ACTIVE,LOW
```

## Expected Outputs

1. **Data Quality Report**: Comprehensive report showing quality metrics by dimension
2. **Validation Results**: Detailed results for each validation rule
3. **Anomaly Report**: Statistical anomalies and outliers detected
4. **Quality Dashboard Data**: Metrics formatted for dashboard consumption
5. **Alert Summary**: Critical and warning alerts generated

## Validation Criteria

Your implementation should:

- [ ] Load and parse validation rules from configuration
- [ ] Profile data to understand characteristics and patterns
- [ ] Execute all validation rules against sample data
- [ ] Calculate quality scores by dimension and overall
- [ ] Detect statistical anomalies and outliers
- [ ] Generate comprehensive quality reports
- [ ] Create appropriate alerts based on validation results
- [ ] Handle validation failures gracefully
- [ ] Provide detailed logging for audit purposes
- [ ] Support extensible rule framework

## Extension Challenges

1. **Machine Learning Integration**: Use ML models for anomaly detection
2. **Real-time Validation**: Implement streaming data validation
3. **Data Lineage Tracking**: Track data quality through transformation pipeline
4. **Automated Rule Discovery**: Automatically suggest validation rules
5. **Quality Improvement Recommendations**: Suggest data quality improvements

## Quality Dimensions Explained

### Completeness

- **Definition**: Percentage of non-null values
- **Calculation**: (Non-null values / Total values) × 100
- **Thresholds**: Critical < 90%, Warning < 95%

### Accuracy

- **Definition**: Percentage of values that are correct
- **Calculation**: (Correct values / Total values) × 100
- **Validation**: Business rule compliance, format validation

### Consistency

- **Definition**: Data uniformity across sources and time
- **Calculation**: Consistency score based on cross-reference checks
- **Examples**: Same customer data across systems

### Validity

- **Definition**: Data conforms to defined formats and constraints
- **Calculation**: (Valid values / Total values) × 100
- **Examples**: Email format, date ranges, enum values

### Timeliness

- **Definition**: Data is up-to-date and available when needed
- **Calculation**: Based on data freshness and SLA compliance
- **Thresholds**: Critical > 24 hours old, Warning > 12 hours old

## Common Data Quality Issues

1. **Missing Values**: Null or empty required fields
2. **Format Violations**: Invalid email, phone, date formats
3. **Outliers**: Values significantly different from normal range
4. **Duplicates**: Duplicate records or keys
5. **Inconsistencies**: Same entity with different values across sources
6. **Stale Data**: Data that hasn't been updated recently
7. **Referential Integrity**: Foreign key violations

## Alerting Strategy

### Alert Levels

- **CRITICAL**: Immediate action required (compliance violations)
- **WARNING**: Attention needed (quality degradation)
- **INFO**: Informational (trend changes)

### Alert Routing

- **Critical**: Immediate notification to on-call team
- **Warning**: Daily digest to data stewards
- **Info**: Weekly summary to management

### Alert Suppression

- Avoid alert fatigue with intelligent suppression
- Group related alerts
- Escalation for unacknowledged critical alerts

## Resources

- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Data Quality Patterns](https://www.datakitchen.io/blog/data-quality-patterns)
- [Statistical Process Control](https://en.wikipedia.org/wiki/Statistical_process_control)
- [JSON Schema Validation](https://json-schema.org/)

## Time Estimate

- Basic implementation: 120-150 minutes
- With extensions: 180-240 minutes

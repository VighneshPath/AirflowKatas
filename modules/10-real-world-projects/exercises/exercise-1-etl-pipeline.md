# Exercise 1: Complete ETL Pipeline

## Scenario

You're working for an e-commerce company that needs to build a daily sales reporting pipeline. The pipeline must extract data from multiple sources, transform it for analytics, and load it into a data warehouse while ensuring data quality throughout the process.

## Business Requirements

1. **Data Sources**:

   - Sales transactions (CSV files from POS systems)
   - Product catalog (JSON API)
   - Customer data (PostgreSQL database)

2. **Transformations**:

   - Calculate daily sales metrics by product and region
   - Enrich transactions with product and customer information
   - Handle missing or invalid data gracefully

3. **Data Quality**:

   - Validate data completeness and accuracy
   - Flag anomalies (e.g., unusually high/low sales)
   - Generate data quality reports

4. **Output**:
   - Load processed data to data warehouse
   - Generate summary reports for business stakeholders
   - Archive raw data for compliance

## Technical Requirements

### Pipeline Structure

Create a DAG named `daily_sales_etl` with the following task structure:

```
extract_sales_data → validate_sales_data → transform_sales_data
extract_product_data → validate_product_data ↗
extract_customer_data → validate_customer_data ↗
                                                ↓
                                        load_to_warehouse → generate_reports
                                                ↓
                                        archive_raw_data
```

### Data Validation Rules

Implement the following validation checks:

1. **Sales Data**: Non-null transaction_id, positive amount, valid date
2. **Product Data**: Valid product_id, non-empty name, positive price
3. **Customer Data**: Valid customer_id, non-empty email format

### Error Handling

- Retry failed tasks up to 3 times with exponential backoff
- Send email alerts for validation failures
- Continue processing even if some data sources fail (partial success)

## Implementation Steps

### Step 1: Set up the DAG structure

Create the main DAG file with proper configuration:

- Schedule: Daily at 2 AM
- Catchup: False
- Max active runs: 1
- Default retry: 3 times

### Step 2: Implement extraction tasks

Create tasks to extract data from each source:

- `extract_sales_data`: Read CSV files from `/data/sales/` directory
- `extract_product_data`: Fetch product catalog from mock API
- `extract_customer_data`: Query customer database

### Step 3: Implement validation tasks

Create validation tasks for each data source:

- Use custom validation functions
- Log validation results
- Raise exceptions for critical failures

### Step 4: Implement transformation task

Create a transformation task that:

- Joins data from all sources
- Calculates daily metrics
- Handles missing data appropriately

### Step 5: Implement loading tasks

Create tasks to:

- Load transformed data to warehouse
- Generate business reports
- Archive raw data

### Step 6: Add monitoring and alerting

Implement:

- Custom metrics collection
- Email notifications for failures
- Data quality dashboard updates

## Sample Data

Use the provided sample data files:

- `data/sales/sales_2024_01_15.csv`
- `data/products/products.json`
- Mock customer database with sample records

## Expected Outputs

1. **Transformed Data**: Clean, enriched sales data in warehouse format
2. **Quality Report**: Summary of validation results and data statistics
3. **Business Report**: Daily sales summary by product and region
4. **Archived Data**: Compressed raw data files with timestamps

## Validation Criteria

Your implementation should:

- [ ] Handle all three data sources correctly
- [ ] Implement comprehensive data validation
- [ ] Transform data according to business requirements
- [ ] Load data to the target warehouse
- [ ] Generate required reports
- [ ] Handle errors gracefully with proper logging
- [ ] Include monitoring and alerting capabilities

## Extension Challenges

Once you complete the basic implementation, try these extensions:

1. Add incremental processing (only process new/changed data)
2. Implement data lineage tracking
3. Add automated data quality scoring
4. Create a data catalog entry for the pipeline output

## Resources

- [Airflow ETL Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Data Quality Patterns](https://www.datakitchen.io/blog/data-quality-patterns)
- [ETL vs ELT Considerations](https://www.integrate.io/blog/etl-vs-elt/)

## Time Estimate

- Basic implementation: 60-90 minutes
- With extensions: 120-150 minutes

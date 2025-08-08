# Exercise 2: API Integration Project

## Scenario

You're building a customer data synchronization pipeline that integrates with multiple external APIs to keep your customer database up-to-date. The pipeline needs to handle authentication, rate limiting, error recovery, and data transformation while ensuring data consistency.

## Business Requirements

1. **Data Sources**:

   - Customer API (REST) - Customer profile updates
   - Payment API (REST) - Payment method information
   - Marketing API (GraphQL) - Campaign engagement data
   - Webhook endpoint - Real-time event notifications

2. **Integration Challenges**:

   - Different authentication methods (API keys, OAuth2, JWT)
   - Rate limiting (100 requests/minute for Customer API, 50/minute for Payment API)
   - Intermittent API failures requiring intelligent retry logic
   - Data format differences requiring transformation

3. **Data Processing**:

   - Merge customer data from multiple sources
   - Detect and resolve data conflicts
   - Maintain audit trail of all changes
   - Handle partial failures gracefully

4. **Output Requirements**:
   - Update customer database with merged data
   - Generate data quality reports
   - Send notifications for critical failures
   - Archive API responses for compliance

## Technical Requirements

### Pipeline Structure

Create a DAG named `customer_data_sync` with the following structure:

```
check_api_health → fetch_customer_updates → validate_customer_data
                → fetch_payment_data → validate_payment_data → merge_customer_data
                → fetch_marketing_data → validate_marketing_data ↗
                                                                ↓
                                                        update_database → generate_reports
                                                                ↓
                                                        send_notifications
```

### API Integration Requirements

1. **Authentication Handling**:

   - Customer API: API key in header
   - Payment API: OAuth2 with token refresh
   - Marketing API: JWT token with expiration handling

2. **Rate Limiting**:

   - Implement exponential backoff for rate limit errors
   - Track API usage to prevent hitting limits
   - Queue requests when approaching limits

3. **Error Handling**:
   - Retry transient failures (5xx errors) up to 3 times
   - Handle authentication failures by refreshing tokens
   - Log all API errors with context for debugging

### Data Quality Requirements

- Validate API response schemas
- Check for required fields and data types
- Detect and flag suspicious data changes
- Maintain data lineage for audit purposes

## Implementation Steps

### Step 1: Set up API connections and authentication

Create Airflow connections for each API:

- Configure authentication credentials
- Set up connection pooling
- Implement token refresh mechanisms

### Step 2: Implement API health checks

Create a task to verify all APIs are accessible:

- Check API endpoints are responding
- Validate authentication is working
- Verify rate limit status

### Step 3: Build data fetching tasks

Implement tasks for each API:

- Handle pagination for large datasets
- Implement rate limiting logic
- Add comprehensive error handling

### Step 4: Create data validation tasks

Build validation for each data source:

- Schema validation using JSON schemas
- Business rule validation
- Data quality scoring

### Step 5: Implement data merging logic

Create intelligent data merging:

- Conflict resolution strategies
- Data precedence rules
- Change detection and logging

### Step 6: Build database update task

Implement database operations:

- Upsert operations for customer records
- Transaction management for consistency
- Rollback capabilities for failures

### Step 7: Add monitoring and alerting

Implement comprehensive monitoring:

- API performance metrics
- Data quality metrics
- Failure notifications

## Mock API Setup

For this exercise, we'll simulate the APIs using mock endpoints:

### Customer API Mock

```python
# Mock responses for customer API
CUSTOMER_API_RESPONSES = [
    {
        "customer_id": "CUST001",
        "email": "john.doe@email.com",
        "first_name": "John",
        "last_name": "Doe",
        "phone": "+1-555-0123",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345"
        },
        "last_updated": "2024-01-15T10:30:00Z"
    }
]
```

### Payment API Mock

```python
# Mock responses for payment API
PAYMENT_API_RESPONSES = [
    {
        "customer_id": "CUST001",
        "payment_methods": [
            {
                "id": "pm_123",
                "type": "credit_card",
                "last_four": "4242",
                "brand": "visa",
                "is_default": True
            }
        ],
        "billing_address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345"
        }
    }
]
```

### Marketing API Mock

```python
# Mock responses for marketing API
MARKETING_API_RESPONSES = [
    {
        "customer_id": "CUST001",
        "campaigns": [
            {
                "campaign_id": "CAMP001",
                "name": "Summer Sale",
                "engagement_score": 85,
                "last_interaction": "2024-01-14T15:20:00Z",
                "status": "active"
            }
        ],
        "preferences": {
            "email_marketing": True,
            "sms_marketing": False,
            "push_notifications": True
        }
    }
]
```

## Expected Outputs

1. **Merged Customer Data**: Complete customer profiles with data from all sources
2. **API Performance Report**: Response times, success rates, error counts
3. **Data Quality Report**: Validation results, conflict resolutions, data completeness
4. **Audit Log**: All API calls, data changes, and system events

## Validation Criteria

Your implementation should:

- [ ] Successfully authenticate with all mock APIs
- [ ] Handle rate limiting gracefully
- [ ] Implement proper retry logic for failures
- [ ] Validate all API responses against schemas
- [ ] Merge data from multiple sources correctly
- [ ] Handle data conflicts intelligently
- [ ] Update database with merged data
- [ ] Generate comprehensive reports
- [ ] Include proper error handling and logging
- [ ] Send appropriate notifications for failures

## Extension Challenges

1. **Real-time Processing**: Add webhook handling for real-time updates
2. **Data Versioning**: Implement customer data versioning and history
3. **Conflict Resolution UI**: Create interface for manual conflict resolution
4. **Performance Optimization**: Add caching and batch processing
5. **Multi-tenant Support**: Handle multiple customer tenants

## Common Pitfalls to Avoid

1. **Authentication Issues**:

   - Not handling token expiration
   - Hardcoding credentials in code
   - Missing error handling for auth failures

2. **Rate Limiting Problems**:

   - Not implementing backoff strategies
   - Ignoring rate limit headers
   - Overwhelming APIs with concurrent requests

3. **Data Quality Issues**:

   - Not validating API responses
   - Ignoring schema changes
   - Poor conflict resolution logic

4. **Error Handling Gaps**:
   - Not distinguishing between retryable and non-retryable errors
   - Missing timeout handling
   - Inadequate logging for debugging

## Resources

- [Airflow HTTP Provider](https://airflow.apache.org/docs/apache-airflow-providers-http/stable/)
- [API Rate Limiting Best Practices](https://cloud.google.com/architecture/rate-limiting-strategies-techniques)
- [OAuth2 with Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [JSON Schema Validation](https://python-jsonschema.readthedocs.io/)

## Time Estimate

- Basic implementation: 90-120 minutes
- With extensions: 150-180 minutes

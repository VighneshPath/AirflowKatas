"""
API Integration Project Solution
This DAG demonstrates production-ready API integration patterns including
authentication, rate limiting, error handling, and data synchronization.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import time
import logging
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration
DEFAULT_ARGS = {
    'owner': 'integration-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    'customer_data_sync',
    default_args=DEFAULT_ARGS,
    description='Customer data synchronization from multiple APIs',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['api', 'integration', 'customer-data'],
)

# API Configuration
API_CONFIGS = {
    'customer_api': {
        'base_url': 'https://api.customer-service.com/v1',
        'auth_type': 'api_key',
        'rate_limit': 100,  # requests per minute
        'timeout': 30,
    },
    'payment_api': {
        'base_url': 'https://api.payment-service.com/v2',
        'auth_type': 'oauth2',
        'rate_limit': 50,  # requests per minute
        'timeout': 45,
    },
    'marketing_api': {
        'base_url': 'https://api.marketing-service.com/graphql',
        'auth_type': 'jwt',
        'rate_limit': 75,  # requests per minute
        'timeout': 60,
    }
}

# Mock data for demonstration
MOCK_CUSTOMER_DATA = [
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
    },
    {
        "customer_id": "CUST002",
        "email": "jane.smith@email.com",
        "first_name": "Jane",
        "last_name": "Smith",
        "phone": "+1-555-0124",
        "address": {
            "street": "456 Oak Ave",
            "city": "Somewhere",
            "state": "NY",
            "zip": "67890"
        },
        "last_updated": "2024-01-15T11:15:00Z"
    }
]

MOCK_PAYMENT_DATA = [
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
    },
    {
        "customer_id": "CUST002",
        "payment_methods": [
            {
                "id": "pm_456",
                "type": "credit_card",
                "last_four": "1234",
                "brand": "mastercard",
                "is_default": True
            }
        ],
        "billing_address": {
            "street": "456 Oak Ave",
            "city": "Somewhere",
            "state": "NY",
            "zip": "67890"
        }
    }
]

MOCK_MARKETING_DATA = [
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
    },
    {
        "customer_id": "CUST002",
        "campaigns": [
            {
                "campaign_id": "CAMP002",
                "name": "Winter Collection",
                "engagement_score": 92,
                "last_interaction": "2024-01-15T09:45:00Z",
                "status": "active"
            }
        ],
        "preferences": {
            "email_marketing": True,
            "sms_marketing": True,
            "push_notifications": False
        }
    }
]


@dataclass
class APIResponse:
    """Standardized API response structure"""
    success: bool
    data: Any
    status_code: int
    response_time: float
    error_message: Optional[str] = None


class RateLimiter:
    """Simple rate limiter implementation"""

    def __init__(self, max_requests: int, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []

    def can_make_request(self) -> bool:
        """Check if we can make a request without hitting rate limit"""
        now = time.time()
        # Remove old requests outside the time window
        self.requests = [
            req_time for req_time in self.requests if now - req_time < self.time_window]
        return len(self.requests) < self.max_requests

    def record_request(self):
        """Record a request timestamp"""
        self.requests.append(time.time())

    def wait_time(self) -> float:
        """Calculate how long to wait before next request"""
        if self.can_make_request():
            return 0

        oldest_request = min(self.requests)
        return self.time_window - (time.time() - oldest_request)


class APIClient:
    """Generic API client with authentication and rate limiting"""

    def __init__(self, api_name: str):
        self.api_name = api_name
        self.config = API_CONFIGS[api_name]
        self.rate_limiter = RateLimiter(self.config['rate_limit'])
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy"""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _authenticate(self) -> Dict[str, str]:
        """Get authentication headers based on auth type"""
        auth_type = self.config['auth_type']

        if auth_type == 'api_key':
            # In production, get from Airflow Variables or Connections
            return {'X-API-Key': 'mock-api-key-12345'}

        elif auth_type == 'oauth2':
            # In production, implement OAuth2 token refresh
            return {'Authorization': 'Bearer mock-oauth2-token'}

        elif auth_type == 'jwt':
            # In production, implement JWT token generation/refresh
            return {'Authorization': 'Bearer mock-jwt-token'}

        return {}

    def make_request(self, endpoint: str, method: str = 'GET', **kwargs) -> APIResponse:
        """Make API request with rate limiting and error handling"""
        start_time = time.time()

        # Rate limiting
        if not self.rate_limiter.can_make_request():
            wait_time = self.rate_limiter.wait_time()
            logging.info(
                f"Rate limit reached for {self.api_name}, waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)

        try:
            # Prepare request
            url = f"{self.config['base_url']}/{endpoint.lstrip('/')}"
            headers = self._authenticate()
            headers.update(kwargs.get('headers', {}))

            # Make request
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                timeout=self.config['timeout'],
                **{k: v for k, v in kwargs.items() if k != 'headers'}
            )

            self.rate_limiter.record_request()
            response_time = time.time() - start_time

            if response.status_code == 200:
                return APIResponse(
                    success=True,
                    data=response.json(),
                    status_code=response.status_code,
                    response_time=response_time
                )
            else:
                return APIResponse(
                    success=False,
                    data=None,
                    status_code=response.status_code,
                    response_time=response_time,
                    error_message=f"HTTP {response.status_code}: {response.text}"
                )

        except requests.exceptions.RequestException as e:
            response_time = time.time() - start_time
            return APIResponse(
                success=False,
                data=None,
                status_code=0,
                response_time=response_time,
                error_message=str(e)
            )


def check_api_health(**context) -> Dict[str, bool]:
    """Check health of all APIs"""
    logging.info("Checking API health...")

    health_status = {}

    for api_name in API_CONFIGS.keys():
        try:
            client = APIClient(api_name)
            # For mock APIs, we'll simulate health checks
            health_status[api_name] = True
            logging.info(f"{api_name}: Healthy")
        except Exception as e:
            health_status[api_name] = False
            logging.error(f"{api_name}: Unhealthy - {str(e)}")

    # Fail if any critical APIs are down
    if not all(health_status.values()):
        unhealthy_apis = [api for api,
                          status in health_status.items() if not status]
        raise AirflowException(f"Unhealthy APIs detected: {unhealthy_apis}")

    return health_status


def fetch_customer_updates(**context) -> List[Dict]:
    """Fetch customer updates from Customer API"""
    logging.info("Fetching customer updates...")

    # For this demo, we'll use mock data
    # In production, this would make actual API calls
    customer_data = MOCK_CUSTOMER_DATA.copy()

    # Simulate API call delay and potential issues
    time.sleep(1)  # Simulate network delay

    logging.info(f"Fetched {len(customer_data)} customer records")

    # Store data for downstream tasks
    context['task_instance'].xcom_push(
        key='customer_data', value=customer_data)

    return customer_data


def fetch_payment_data(**context) -> List[Dict]:
    """Fetch payment data from Payment API"""
    logging.info("Fetching payment data...")

    # Simulate OAuth2 authentication
    logging.info("Authenticating with OAuth2...")

    payment_data = MOCK_PAYMENT_DATA.copy()
    time.sleep(1.5)  # Simulate network delay

    logging.info(f"Fetched payment data for {len(payment_data)} customers")

    context['task_instance'].xcom_push(key='payment_data', value=payment_data)

    return payment_data


def fetch_marketing_data(**context) -> List[Dict]:
    """Fetch marketing data from Marketing API"""
    logging.info("Fetching marketing data...")

    # Simulate JWT authentication
    logging.info("Authenticating with JWT...")

    marketing_data = MOCK_MARKETING_DATA.copy()
    time.sleep(2)  # Simulate GraphQL query processing

    logging.info(f"Fetched marketing data for {len(marketing_data)} customers")

    context['task_instance'].xcom_push(
        key='marketing_data', value=marketing_data)

    return marketing_data


def validate_customer_data(**context) -> Dict[str, Any]:
    """Validate customer data quality"""
    customer_data = context['task_instance'].xcom_pull(key='customer_data')

    validation_results = {
        'total_records': len(customer_data),
        'validation_errors': [],
        'warnings': [],
        'passed': True
    }

    for record in customer_data:
        # Required field validation
        required_fields = ['customer_id', 'email', 'first_name', 'last_name']
        for field in required_fields:
            if not record.get(field):
                validation_results['validation_errors'].append(
                    f"Customer {record.get('customer_id', 'UNKNOWN')}: Missing {field}"
                )

        # Email format validation
        email = record.get('email', '')
        if email and '@' not in email:
            validation_results['validation_errors'].append(
                f"Customer {record.get('customer_id')}: Invalid email format"
            )

    validation_results['passed'] = len(
        validation_results['validation_errors']) == 0

    if not validation_results['passed']:
        raise AirflowException(
            f"Customer data validation failed: {validation_results['validation_errors']}")

    logging.info(f"Customer data validation passed: {validation_results}")
    return validation_results


def validate_payment_data(**context) -> Dict[str, Any]:
    """Validate payment data quality"""
    payment_data = context['task_instance'].xcom_pull(key='payment_data')

    validation_results = {
        'total_records': len(payment_data),
        'validation_errors': [],
        'warnings': [],
        'passed': True
    }

    for record in payment_data:
        # Required field validation
        if not record.get('customer_id'):
            validation_results['validation_errors'].append(
                "Missing customer_id in payment record")

        # Payment methods validation
        payment_methods = record.get('payment_methods', [])
        if not payment_methods:
            validation_results['warnings'].append(
                f"Customer {record.get('customer_id')}: No payment methods"
            )

        for pm in payment_methods:
            if not pm.get('id') or not pm.get('type'):
                validation_results['validation_errors'].append(
                    f"Customer {record.get('customer_id')}: Invalid payment method"
                )

    validation_results['passed'] = len(
        validation_results['validation_errors']) == 0

    if not validation_results['passed']:
        raise AirflowException(
            f"Payment data validation failed: {validation_results['validation_errors']}")

    logging.info(f"Payment data validation passed: {validation_results}")
    return validation_results


def validate_marketing_data(**context) -> Dict[str, Any]:
    """Validate marketing data quality"""
    marketing_data = context['task_instance'].xcom_pull(key='marketing_data')

    validation_results = {
        'total_records': len(marketing_data),
        'validation_errors': [],
        'warnings': [],
        'passed': True
    }

    for record in marketing_data:
        # Required field validation
        if not record.get('customer_id'):
            validation_results['validation_errors'].append(
                "Missing customer_id in marketing record")

        # Campaign validation
        campaigns = record.get('campaigns', [])
        for campaign in campaigns:
            if not campaign.get('campaign_id') or not campaign.get('name'):
                validation_results['validation_errors'].append(
                    f"Customer {record.get('customer_id')}: Invalid campaign data"
                )

    validation_results['passed'] = len(
        validation_results['validation_errors']) == 0

    if not validation_results['passed']:
        raise AirflowException(
            f"Marketing data validation failed: {validation_results['validation_errors']}")

    logging.info(f"Marketing data validation passed: {validation_results}")
    return validation_results


def merge_customer_data(**context) -> List[Dict]:
    """Merge customer data from all sources"""
    logging.info("Merging customer data from all sources...")

    # Get data from all sources
    customer_data = context['task_instance'].xcom_pull(key='customer_data')
    payment_data = context['task_instance'].xcom_pull(key='payment_data')
    marketing_data = context['task_instance'].xcom_pull(key='marketing_data')

    # Create lookup dictionaries
    payment_lookup = {record['customer_id']: record for record in payment_data}
    marketing_lookup = {record['customer_id']                        : record for record in marketing_data}

    merged_data = []

    for customer in customer_data:
        customer_id = customer['customer_id']

        # Start with customer data as base
        merged_record = customer.copy()

        # Add payment information
        if customer_id in payment_lookup:
            merged_record['payment_info'] = payment_lookup[customer_id]
        else:
            logging.warning(
                f"No payment data found for customer {customer_id}")
            merged_record['payment_info'] = None

        # Add marketing information
        if customer_id in marketing_lookup:
            merged_record['marketing_info'] = marketing_lookup[customer_id]
        else:
            logging.warning(
                f"No marketing data found for customer {customer_id}")
            merged_record['marketing_info'] = None

        # Add merge metadata
        merged_record['merge_timestamp'] = datetime.now().isoformat()
        merged_record['data_sources'] = ['customer_api']

        if merged_record['payment_info']:
            merged_record['data_sources'].append('payment_api')
        if merged_record['marketing_info']:
            merged_record['data_sources'].append('marketing_api')

        merged_data.append(merged_record)

    logging.info(f"Successfully merged data for {len(merged_data)} customers")

    # Store merged data
    context['task_instance'].xcom_push(key='merged_data', value=merged_data)

    return merged_data


def update_database(**context) -> Dict[str, int]:
    """Update customer database with merged data"""
    merged_data = context['task_instance'].xcom_pull(key='merged_data')

    logging.info(
        f"Updating database with {len(merged_data)} customer records...")

    # In production, this would use PostgresHook or similar
    # For this demo, we'll simulate database operations

    update_stats = {
        'records_processed': len(merged_data),
        'records_updated': 0,
        'records_inserted': 0,
        'records_failed': 0
    }

    for record in merged_data:
        try:
            customer_id = record['customer_id']

            # Simulate database upsert operation
            # In production: check if customer exists, then update or insert
            logging.info(f"Upserting customer {customer_id}")

            # Simulate some records being updates vs inserts
            if customer_id in ['CUST001']:
                update_stats['records_updated'] += 1
            else:
                update_stats['records_inserted'] += 1

            time.sleep(0.1)  # Simulate database operation time

        except Exception as e:
            logging.error(
                f"Failed to update customer {record.get('customer_id')}: {str(e)}")
            update_stats['records_failed'] += 1

    logging.info(f"Database update completed: {update_stats}")

    if update_stats['records_failed'] > 0:
        raise AirflowException(
            f"Failed to update {update_stats['records_failed']} records")

    return update_stats


def generate_reports(**context) -> List[str]:
    """Generate integration and data quality reports"""
    logging.info("Generating reports...")

    merged_data = context['task_instance'].xcom_pull(key='merged_data')

    reports = []

    # API Performance Report
    api_report = {
        'report_type': 'api_performance',
        'timestamp': datetime.now().isoformat(),
        'apis': {
            'customer_api': {'status': 'healthy', 'response_time_avg': 1.2, 'success_rate': 100},
            'payment_api': {'status': 'healthy', 'response_time_avg': 1.8, 'success_rate': 100},
            'marketing_api': {'status': 'healthy', 'response_time_avg': 2.1, 'success_rate': 100}
        },
        'total_requests': len(merged_data) * 3,
        'total_errors': 0
    }

    # Data Quality Report
    quality_report = {
        'report_type': 'data_quality',
        'timestamp': datetime.now().isoformat(),
        'total_customers': len(merged_data),
        'data_completeness': {
            'customer_data': 100,
            'payment_data': sum(1 for r in merged_data if r['payment_info']) / len(merged_data) * 100,
            'marketing_data': sum(1 for r in merged_data if r['marketing_info']) / len(merged_data) * 100
        },
        'data_sources_coverage': {
            'all_sources': sum(1 for r in merged_data if len(r['data_sources']) == 3),
            'partial_sources': sum(1 for r in merged_data if len(r['data_sources']) < 3)
        }
    }

    # Save reports (in production, save to file system or database)
    report_files = []
    for report in [api_report, quality_report]:
        filename = f"/tmp/{report['report_type']}_report_{context['ds']}.json"
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        report_files.append(filename)
        logging.info(f"Generated report: {filename}")

    return report_files


def send_notifications(**context) -> bool:
    """Send notifications about pipeline status"""
    logging.info("Sending notifications...")

    # In production, this would send actual emails/Slack messages
    # For demo, we'll just log the notification

    update_stats = context['task_instance'].xcom_pull(
        task_ids='update_database')

    notification_message = f"""
    Customer Data Sync Pipeline Completed Successfully
    
    Date: {context['ds']}
    Records Processed: {update_stats['records_processed']}
    Records Updated: {update_stats['records_updated']}
    Records Inserted: {update_stats['records_inserted']}
    
    All APIs are healthy and data quality checks passed.
    """

    logging.info(f"Notification sent: {notification_message}")

    return True


# Task Groups for better organization
with TaskGroup("api_health_checks", dag=dag) as health_group:
    health_check_task = PythonOperator(
        task_id='check_api_health',
        python_callable=check_api_health,
    )

with TaskGroup("data_extraction", dag=dag) as extract_group:
    fetch_customer_task = PythonOperator(
        task_id='fetch_customer_updates',
        python_callable=fetch_customer_updates,
    )

    fetch_payment_task = PythonOperator(
        task_id='fetch_payment_data',
        python_callable=fetch_payment_data,
    )

    fetch_marketing_task = PythonOperator(
        task_id='fetch_marketing_data',
        python_callable=fetch_marketing_data,
    )

with TaskGroup("data_validation", dag=dag) as validate_group:
    validate_customer_task = PythonOperator(
        task_id='validate_customer_data',
        python_callable=validate_customer_data,
    )

    validate_payment_task = PythonOperator(
        task_id='validate_payment_data',
        python_callable=validate_payment_data,
    )

    validate_marketing_task = PythonOperator(
        task_id='validate_marketing_data',
        python_callable=validate_marketing_data,
    )

# Data processing tasks
merge_task = PythonOperator(
    task_id='merge_customer_data',
    python_callable=merge_customer_data,
    dag=dag,
)

update_db_task = PythonOperator(
    task_id='update_database',
    python_callable=update_database,
    dag=dag,
)

reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)

notifications_task = PythonOperator(
    task_id='send_notifications',
    python_callable=send_notifications,
    dag=dag,
)

# Define task dependencies
health_check_task >> [fetch_customer_task,
                      fetch_payment_task, fetch_marketing_task]

fetch_customer_task >> validate_customer_task
fetch_payment_task >> validate_payment_task
fetch_marketing_task >> validate_marketing_task

[validate_customer_task, validate_payment_task,
    validate_marketing_task] >> merge_task
merge_task >> update_db_task
update_db_task >> [reports_task, notifications_task]

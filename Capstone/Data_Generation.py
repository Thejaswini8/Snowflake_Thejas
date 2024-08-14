import pandas as pd
import random
from faker import Faker

fake = Faker()

# Country to currency and city mapping
country_currency_mapping = {
    'United States': {'currency': 'USD', 'cities': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']},
    'India': {'currency': 'INR', 'cities': ['Delhi', 'Mumbai', 'Hyderabad', 'Chennai', 'Bangalore']},
    'United Kingdom': {'currency': 'GBP', 'cities': ['London', 'Birmingham', 'Manchester', 'Glasgow', 'Liverpool']},
    'Japan': {'currency': 'JPY', 'cities': ['Tokyo', 'Osaka', 'Nagoya', 'Sapporo', 'Fukuoka']},
    'Germany': {'currency': 'EUR', 'cities': ['Berlin', 'Hamburg', 'Munich', 'Cologne', 'Frankfurt']}
}

# Transaction data generation
def generate_transaction_data(num_transactions, customers_df, accounts_df):
    transactions = []
    channels = ['online', 'mobile', 'ATM', 'in-branch']
    transaction_types = ['purchase', 'transfer', 'withdrawal', 'deposit']
    merchants = ['Amazon', 'Walmart', 'Target', 'Best Buy', 'Costco']
    categories = ['Retail', 'Grocery', 'Electronics', 'Clothing', 'Miscellaneous']

    for _ in range(num_transactions):
        customer = customers_df.sample(1).iloc[0]
        account = accounts_df[accounts_df['customer_id'] == customer['customer_id']].sample(1).iloc[0]
        
        transaction_id = fake.uuid4()
        customer_id = customer['customer_id']
        transaction_date = fake.date_time_between(start_date='-2y', end_date='now')
        amount = round(random.uniform(1.0, 10000.0), 2)
        country_info = country_currency_mapping.get(customer['country'], {'currency': 'USD', 'cities': ['New York']})
        currency = country_info['currency']
        transaction_type = random.choice(transaction_types)
        channel = random.choice(channels)
        merchant_name = random.choice(merchants)
        merchant_category = random.choice(categories)
        location_country = customer['country']
        location_city = random.choice(country_info['cities'])
        is_flagged = fake.boolean(chance_of_getting_true=5)  # 5% chance of being flagged

        transactions.append({
            'transaction_id': transaction_id,
            'customer_id': customer_id,
            'transaction_date': transaction_date,
            'amount': amount,
            'currency': currency,
            'transaction_type': transaction_type,
            'channel': channel,
            'merchant_name': merchant_name,
            'merchant_category': merchant_category,
            'location_country': location_country,
            'location_city': location_city,
            'is_flagged': is_flagged
        })

    return pd.DataFrame(transactions)

# Customer data generation
def generate_customers(num_customers):
    customers = []
    countries = list(country_currency_mapping.keys())
    for _ in range(num_customers):
        customer_id = fake.uuid4()
        first_name = fake.first_name()
        last_name = fake.last_name()
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90)
        gender = random.choice(['Male', 'Female'])
        email = fake.email()
        phone_number = fake.phone_number()
        address = fake.street_address()
        country = random.choice(countries)
        city = random.choice(country_currency_mapping[country]['cities'])
        occupation = fake.job()
        income_bracket = random.choice(['Low', 'Medium', 'High'])
        customer_since = fake.date_between(start_date='-10y', end_date='now')

        customers.append({
            'customer_id': customer_id,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': date_of_birth,
            'gender': gender,
            'email': email,
            'phone_number': phone_number,
            'address': address,
            'city': city,
            'country': country,
            'occupation': occupation,
            'income_bracket': income_bracket,
            'customer_since': customer_since
        })

    return pd.DataFrame(customers)

# Account data generation
def generate_accounts(customers_df):
    accounts = []
    account_types = ['checking', 'savings', 'credit card', 'loan']
    account_statuses = ['active', 'dormant', 'closed']

    for _, customer in customers_df.iterrows():
        for _ in range(random.randint(1, 3)):  # Each customer can have 1 to 3 accounts
            account_id = fake.uuid4()
            customer_id = customer['customer_id']
            account_type = random.choice(account_types)
            account_status = random.choice(account_statuses)
            open_date = fake.date_between(start_date=customer['customer_since'])
            current_balance = round(random.uniform(0.0, 100000.0), 2)
            currency = country_currency_mapping.get(customer['country'], {'currency': 'USD'})['currency']
            credit_limit = round(random.uniform(1000.0, 50000.0), 2) if account_type == 'credit card' else 0.0

            accounts.append({
                'account_id': account_id,
                'customer_id': customer_id,
                'account_type': account_type,
                'account_status': account_status,
                'open_date': open_date,
                'current_balance': current_balance,
                'currency': currency,
                'credit_limit': credit_limit
            })

    return pd.DataFrame(accounts)

# Credit data generation
def generate_credit_data(customers_df):
    credit_data = []
    for _, customer in customers_df.iterrows():
        credit_score = random.randint(300, 850)
        number_of_credit_accounts = random.randint(1, 10)
        total_credit_limit = round(random.uniform(1000.0, 50000.0), 2)
        total_credit_used = round(random.uniform(0.0, total_credit_limit), 2)
        number_of_late_payments = random.randint(0, 5)
        bankruptcies = random.randint(0, 1)

        credit_data.append({
            'customer_id': customer['customer_id'],
            'credit_score': credit_score,
            'number_of_credit_accounts': number_of_credit_accounts,
            'total_credit_limit': total_credit_limit,
            'total_credit_used': total_credit_used,
            'number_of_late_payments': number_of_late_payments,
            'bankruptcies': bankruptcies
        })

    return pd.DataFrame(credit_data)

# Watchlist data generation
def generate_watchlist_data(num_entities):
    watchlist = []
    entity_types = ['Individual', 'Organization']
    risk_categories = ['Low', 'Medium', 'High']
    sources = ['OFAC', 'UN', 'EU', 'Interpol']

    for _ in range(num_entities):
        entity_id = fake.uuid4()
        entity_name = fake.name() if random.choice(entity_types) == 'Individual' else fake.company()
        entity_type = random.choice(entity_types)
        risk_category = random.choice(risk_categories)
        listed_date = fake.date_between(start_date='-10y', end_date='now')
        source = random.choice(sources)

        watchlist.append({
            'entity_id': entity_id,
            'entity_name': entity_name,
            'entity_type': entity_type,
            'risk_category': risk_category,
            'listed_date': listed_date,
            'source': source
        })

    return pd.DataFrame(watchlist)

# Generate all data
def generate_data(num_customers, num_transactions, num_watchlist_entities):
    customers_df = generate_customers(num_customers)
    accounts_df = generate_accounts(customers_df)
    transactions_df = generate_transaction_data(num_transactions, customers_df, accounts_df)
    credit_data_df = generate_credit_data(customers_df)
    watchlist_data_df = generate_watchlist_data(num_watchlist_entities)

    return customers_df, accounts_df, transactions_df, credit_data_df, watchlist_data_df

# Save data to CSV
def save_data_to_csv(customers_df, accounts_df, transactions_df, credit_data_df, watchlist_data_df):
    customers_df.to_csv('customers1.csv', index=False)
    accounts_df.to_csv('accounts1.csv', index=False)
    transactions_df.to_csv('transactions1.csv', index=False)
    credit_data_df.to_csv('credit_data1.csv', index=False)
    watchlist_data_df.to_csv('watchlist_data1.csv', index=False)

# Main function
if __name__ == "__main__":
    num_customers = 1000
    num_transactions = 10000
    num_watchlist_entities = 100

    customers_df, accounts_df, transactions_df, credit_data_df, watchlist_data_df = generate_data(num_customers, num_transactions, num_watchlist_entities)
    save_data_to_csv(customers_df, accounts_df, transactions_df, credit_data_df, watchlist_data_df)

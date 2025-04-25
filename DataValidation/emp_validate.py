import csv, re, statistics
from datetime import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

def count_null_name_rows(csv_file_path, name_column='name'):
    null_count = 0
    
    with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        if name_column not in reader.fieldnames:
            raise ValueError(f"Column '{name_column}' not found in CSV file")
        
        for row in reader:
            if not row[name_column] or row[name_column].strip() == '':
                null_count += 1
                
    return null_count

def count_missing_contact_records(csv_file):
    missing_count = 0
    
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get('phone', '').strip() and not row.get('email', '').strip():
                missing_count += 1
                
    return missing_count

def count_earlier_2015(csv_file, date_column='hire_date'):
    earlier_2015 = 0
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        if date_column not in reader.fieldnames:
            raise ValueError(f"Date column '{date_column}' not found")
        
        for row in reader:
            try:
                
                hire_date = datetime.strptime(row[date_column].strip(), '%Y-%m-%d').date()
                if hire_date.year < 2015:
                    earlier_2015 += 1
            except ValueError:
                
                earlier_2015 += 1
    
    return earlier_2015

def count_out_of_range_employees(csv_file):
    violations = 0
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                salary = float(row['salary'])
                if salary < 30000 or salary > 200000:
                    violations += 1
            except (ValueError, KeyError):
                
                violations += 1
    
    return violations

def count_hire_after_birth(csv_file):
    violations = 0
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                
                birth_date = datetime.strptime(row['birth_date'].strip(), '%Y-%m-%d')
                hire_date = datetime.strptime(row['hire_date'].strip(), '%Y-%m-%d')
                
                if birth_date >= hire_date:
                    violations += 1
                    
            except (ValueError, KeyError):
                violations += 1
    
    return violations


def count_invalid_postal_codes(csv_file):
    violations = 0
    
    postal_patterns = {
        'US': r'^\d{5}(-\d{4})?$',
        'CA': r'^[A-Z]\d[A-Z] \d[A-Z]\d$',
        'UK': r'^[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}$'
    }
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                country = row['country'].strip().upper()
                postal_code = row['postal_code'].strip().replace(' ', '')
                
                
                if country not in postal_patterns:
                    continue
                    
                
                if not re.match(postal_patterns[country], postal_code, re.IGNORECASE):
                    violations += 1
                    
            except (KeyError, AttributeError):
                
                violations += 1
    
    return violations

def count_invalid_managers(csv_file):
    violations = 0
    employee_ids = set()
    
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                employee_ids.add(row['eid'].strip())
            except KeyError:
                pass
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                reports_to = row['reports_to'].strip()
                
                if reports_to and reports_to not in employee_ids:
                    violations += 1
            except KeyError:
                pass  
    
    return violations

import csv
from datetime import datetime

def count_invalid_reporting_chains(csv_file):
    violations = 0
    employees = {}
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                eid = row['eid']
                hire_date = datetime.strptime(row['hire_date'].strip(), '%Y-%m-%d')
                employees[eid] = hire_date
            except (KeyError, ValueError):
                continue
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                reporter_eid = row['eid']
                manager_eid = row['reports_to']
                
                if not manager_eid or manager_eid not in employees or reporter_eid not in employees:
                    continue
                
                if employees[reporter_eid] < employees[manager_eid]:
                    violations += 1
                    
            except (KeyError, ValueError):
                continue
    
    return violations

def validate_cities_at_least_1(csv_file):
    city_counts = defaultdict(int)
    
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row['city'].strip()
            city_counts[city] += 1
    
    violating_cities = [city for city, count in city_counts.items() if count <= 1]
    
    is_valid = len(violating_cities) == 0
    return is_valid, violating_cities


def validate_salary_median(csv_file):
    salaries = []
    threshold=0.8
    margin=0.2
    
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                salary = float(row['salary'])
                salaries.append(salary)
            except (KeyError, ValueError):
                continue
    
    if not salaries:
        return False, 0.0, 0
    
    median = statistics.median(salaries)
    lower = median * (1 - margin)
    upper = median * (1 + margin)
    
    within_range = sum(lower <= s <= upper for s in salaries)
    percentage = within_range / len(salaries)
    
    is_valid = percentage >= threshold
    return is_valid, percentage, median


def validate_salary_distribution(csv_file):
    salaries = []

    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            try:
                salary = float(row['salary'])
                salaries.append(salary)
            except ValueError:
                continue

    q1 = np.percentile(salaries, 15)
    q3 = np.percentile(salaries, 95)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    filtered_salaries = [s for s in salaries if lower_bound <= s <= upper_bound]

    plt.hist(filtered_salaries, bins=30)
    plt.xlabel("Salary")
    plt.ylabel("Frequency")
    plt.show()


def validate_age_distribution(csv_file, alpha=0.05):
    ages = []
    with open(csv_file, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                birth_date = datetime.strptime(row['birth_date'], '%Y-%m-%d')
                age = (datetime.now() - birth_date).days / 365.25
                ages.append(age)
            except (KeyError, ValueError):
                continue
    
    ages = np.array(ages)

    q1 = np.percentile(ages, 15)
    q3 = np.percentile(ages, 95)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    filtered_ages = [a for a in ages if lower_bound <= a <= upper_bound]

    plt.hist(filtered_ages, bins=30)
    plt.xlabel("Age")
    plt.ylabel("Frequency")
    plt.show()





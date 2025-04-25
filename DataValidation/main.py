import emp_validate

if __name__ == "__main__":
    file_path = "employees.csv"
    # try:
    #     count = emp_validate.count_null_name_rows(file_path)
    #     print(f"Number of rows with null name fields: {count}")
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     count = emp_validate.count_missing_contact_records(file_path)
    #     print(f"Number of rows that have no phone or email: {count}")
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     count = emp_validate.count_earlier_2015(file_path)
    #     print(f"Number of rows with employees hired later than 2015: {count}")
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #    count = emp_validate.count_out_of_range_employees(file_path)
    #    print(f"Number of rows with salaries out of range 30.000, 200.000: {count}")
            
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #    count = emp_validate.count_hire_after_birth(file_path)
    #    print(f"Number of rows with hire date later than birth date: {count}")
            
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     bad_records = emp_validate.count_invalid_postal_codes(file_path)
    #     print(f"Employees with invalid postal codes: {bad_records}")

    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #    count = emp_validate.count_invalid_managers(file_path)
    #    print(f"Number of rows with invalid manager id: {count}")
            
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     bad_relationships = emp_validate.count_invalid_reporting_chains(file_path)
    #     print(f"Number of rows with invalid manager-subordinate relationships: {bad_relationships}")

    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     valid, bad_cities = emp_validate.validate_cities_at_least_1(file_path)
    #     if valid:
    #         print("Data set is valid with respect to this assertion")
    #     else:
    #         print(f"Data set is not valid with respect to this assertion. Violating {len(bad_cities)} cities.")
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     valid = emp_validate.validate_salary_median(file_path)
    #     if valid:
    #         print("Data set is valid with respect to this assertion")
    #     else:
    #         print(f"Data set is not valid with respect to this assertion.")
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     emp_validate.validate_salary_distribution(file_path)
    # except Exception as e:
    #     print(f"Error: {e}")

    try:
        emp_validate.validate_age_distribution(file_path)
    except Exception as e:
        print(f"Error: {e}")
"""
Create Sample Data for PySpark Pipeline
This script creates a CSV file with 10,000 employee records
"""

import csv
import os
from datetime import datetime

def create_sample_data():
    """Generate sample employee data"""
    
    # Create folder if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')
        print(f"✓ Created folder: data/")
    
    # Define CSV file path
    csv_file = 'data/data.csv'
    
    # Create CSV file with 10,000 rows
    print(f"\n⏳ Creating sample data with 10,000 rows...")
    print(f"   This may take a few seconds...\n")
    
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        
        # Write header
        writer.writerow(['id', 'name', 'age', 'salary', 'department', 'join_year'])
        
        # Write 10,000 data rows
        departments = ['IT', 'HR', 'Sales', 'Finance', 'Marketing']
        
        for i in range(1, 100001):
            emp_id = i
            emp_name = f'Employee_{i}'
            emp_age = 22 + (i % 45)  # Ages between 22-67
            emp_salary = 30000 + (i * 50) + (i % 100000)  # Salaries vary
            emp_dept = departments[i % 5]  # Rotate through departments
            emp_year = 2015 + ((i // 2000) % 10)  # Vary join years
            
            writer.writerow([emp_id, emp_name, emp_age, emp_salary, emp_dept, emp_year])
            
            # Progress indicator
            if (i + 1) % 2000 == 0:
                print(f"   {i:,} rows created...", end='\r')
    
    # Print completion message
    print(f"\n✓ DONE! Sample data created successfully!")
    print(f"   File: {os.path.abspath(csv_file)}")
    print(f"   Total rows: 10,000 employee records")
    print(f"   File size: {os.path.getsize(csv_file) / 1024:.2f} KB")
    
    # Print sample preview
    print(f"\n📋 Data Preview:")
    print(f"   Columns: id, name, age, salary, department, join_year")
    print(f"   Sample row: 1, Employee_1, 23, 30050, IT, 2015")
    print(f"\n✓ Ready for PySpark pipeline!\n")

if __name__ == "__main__":
    create_sample_data()
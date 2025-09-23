import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder, StandardScaler

# Step 0: Create Sample Dataset
def create_sample_data(filepath):
    data = {
        'Name': ['John', 'Alice', 'Bob', 'Charlie', 'Diana'],
        'Age': [25, None, 30, 28, 26],
        'Gender': ['Male', 'Female', 'Male', 'Male', 'Female'],
        'Salary': [50000, 60000, None, 52000, 58000],
        'Department': ['IT', 'HR', 'Finance', 'IT', None]
    }
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    print(f"Sample dataset created at {filepath}")

# Step 1: Extract
def extract_data(filepath):
    df = pd.read_csv(filepath)
    print("✅ Data extracted")
    return df

# Step 2: Transform
def transform_data(df):
    imputer = SimpleImputer(strategy='mean')
    df[['Age', 'Salary']] = imputer.fit_transform(df[['Age', 'Salary']])
    
    df['Department'].fillna('Unknown', inplace=True)

    le_gender = LabelEncoder()
    df['Gender'] = le_gender.fit_transform(df['Gender'])

    le_dept = LabelEncoder()
    df['Department'] = le_dept.fit_transform(df['Department'])

    scaler = StandardScaler()
    df[['Age', 'Salary']] = scaler.fit_transform(df[['Age', 'Salary']])

    print("✅ Data transformed")
    return df

# Step 3: Load
def load_data(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"✅ Transformed data saved to {output_path}")

# Run ETL Pipeline
def run_pipeline():
    input_path = "sample_data.csv"
    output_path = "processed_data.csv"
    
    create_sample_data(input_path)
    raw_data = extract_data(input_path)
    processed_data = transform_data(raw_data)
    load_data(processed_data, output_path)

# Execute
run_pipeline()

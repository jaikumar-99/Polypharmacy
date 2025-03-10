# Polypharmacy
**AI-Driven Polypharmacy Risk Prediction – All of Us Hackathon 2024**
Overview
This repository contains the implementation of an AI-driven Polypharmacy Risk Prediction System to detect potential adverse drug interactions and optimize medication safety. The project was developed for the All of Us Hackathon 2024, leveraging biomedical data, machine learning, and predictive analytics to enhance precision medicine.

**Dataset Details**
Total Records: ~300,000 patient medication records
Unique Patients: ~60,000
Data Source: Extracted from All of Us Workbench (BigQuery)
Time Period: Covers multi-year patient prescription history
Key Fields & Features
![image](https://github.com/user-attachments/assets/4341c0c7-385a-4d9c-aecd-3b340cbc706d)

**Technologies Used**
Data Processing & Storage: BigQuery, Pandas, NumPy
Computing Environment: Jupyter Notebook (All of Us Workbench)
Machine Learning: Scikit-learn, XGBoost, TensorFlow
Feature Engineering & Analysis: SHAP (Explainable AI), PCA, One-Hot Encoding
Visualization: Matplotlib, Seaborn
Security & Compliance: HIPAA-compliant environment, All of Us Workbench
Data Cleaning & Processing
Missing Value Handling:
Used median imputation for missing drug dosage values.
Dropped records with excessive missingness.
Duplicate Handling:
Removed duplicate prescriptions for the same drug and patient.
Kept the most recent valid prescription record per patient.
Outlier Detection & Removal:
Applied IQR filtering to remove extreme dosage values.
Cross-validated drug dosages with standard clinical dosage guidelines.
**Key Innovation**
AI-Powered Polypharmacy Risk Model:
Used multi-agent AI models to detect adverse drug-drug interactions.
Integrated SHAP-based explainability to identify high-risk drug combinations.
Developed a Personalized Polypharmacy Risk Score, providing individualized risk assessments for each patient.


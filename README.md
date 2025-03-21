# Polypharmacy
**AI-Driven Polypharmacy Risk Prediction – All of Us Hackathon 2024**
Overview
This repository contains the implementation of an AI-driven Polypharmacy Risk Prediction System to detect potential adverse drug interactions and optimize medication safety. The project was developed for the All of Us Hackathon 2024, leveraging biomedical data, machine learning, and predictive analytics to enhance precision medicine.

**Dataset Details**
Total Records: ~**300,000** patient medication records
Unique Patients: ~60,000
Data Source: Extracted from All of Us Workbench (BigQuery)
Time Period: Covers multi-year patient prescription history
Key Fields & Features
![image](https://github.com/user-attachments/assets/4341c0c7-385a-4d9c-aecd-3b340cbc706d)

**Technologies Used**

language:Python

Architecture: MAS

Data Processing & Storage:**BigQuery, Pandas, NumPy**

![image](https://github.com/user-attachments/assets/e3e531f6-df9e-479c-aac8-2dac8673b1f7)



Computing Environment: Jupyter Notebook (All of Us Workbench)


Machine Learning: **Scikit-learn, XGBoost, TensorFlow**

Feature Engineering & Analysis: **SHAP (Explainable AI), PCA, One-Hot Encoding**

Visualization: **Matplotlib, Seaborn**
Security & Compliance: HIPAA-compliant environment, All of Us Workbench


**Data Cleaning & Processing
Missing Value Handling:**
Used median imputation for missing drug dosage values.
Dropped records with excessive missingness.

**Duplicate Handling:**
Removed duplicate prescriptions for the same drug and patient.
Kept the most recent valid prescription record per patient.

**Outlier Detection & Removal:**
Applied IQR filtering to remove extreme dosage values.
Cross-validated drug dosages with standard clinical dosage guidelines.

**Machine Learning Algorithms Used**
**Logistic Regression**: Baseline classification model for risk prediction.
**Random Forest Classifier**: Used for initial feature selection and importance ranking.
**XGBoost**: Optimized gradient boosting model for improved prediction accuracy.
**Neural Networks** (TensorFlow/Keras): Tested deep learning models for multi-drug interaction predictions.
**K-Means Clustering**: Identified patient subgroups based on prescription patterns and risk profiles.
**SHAP (SHapley Additive exPlanations)**: Used for model explainability and feature impact analysis.
**Principal Component Analysis (PCA)**: Dimensionality reduction to improve computational efficiency.

**Key Innovation**
AI-Powered Polypharmacy Risk Model:
Used ML models to detect adverse drug-drug interactions.
Integrated SHAP-based explainability to identify high-risk drug combinations.
Developed a Personalized Polypharmacy Risk Score, providing individualized risk assessments for each patient.


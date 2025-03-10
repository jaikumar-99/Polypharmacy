#!/usr/bin/env python
# coding: utf-8

# # Demographics

# In[1]:


import pandas
import os

# This query represents dataset "Diabetes-J1" for domain "person" and was generated for All of Us Registered Tier Dataset v7
dataset_12374778_person_sql = """
    SELECT
        person.person_id,
        person.gender_concept_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        person.race_concept_id,
        p_race_concept.concept_name as race,
        person.ethnicity_concept_id,
        p_ethnicity_concept.concept_name as ethnicity,
        person.sex_at_birth_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.person` person 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_race_concept 
            ON person.race_concept_id = p_race_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_ethnicity_concept 
            ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id       
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                        WHERE
                            concept_id IN (4274025)       
                            AND full_text LIKE '%_rank1]%'      ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 1 
                        AND is_selectable = 1) 
                    AND is_standard = 1 ) 
                    AND visit_concept_id IN (8717) ) criteria 
            GROUP BY
                criteria.person_id, criteria.concept_id 
            HAVING
                COUNT(criteria.person_id) >= 3 ) )"""

dataset_12374778_person_df = pandas.read_gbq(
    dataset_12374778_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12374778_person_df.head(5)


# In[2]:


# Person columns
dataset_12374778_person_df.columns


# In[3]:


demo_columns = ['person_id', 'gender', 'date_of_birth', 'race', 'ethnicity']

person_df = dataset_12374778_person_df[demo_columns]

person_df.head()


# In[4]:


import datetime
import pandas as pd
person_df['age'] =  pd.Timestamp.now().year - person_df['date_of_birth'].dt.year

person_df


# In[5]:


person_df.info()


# In[6]:


person_df1 = person_df.drop(['date_of_birth'], axis=1)


# In[7]:


person_df1


# In[ ]:





# # Drug

# In[8]:


import pandas
import os

# This query represents dataset "Diabetes-J1" for domain "drug" and was generated for All of Us Registered Tier Dataset v7
dataset_12374778_drug_sql = """
    SELECT
        d_exposure.person_id,
        d_exposure.drug_concept_id,
        d_standard_concept.concept_name as standard_concept_name,
        d_standard_concept.concept_code as standard_concept_code,
        d_standard_concept.vocabulary_id as standard_vocabulary,
        d_exposure.drug_exposure_start_datetime,
        d_exposure.drug_exposure_end_datetime,
        d_exposure.verbatim_end_date,
        d_exposure.drug_type_concept_id,
        d_type.concept_name as drug_type_concept_name,
        d_exposure.stop_reason,
        d_exposure.refills,
        d_exposure.quantity,
        d_exposure.days_supply,
        d_exposure.sig,
        d_exposure.route_concept_id,
        d_route.concept_name as route_concept_name,
        d_exposure.lot_number,
        d_exposure.visit_occurrence_id,
        d_visit.concept_name as visit_occurrence_concept_name,
        d_exposure.drug_source_value,
        d_exposure.drug_source_concept_id,
        d_source_concept.concept_name as source_concept_name,
        d_source_concept.concept_code as source_concept_code,
        d_source_concept.vocabulary_id as source_vocabulary,
        d_exposure.route_source_value,
        d_exposure.dose_unit_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.drug_exposure` d_exposure 
        WHERE
            (
                drug_concept_id IN (SELECT
                    DISTINCT ca.descendant_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria_ancestor` ca 
                JOIN
                    (SELECT
                        DISTINCT c.concept_id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c       
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id             
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr             
                        WHERE
                            concept_id IN (21600046, 21601136, 21601238, 21603216, 21603551, 21604254)             
                            AND full_text LIKE '%_rank1]%'       ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 1 
                        AND is_selectable = 1) b 
                        ON (ca.ancestor_id = b.concept_id)))  
                    AND (d_exposure.PERSON_ID IN (SELECT
                        distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (4274025)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 ) 
                            AND age_at_event >= 40  
                            AND visit_concept_id IN (8717) ) criteria 
                    GROUP BY
                        criteria.person_id, criteria.concept_id 
                    HAVING
                        COUNT(criteria.person_id) >= 3 ) )
            )) d_exposure 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_standard_concept 
            ON d_exposure.drug_concept_id = d_standard_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_type 
            ON d_exposure.drug_type_concept_id = d_type.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_route 
            ON d_exposure.route_concept_id = d_route.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
            ON d_exposure.visit_occurrence_id = v.visit_occurrence_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_visit 
            ON v.visit_concept_id = d_visit.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` d_source_concept 
            ON d_exposure.drug_source_concept_id = d_source_concept.concept_id"""

dataset_12374778_drug_df = pandas.read_gbq(
    dataset_12374778_drug_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12374778_drug_df.head(5)


# In[9]:


dataset_12374778_drug_df.columns


# In[10]:


drug_cols = ['person_id','standard_concept_name','drug_exposure_start_datetime', 'drug_exposure_end_datetime',
            'refills', 'quantity', 'days_supply']

drug_df = dataset_12374778_drug_df[drug_cols]
drug_df.head()


# In[11]:


drug_df.shape


# In[12]:


drug_df.info()


# In[13]:


# Top 10 Drugs 
drug_df_distinct_users = drug_df.drop_duplicates(subset=['person_id'])
drug_df_distinct_users['standard_concept_name'].value_counts()[:50]


# # Conditions

# In[14]:


import pandas
import os

# This query represents dataset "Diabetes-J1" for domain "condition" and was generated for All of Us Registered Tier Dataset v7
dataset_12374778_condition_sql = """
    SELECT
        c_occurrence.person_id,
        c_occurrence.condition_concept_id,
        c_standard_concept.concept_name as standard_concept_name,
        c_standard_concept.concept_code as standard_concept_code,
        c_standard_concept.vocabulary_id as standard_vocabulary,
        c_occurrence.condition_start_datetime,
        c_occurrence.condition_end_datetime,
        c_occurrence.condition_type_concept_id,
        c_type.concept_name as condition_type_concept_name,
        c_occurrence.stop_reason,
        c_occurrence.visit_occurrence_id,
        visit.concept_name as visit_occurrence_concept_name,
        c_occurrence.condition_source_value,
        c_occurrence.condition_source_concept_id,
        c_source_concept.concept_name as source_concept_name,
        c_source_concept.concept_code as source_concept_code,
        c_source_concept.vocabulary_id as source_vocabulary,
        c_occurrence.condition_status_source_value,
        c_occurrence.condition_status_concept_id,
        c_status.concept_name as condition_status_concept_name 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (4094294, 4199402, 4274025)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                c_occurrence.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (4274025)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 ) 
                            AND age_at_event >= 40  
                            AND visit_concept_id IN (8717) ) criteria 
                    GROUP BY
                        criteria.person_id, criteria.concept_id 
                    HAVING
                        COUNT(criteria.person_id) >= 3 ) )
            )) c_occurrence 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
            ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
            ON c_occurrence.condition_type_concept_id = c_type.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
            ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
            ON v.visit_concept_id = visit.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
            ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
            ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

dataset_12374778_condition_df = pandas.read_gbq(
    dataset_12374778_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12374778_condition_df.head(5)


# In[15]:


dataset_12374778_condition_df['standard_concept_name'].value_counts()[:50]

## Essential hypertension
## Hyperlipidemia
## Type 2 diabetes mellitus
## Type 2 diabetes mellitus without complication
## Obesity
## Hyperglycemia due to type 2 diabetes mellitus
## Hypothyroidism


# In[16]:


## Essential hypertension
## Hyperlipidemia
## Type 2 diabetes mellitus
## Type 2 diabetes mellitus without complication
## Obesity
## Hyperglycemia due to type 2 diabetes mellitus
## Hypothyroidism


# In[17]:


dataset_12374778_condition_df.columns


# In[18]:


condition_cols = ['person_id', 'standard_concept_name', 'condition_start_datetime', 'condition_end_datetime','source_concept_name']
type2_df = dataset_12374778_condition_df[condition_cols]
df_diabetes1 = type2_df[type2_df['standard_concept_name'] == 'Type 2 diabetes mellitus']
df_diabetes2 = type2_df[type2_df['standard_concept_name'] == 'Type 2 diabetes mellitus without complication']

df_diabetes = type2_df[(type2_df['standard_concept_name'] == 'Type 2 diabetes mellitus') | 
                       (type2_df['standard_concept_name'] == 'Type 2 diabetes mellitus without complication') ]
#                       (type2_df['standard_concept_name'] == 'Essential hypertension') |
#                       (type2_df['standard_concept_name'] == 'Hyperlipidemia') |
#                       (type2_df['standard_concept_name'] == 'Obesity') |
#                       (type2_df['standard_concept_name'] == 'Hyperglycemia due to type 2 diabetes mellitus') |
#                       (type2_df['standard_concept_name'] == 'Hypothyroidism')]

df_diabetes.head()


# In[19]:


df_diabetes.info()


# # Measurements

# In[20]:


import pandas
import os

# This query represents dataset "Diabetes-J1" for domain "measurement" and was generated for All of Us Registered Tier Dataset v7
dataset_12374778_measurement_sql = """
    SELECT
        measurement.person_id,
        measurement.measurement_concept_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        m_standard_concept.vocabulary_id as standard_vocabulary,
        measurement.measurement_datetime,
        measurement.measurement_type_concept_id,
        m_type.concept_name as measurement_type_concept_name,
        measurement.operator_concept_id,
        m_operator.concept_name as operator_concept_name,
        measurement.value_as_number,
        measurement.value_as_concept_id,
        m_value.concept_name as value_as_concept_name,
        measurement.unit_concept_id,
        m_unit.concept_name as unit_concept_name,
        measurement.range_low,
        measurement.range_high,
        measurement.visit_occurrence_id,
        m_visit.concept_name as visit_occurrence_concept_name,
        measurement.measurement_source_value,
        measurement.measurement_source_concept_id,
        m_source_concept.concept_name as source_concept_name,
        m_source_concept.concept_code as source_concept_code,
        m_source_concept.vocabulary_id as source_vocabulary,
        measurement.unit_source_value,
        measurement.value_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (3004249, 3012888, 3022318, 3025315, 3027018, 3036277, 40772531, 40775801, 40776230, 40779159, 40779250, 40779671, 40782562, 40785816, 40785850, 40789120, 40792413, 40795725, 40795740, 40797982)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                measurement.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (4274025)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 ) 
                            AND age_at_event >= 40  
                            AND visit_concept_id IN (8717) ) criteria 
                    GROUP BY
                        criteria.person_id, criteria.concept_id 
                    HAVING
                        COUNT(criteria.person_id) >= 3 ) )
            )) measurement 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
            ON measurement.measurement_concept_id = m_standard_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
            ON measurement.measurement_type_concept_id = m_type.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
            ON measurement.operator_concept_id = m_operator.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
            ON measurement.value_as_concept_id = m_value.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
            ON measurement.unit_concept_id = m_unit.concept_id 
    LEFT JOIn
        `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
            ON measurement.visit_occurrence_id = v.visit_occurrence_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_visit 
            ON v.visit_concept_id = m_visit.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
            ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_12374778_measurement_df = pandas.read_gbq(
    dataset_12374778_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12374778_measurement_df.head(5)


# In[21]:


# 
dataset_12374778_measurement_df.columns


# In[22]:


measurements_cols = ['person_id','standard_concept_name','measurement_datetime','value_as_number']
measurement_df = dataset_12374778_measurement_df[measurements_cols]

measurement_df.head()


# In[23]:


dataset_12374778_measurement_df['standard_concept_name'].value_counts()[:20]


# In[24]:


# Heart rate                                                           1051101
# Body weight                                                           114575
# Body height                                                            86553
# Creatinine [Mass/volume] in Serum or Plasma                            48913
# Calcium [Mass/volume] in Serum or Plasma                               48323
# Hemoglobin [Mass/volume] in Blood                                      44980
# Erythrocytes [#/volume] in Blood by Automated count                    43782
# Glucose [Mass/volume] in Blood                                         35394
# Platelets [#/volume] in Blood                                          35013
# Systolic blood pressure                                                31250
# Diastolic blood pressure                                               31241
# Lymphocytes/100 leukocytes in Blood by Manual count


# In[25]:


measurement_df['standard_concept_name'].value_counts()[:20]

measurement_df_final = measurement_df[(measurement_df['standard_concept_name'] == 'Heart rate')]
# (measurement_df['standard_concept_name'] == 'Body weight') |
#                       (measurement_df['standard_concept_name'] == 'Calcium [Mass/volume] in Serum or Plasma') |
#                       (measurement_df['standard_concept_name'] == 'Glucose [Mass/volume] in Serum or Plasma') |
#                       (measurement_df['standard_concept_name'] == 'Platelets [#/volume] in Blood ') |
#                       (measurement_df['standard_concept_name'] == 'Systolic blood pressure') |
#                       (measurement_df['standard_concept_name'] == 'Diastolic blood pressure') |
#                       (measurement_df['standard_concept_name'] == 'Lymphocytes/100 leukocytes in Blood by Manual count')

measurement_df_final.info()


# In[26]:


measurement_df_final.shape


# # Implementation

# In[27]:


df_diabetes2 = df_diabetes.rename(
    columns = {
        'standard_concept_name':'disease',
        'condition_start_datetime':'disease_start_dateTime',
        'condition_end_datetime':'disease_end_dateTime'
    }
)
df_diabetes2 = df_diabetes2[['person_id', 'disease','disease_start_dateTime', 'disease_end_dateTime']]

df_diabetes2


# In[28]:


import pandas as pd
df_diabetes2['disease_duration'] = (pd.Timestamp.now(tz='UTC') - df_diabetes2['disease_start_dateTime']).dt.days

df_diabetes3 = df_diabetes2[['person_id', 'disease', 'disease_duration']]

df_diabetes3


# In[29]:


df_diabetes4 = df_diabetes3.drop_duplicates()
df_diabetes4


# In[30]:


df1 = pd.merge(df_diabetes4, person_df1, how='left')
df1


# In[31]:


drug_df


# In[32]:


drug_df1 = drug_df[['person_id', 'standard_concept_name']]
drug_df1['drug_duration'] = (drug_df['drug_exposure_end_datetime'] - drug_df['drug_exposure_start_datetime']).dt.days


# In[33]:


df2 = pd.merge(df1, drug_df1, how='inner', on='person_id')


# In[34]:


df2


# In[ ]:





# In[35]:


df3 = df2.dropna()


# In[36]:


df4 = df3[df3['drug_duration']<1080]


# In[37]:


df4
# df_5 = df4.drop_duplicates()
# df_5


# In[ ]:





# In[38]:


df5 = df3[['disease', 'disease_duration', 'gender', 'standard_concept_name', 'drug_duration']]
df6 = df5.drop_duplicates()

df6.shape
df6


# ## Adding new measurements

# In[39]:


import pandas
import os

# This query represents dataset "Measurements2 dataset" for domain "measurement" and was generated for All of Us Registered Tier Dataset v7
dataset_01571333_measurement_sql = """
    SELECT
        measurement.person_id,
        m_standard_concept.concept_name as standard_concept_name,
        measurement.value_as_number 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (3004501, 3025315, 3027018, 3027114, 3036277)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                measurement.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (201826, 4193704)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 )) criteria ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                    WHERE
                        DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 40 AND 91 
                        AND NOT EXISTS (      SELECT
                            'x'      
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                        WHERE
                            d.person_id = p.person_id ) ) )
            )) measurement 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
            ON measurement.measurement_concept_id = m_standard_concept.concept_id"""

dataset_01571333_measurement_df = pandas.read_gbq(
    dataset_01571333_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_01571333_measurement_df.head(5)


# In[40]:


df = dataset_01571333_measurement_df.dropna()
df = dataset_01571333_measurement_df.drop_duplicates()


# In[41]:


## Heart Rate Measurement
df_heart_rate = df[df['standard_concept_name']=='Heart rate']
df_heart_rate = df_heart_rate.rename(columns={
    'value_as_number':'heart_rate'
})

df_heart_rate=df_heart_rate[['person_id', 'heart_rate']]
df_heart_rate = df_heart_rate.dropna()
df_heart_rate = df_heart_rate.drop_duplicates(subset=['person_id'])
df_heart_rate


# In[42]:


# glucose Measurement
df_glouse = df[df['standard_concept_name']=='Glucose [Mass/volume] in Serum or Plasma']
df_glouse = df_glouse.rename(columns={
    'value_as_number':'glocose'
})

df_glouse=df_glouse[['person_id', 'glocose']]
df_glouse = df_glouse.dropna()
df_glouse = df_glouse.drop_duplicates(subset=['person_id'])
df_glouse


# In[43]:


# Body weight Measurement
df_weight = df[df['standard_concept_name']=='Body weight']
df_weight = df_weight.rename(columns={
    'value_as_number':'body_weight'
})

df_weight=df_weight[['person_id', 'body_weight']]
df_weight = df_weight.dropna()
df_weight = df_weight.drop_duplicates(subset=['person_id'])
df_weight


# In[44]:


# Cholestrel Measurement
df_cholesterol = df[df['standard_concept_name']=='Cholesterol [Mass/volume] in Serum or Plasma']
df_cholesterol = df_cholesterol.rename(columns={
    'value_as_number':'cholesterol'
})

df_cholesterol=df_cholesterol[['person_id', 'cholesterol']]
df_cholesterol = df_cholesterol.dropna()
df_cholesterol = df_cholesterol.drop_duplicates(subset=['person_id'])
df_cholesterol


# In[45]:


import pandas as pd
df_with_heart_rate = pd.merge(df3, df_heart_rate, how='left', on='person_id')


# In[46]:


df_with_glucose= pd.merge(df_with_heart_rate, df_glouse, how='left', on='person_id')


# In[47]:


df_with_cholestrel = pd.merge(df_with_glucose, df_cholesterol, how='left', on='person_id')


# In[48]:


df_with_weight = pd.merge(df_with_cholestrel, df_weight, how='left', on='person_id')


# In[ ]:





# In[49]:


df_with_weight.shape


# In[50]:


# Drop 'drug_duration' and 'disease_duration' columns from df_with_weight
df_with_weight = df_with_weight.drop(columns=['person_id']) 
# # Display the first few rows to verify the columns are removed
# print(df_with_weight.head())

df_with_weight


# In[67]:


uniq_val = df_with_weight.drop_duplicates()
uniq_val


# In[68]:


uniq_val1 = uniq_val.copy()
uniq_val1['drug_dose'] = uniq_val1['standard_concept_name'].str.extract(r'(\d+\.?\d*)').fillna(0).astype(float)
uniq_val1


# In[69]:


uniq_val1['drug_main_product'] = uniq_val1['standard_concept_name'].str.split().str[0]


# In[78]:


uniq_val1['drug_main_product'].value_counts()[:5]

values_to_keep1 = ['acetaminophen', 'glucose ', 'sodium', 'aspirin','lidocaine']
cols_keep = ['disease','gender','age','drug_main_product','heart_rate','glocose','cholesterol','body_weight','drug_dose']
uniq_cols_df = uniq_val1[cols_keep]
filtered_data2 = uniq_cols_df[uniq_cols_df['drug_main_product'].isin(values_to_keep1)]
filtered_data2 = filtered_data2.drop_duplicates()


# In[86]:


filtered_data2.to_csv('testdata.csv', index=False)


# In[87]:


filtered_data2


# In[52]:


# final_df = df_with_weight[(df_with_weight['drug_duration'] > 365) & (df_with_weight['drug_duration'] < 1080)]

# final_df = final_df.rename(columns={
#     'standard_concept_name':'drug'
# })
# final_df1 = final_df[['person_id', 'disease', 'disease_duration', 'gender', 'age', 'drug', 'glocose',
#                      'drug_duration', 'heart_rate', 'cholesterol', 'body_weight']]

# # final_df1


# In[53]:


# final_df1 = final_df[['person_id', 'disease', 'gender', 'age', 'drug', 'glocose',
#                      'drug_duration', 'heart_rate', 'cholesterol', 'body_weight']]

# final_df1


# In[65]:


total_standard_concept_names = uniq_val['standard_concept_name'].value_counts()
total_standard_concept_names[:50]


# In[55]:


# Check unique values in the 'standard_concept_name' column
unique_standard_concept_names = uniq_val['standard_concept_name'].unique()

# Display the unique values
unique_standard_concept_names


# In[56]:


# List of values to keep
# values_to_keep = ['vancomycin', 'lidocaine', 'cyclosporine']


# Filter the DataFrame to keep only the rows where 'standard_concept_name' is in the list
filtered_data = uniq_val[uniq_val['standard_concept_name'].isin(values_to_keep)]

# Display the filtered DataFrame
filtered_data


# In[57]:


# Create a new column 'disease_flag' where:
# 0 = 'without complication', 1 = any other disease
filtered_data['disease_flag'] = filtered_data['disease'].apply(lambda x: 0 if 'without complication' in x else 1)

# Display the first few rows to verify the flag
filtered_data


# In[58]:


filtered_data = filtered_data.drop(columns=['disease']) 
filtered_data


# In[59]:


# Filter the rows that contain NaN values in any column
nan_records = filtered_data[filtered_data.isnull().any(axis=1)]

nan_records


# In[60]:


# Drop rows that contain NaN values in any column
filtered_data_cleaned = filtered_data.dropna()

# Display the cleaned DataFrame to verify that NaN records are dropped
filtered_data_cleaned

# Optionally, check how many rows were dropped
print(f"Number of rows before dropping NaN: {len(filtered_data)}")
print(f"Number of rows after dropping NaN: {len(filtered_data_cleaned)}")


# In[61]:


filtered_data_cleaned


# In[62]:


# Creating a dictionary to map the drug names to integers
drug_mapping = {
    'vancomycin': 1,
    'lidocaine': 2,
    'cyclosporine': 3
}

# Applying the mapping to the 'drug' column in the dataset
filtered_data_cleaned['drug_id'] = filtered_data_cleaned['standard_concept_name'].map(drug_mapping)

# Display the updated dataset
filtered_data_cleaned


# In[ ]:





# In[ ]:


#final_data.to_csv('final_data.csv', index=False)


# In[ ]:





# In[2]:


get_ipython().system('ls -l')


# # Model Training

# In[ ]:


# Filling NaN values with the mean of their respective columns
health_data_cleaned = final_data.fillna(final_data.mean())



# In[ ]:


# Splitting the dataset into features (X) and target (y)
X = health_data_cleaned.drop(columns=['disease_flag'])  # We drop 'person_id' as it's just an identifier
y = health_data_cleaned['disease_flag']


# In[ ]:


from sklearn.model_selection import train_test_split

# Splitting data into 80% train and 20% test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# In[ ]:


from sklearn.ensemble import RandomForestClassifier

# Create and train the Random Forest model
rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
rf_model.fit(X_train, y_train)

# Predictions and evaluation
y_pred_rf = rf_model.predict(X_test)


# In[ ]:


from sklearn.linear_model import LogisticRegression

# Create and train the Logistic Regression model
log_reg = LogisticRegression(max_iter=1000)
log_reg.fit(X_train, y_train)

# Predictions and evaluation
y_pred_logreg = log_reg.predict(X_test)


# In[ ]:


import xgboost as xgb

# Create and train the XGBoost model
xgb_model = xgb.XGBClassifier()
xgb_model.fit(X_train, y_train)

# Predictions and evaluation
y_pred_xgb = xgb_model.predict(X_test)


# In[ ]:


from sklearn.metrics import accuracy_score, classification_report

# Logistic Regression
print("Logistic Regression Performance:")
print(classification_report(y_test, y_pred_logreg))
print("Accuracy: ", accuracy_score(y_test, y_pred_logreg))

# Random Forest
print("Random Forest Performance:")
print(classification_report(y_test, y_pred_rf))
print("Accuracy: ", accuracy_score(y_test, y_pred_rf))

# XGBoost
print("XGBoost Performance:")
print(classification_report(y_test, y_pred_xgb))
print("Accuracy: ", accuracy_score(y_test, y_pred_xgb))


# # Diabetes risk prediction Implementation 

# In[194]:


import pandas as pd
#data = pd.read_csv('final_data.csv')
# data = pd.read_csv('data_synthetic.csv') 
# data.drop('Unnamed: 0', axis = 1, inplace=True)

data = pd.read_csv('data_version2.csv') 


# In[195]:


df = data.rename(
    columns = {
        'disease_flag':'disease',
        'drug_id' : 'drug'
    }
)
df.sample(5)


# In[196]:


features = df.columns
num_cols = [col for col in df.columns if type(col) != 'o']
categorical_cols = [col for col in df.columns if type(col) == 'o']
print(f"Total number of columns: {len(features)}")
print(f"Number of numeric columns: {len(num_cols)} : {num_cols}")
print(f"Number of categorical columns: {len(categorical_cols)} : {categorical_cols}")


# In[197]:


import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns
import plotly.express as px
import warnings

warnings.filterwarnings("ignore")

get_ipython().run_line_magic('matplotlib', 'inline')


# In[198]:


numeric_features = ['heart_rate', 'glocose', 'cholesterol', 'body_weight']
plt.figure(figsize=(15, 10))
plt.suptitle('Univariate Analysis of Numerical Features', fontsize=20, fontweight='bold', alpha=0.8, y=1.)

for i in range(0, len(numeric_features)):
    plt.subplot(2, 2, i+1)
    sns.kdeplot(x=df[numeric_features[i]], color='blue')
    plt.xlabel(numeric_features[i])
    plt.tight_layout()
    
# save plot
# plt.savefig('./images/Univariate_Cat.png')


# In[199]:


# # categorical columns
categorical_features = ['disease', 'drug_type']
plt.figure(figsize=(15, 8))
plt.suptitle('Univariate Analysis of Categorical Features', fontsize=20, fontweight='bold', alpha=0.8, y=1.)

for i in range(0, len(categorical_features)):
    plt.subplot(3, 3, i+1)
    sns.countplot(x=df[categorical_features[i]])
    plt.xlabel(categorical_features[i])
    plt.tight_layout()
    
# save plot
# plt.savefig('./images/Univariate_Cat.png')


# In[200]:


# porportion of count data on categorical columns
categorical_features = ['disease', 'drug_type']
for col in categorical_features:
    print(df[col].value_counts(normalize=True)*100)
    print('------------------------------')


# In[201]:


discrete_features=[feature for feature in num_cols if len(df[feature].unique()) <=5]

continuous_features=[feature for feature in num_cols if len(df[feature].unique()) > 5]

print('We have {} discrete features : {}'.format(len(discrete_features), discrete_features))
print('\nWe have {} continuous_features : {}'.format(len(continuous_features), continuous_features))


# In[202]:


clr1 =['#1E90FF', '#DC143C']
fig, ax = plt.subplots(6, 2, figsize=(10, 12))
fig.suptitle('Distribution of Numerical Features by Disease risk', color='#3C3744',
             fontsize=20, fontweight='bold', ha='center')
for i, col in enumerate(continuous_features):
    sns.boxplot(data=df, x= 'disease', y = col, palette= clr1, ax= ax[i, 0])
    ax[i, 0].set_title(f'Boxplot of {col}', fontsize=12)
    sns.histplot(data=df, x=col, hue='disease', kde=True, multiple='stack',
                bins=20, palette=clr1, ax=ax[i,1])
    ax[i,1].set_title(f'Histogram of {col}', fontsize=14)
fig.tight_layout()


# In[203]:


percentage = df.disease.value_counts(normalize=True)*100
percentage


# In[204]:


percentage = df.disease.value_counts(normalize=True)*100
labels = ["with risk", "without risk"]

# plot PieChart with plotly library
fig, ax = plt.subplots(figsize=(15, 8))
explode = (0, 0.1)
colors = ['#1188ff','#e63a2a']
ax.pie(percentage, labels = labels, explode=explode, shadow=True,
        colors=colors, startangle = 90,autopct='%1.2f%%')
plt.show()


# In[205]:


# Splitting X and y for all Experiments
X= df.drop('disease', axis=1)
y = df['disease']

from imblearn.combine import SMOTEENN, SMOTETomek

# Resampling the minority class. The strategy can be changed as required.
smt = SMOTETomek(random_state=42,sampling_strategy='minority',n_jobs=-1)
# Fit the model to generate the data.
X_res, y_res = smt.fit_resample(X, y)


# ### Modelling 

# In[206]:


def evaluate_clf(true, predicted):
    '''
    This function takes in true values and predicted values
    Returns: Accuracy, F1-Score, Precision, Recall, Roc-auc Score
    '''
    acc = accuracy_score(true, predicted) # Calculate Accuracy
    f1 = f1_score(true, predicted) # Calculate F1-score
    precision = precision_score(true, predicted) # Calculate Precision
    recall = recall_score(true, predicted)  # Calculate Recall
    roc_auc = roc_auc_score(true, predicted) #Calculate Roc
    return acc, f1 , precision, recall, roc_auc


# In[ ]:





# In[207]:


# Create a function which can evaluate models and return a report 
def evaluate_models(X, y, models):
    '''
    This function takes in X and y and models dictionary as input
    It splits the data into Train Test split
    Iterates through the given model dictionary and evaluates the metrics
    Returns: Dataframe which contains report of all models metrics with cost
    '''
    # separate dataset into train and test
    X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2,random_state=42)
    
    cost_list=[]
    models_list = []
    accuracy_list = []
    
    for i in range(len(list(models))):
        model = list(models.values())[i]
        model.fit(X_train, y_train) # Train model

        # Make predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        # Training set performance
        model_train_accuracy, model_train_f1,model_train_precision,\
        model_train_recall,model_train_rocauc_score=evaluate_clf(y_train ,y_train_pred)
        


        # Test set performance
        model_test_accuracy,model_test_f1,model_test_precision,\
        model_test_recall,model_test_rocauc_score=evaluate_clf(y_test, y_test_pred)
        

        print(list(models.keys())[i])
        models_list.append(list(models.keys())[i])

        print('Model performance for Training set')
        print("- Accuracy: {:.4f}".format(model_train_accuracy))
        print('- F1 score: {:.4f}'.format(model_train_f1)) 
        print('- Precision: {:.4f}'.format(model_train_precision))
        print('- Recall: {:.4f}'.format(model_train_recall))
        print('- Roc Auc Score: {:.4f}'.format(model_train_rocauc_score))
        

        print('----------------------------------')

        print('Model performance for Test set')
        print('- Accuracy: {:.4f}'.format(model_test_accuracy))
        print('- F1 score: {:.4f}'.format(model_test_f1))
        print('- Precision: {:.4f}'.format(model_test_precision))
        print('- Recall: {:.4f}'.format(model_test_recall))
        print('- Roc Auc Score: {:.4f}'.format(model_test_rocauc_score))
        
        
        print('='*35)
        print('\n')
        
    #report=pd.DataFrame(list(zip(models_list, cost_list)), columns=['Model Name', 'Cost']).sort_values(by=["Cost"])
        
    # return report


# In[208]:


from catboost import CatBoostClassifier


# In[209]:


from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from catboost import CatBoostClassifier
from sklearn.metrics import accuracy_score, classification_report,ConfusionMatrixDisplay, \
                            precision_score, recall_score, f1_score, roc_auc_score,roc_curve,confusion_matrix


from sklearn import metrics 
from sklearn.model_selection import  train_test_split, cross_val_score
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier
from sklearn.preprocessing import StandardScaler


warnings.filterwarnings("ignore")
get_ipython().run_line_magic('matplotlib', 'inline')


# In[210]:


# Dictionary which contains models for experiment
models = {
    "Random Forest": RandomForestClassifier(),
    "Decision Tree": DecisionTreeClassifier(),
    "Gradient Boosting": GradientBoostingClassifier(),
    "Logistic Regression": LogisticRegression(),
     "K-Neighbors Classifier": KNeighborsClassifier(),
    "XGBClassifier": XGBClassifier(), 
     "CatBoosting Classifier": CatBoostClassifier(verbose=False),
    "AdaBoost Classifier": AdaBoostClassifier(),
    "Support Vector Machines":SVC()
}


# In[211]:


evaluate_models(X_res, y_res, models)


# In[212]:


#Initialize few parameter for Hyperparamter tuning
log_params = {
    'penalty' : ['l1', 'l2', 'elasticnet'],
    'solver' : ['lbfgs', 'liblinear', 'newton-cg', 'newton-cholesky', 'sag', 'saga'],
    'max_iter': [30, 35, 40, 45, 50, 55, 60]
}

ad_params = {
    'n_estimators' :[35, 40, 45, 50, 55, 60],
     'learning_rate': [0.01,0.05, 0.1]
    
}

knn_params = {
    "algorithm": ['auto', 'ball_tree', 'kd_tree','brute'],
    "weights": ['uniform', 'distance'],
    "n_neighbors": [3, 4, 5, 7, 9],
}


# In[213]:


# Models list for Hyperparameter tuning
randomcv_models = [
    ('AdaBoost', AdaBoostClassifier(), ad_params),
    ("Log", LogisticRegression(), log_params),
    ("K-Neighbors Classifier", KNeighborsClassifier(),knn_params)
]


# In[214]:


from sklearn.model_selection import RandomizedSearchCV

model_param = {}
for name, model, params in randomcv_models:
    random = RandomizedSearchCV(estimator=model,
                                   param_distributions=params,
                                   n_iter=20,
                                   cv=3,
                                   verbose=3, 
                                   n_jobs=-1)
    random.fit(X_res, y_res)
    model_param[name] = random.best_params_

for model_name in model_param:
    print(f"---------------- Best Params for {model_name} -------------------")
    print(model_param[model_name])


# In[215]:


model_param


# In[216]:


log_best_params = model_param['Log']


# In[217]:


models_best = {
    "Logistic Regression": LogisticRegression(**model_param['Log']),
    "AdaBoost Classifier": AdaBoostClassifier(**model_param['AdaBoost']),
    "K-Neighbors Classifier": KNeighborsClassifier(**model_param['K-Neighbors Classifier']),
    
}


# In[218]:


evaluate_models(X_res, y_res, models_best)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Solution 2

# In[35]:


dataframe = pd.read_csv('testdata.csv')

dataframe.shape


# In[36]:


dataframe


# In[37]:


dataframe.isna().sum()


# In[38]:


columns_to_fill = ['heart_rate', 'glocose', 'cholesterol', 'body_weight']
dataframe[columns_to_fill] = dataframe[columns_to_fill].apply(lambda col: col.fillna(col.mean()))
dataframe


# In[39]:


# Applying StandardScaler to scale the numerical features
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder


# In[40]:


dataframe


# In[41]:


# LabelEncoder for 'drug_main_product'
le = LabelEncoder()
dataframe['drug_main_product_encoded'] = le.fit_transform(dataframe['drug_main_product'])


# In[42]:


dataframe


# In[43]:


# Mapping target 'disease': 1 as Type 2 Diabetes Mellitus, 0 for without complication
dataframe['disease'] = dataframe['disease'].map({'Type 2 diabetes mellitus': 1, 'Type 2 diabetes mellitus without complication': 0})


# In[44]:


dataframe['gender_encoded'] = le.fit_transform(dataframe['gender'])
dataframe.drop('gender', axis=1, inplace=True)


# In[45]:


dataframe = dataframe.rename(
    columns = {
        'drug_main_product_encoded':'drug_type',
        'gender_encoded':'gender'
    }
)
dataframe = dataframe[['disease', 'age','heart_rate','glocose','cholesterol','body_weight','drug_dose','drug_type','gender']]

dataframe


# In[46]:


dataframe.to_csv('data_version2.csv', index=False)


# In[47]:


df10 = pd.read_csv('data_version2.csv')
# Set random seed for reproducibility
np.random.seed(42)

# Function to create synthetic data by adding noise
def generate_synthetic_data(df, noise_factor=0.1):
    # Make a copy of the dataframe to avoid modifying the original
    synthetic_df = df.copy()

    # Apply random noise to numerical columns
    for column in df.select_dtypes(include=[np.number]).columns:
        noise = np.random.normal(0, noise_factor * df[column].std(), df[column].shape)
        synthetic_df[column] = df[column] + noise

    # Apply slight modifications to categorical columns (if any)
    for column in df.select_dtypes(include=[object]).columns:
        # Adding noise to categories (like shuffling)
        synthetic_df[column] = np.random.choice(df[column].unique(), size=len(df))
    
    return synthetic_df

# Generate synthetic data
synthetic_df = generate_synthetic_data(df10)

# Save the synthetic dataset to a new CSV file
synthetic_df.to_csv('synthetic_dataset.csv', index=False)

# Display the new synthetic dataset
synthetic_df.head()


# In[48]:


synthetic_df['disease'] = synthetic_df['disease'].apply(lambda x: 1 if x> 0.5 else 0)


# In[49]:


synthetic_df['drug_dose'] = synthetic_df['drug_dose'].apply(lambda x: round(x))


# In[50]:


synthetic_df['gender'] = synthetic_df['gender'].apply(lambda x: int(x))


# In[51]:


synthetic_df['age'] = synthetic_df['age'].apply(lambda x: int(x))


# In[52]:


synthetic_df


# In[53]:


dataframe


# In[54]:


final_dataframe = pd.concat([dataframe,synthetic_df])
final_dataframe


# In[59]:


final_dataframe.to_csv('data_synthetic.csv', index=False)


# In[60]:


final_dataframe


# # Solution 3

# In[82]:


data = pd.read_csv('data_version2.csv') 
data


# In[93]:


import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

# Load the dataset
data = pd.read_csv('data_version2.csv')

# Separate features (X) and target variable (y)
X = data.drop(columns=['disease'])
y = data['disease']

# Convert categorical data into numeric using Label Encoding
label_encoder = LabelEncoder()
X['gender'] = label_encoder.fit_transform(X['gender'])
X['drug_type'] = label_encoder.fit_transform(X['drug_type'])

# Standardize numerical features
scaler = StandardScaler()
numerical_columns = ['age', 'heart_rate', 'glocose', 'cholesterol', 'body_weight', 'drug_dose']
X[numerical_columns] = scaler.fit_transform(X[numerical_columns])

# Split the dataset into training and test sets (80% training, 20% testing)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Hyperparameter tuning using GridSearchCV
def tune_model(model, params):
    grid_search = GridSearchCV(model, param_grid=params, cv=5, scoring='accuracy', n_jobs=-1, verbose=2)
    grid_search.fit(X_train, y_train)
    return grid_search.best_estimator_

# Function to evaluate model performance
def evaluate_model(model):
    # Training performance
    y_train_pred = model.predict(X_train)
    train_accuracy = accuracy_score(y_train, y_train_pred)
    train_f1 = f1_score(y_train, y_train_pred, average='weighted')
    train_precision = precision_score(y_train, y_train_pred, average='weighted')
    train_recall = recall_score(y_train, y_train_pred, average='weighted')

    # Test performance
    y_test_pred = model.predict(X_test)
    test_accuracy = accuracy_score(y_test, y_test_pred)
    test_f1 = f1_score(y_test, y_test_pred, average='weighted')
    test_precision = precision_score(y_test, y_test_pred, average='weighted')
    test_recall = recall_score(y_test, y_test_pred, average='weighted')

    # Print results
    print(f"Model performance for Training set")
    print(f"- Accuracy: {train_accuracy:.4f}")
    print(f"- F1 score: {train_f1:.4f}")
    print(f"- Precision: {train_precision:.4f}")
    print(f"- Recall: {train_recall:.4f}")
    print("----------------------------------")
    print(f"Model performance for Test set")
    print(f"- Accuracy: {test_accuracy:.4f}")
    print(f"- F1 score: {test_f1:.4f}")
    print(f"- Precision: {test_precision:.4f}")
    print(f"- Recall: {test_recall:.4f}")
    print("===================================\n")

# Logistic Regression with regularization
log_reg = LogisticRegression(C=0.1, max_iter=500)
log_reg_params = {'C': [0.01, 0.1, 1, 10]}  # Tuning regularization strength

print("Tuning Logistic Regression...")
best_log_reg = tune_model(log_reg, log_reg_params)
evaluate_model(best_log_reg)

# Random Forest
rf = RandomForestClassifier(random_state=42)
rf_params = {
    'n_estimators': [100, 200],
    'max_depth': [5, 10, None],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4]
}

print("Tuning Random Forest...")
best_rf = tune_model(rf, rf_params)
evaluate_model(best_rf)

# Gradient Boosting
gb = GradientBoostingClassifier(random_state=42)
gb_params = {
    'n_estimators': [100, 200],
    'learning_rate': [0.01, 0.1, 0.2],
    'max_depth': [3, 5, 7],
    'subsample': [0.8, 1.0]
}

print("Tuning Gradient Boosting...")
best_gb = tune_model(gb, gb_params)
evaluate_model(best_gb)


# In[95]:


from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

# Random Forest with hyperparameter tuning
rf = RandomForestClassifier(random_state=42)

# Hyperparameter grid
param_grid_rf = {
    'n_estimators': [100, 200, 300],
    'max_depth': [10, 20, 30, None],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4]
}

# GridSearchCV
grid_search_rf = GridSearchCV(estimator=rf, param_grid=param_grid_rf, cv=5, scoring='accuracy', n_jobs=-1, verbose=2)
grid_search_rf.fit(X_train, y_train)

# Best Random Forest model
best_rf = grid_search_rf.best_estimator_

# Evaluate model
y_test_pred_rf = best_rf.predict(X_test)
test_accuracy_rf = accuracy_score(y_test, y_test_pred_rf)
print(f"Random Forest Test Accuracy: {test_accuracy_rf:.4f}")


# In[96]:


from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import GridSearchCV

# Gradient Boosting with hyperparameter tuning
gb = GradientBoostingClassifier(random_state=42)

# Hyperparameter grid
param_grid_gb = {
    'n_estimators': [100, 200],
    'learning_rate': [0.01, 0.1, 0.2],
    'max_depth': [3, 5, 7],
    'subsample': [0.8, 1.0]
}

# GridSearchCV
grid_search_gb = GridSearchCV(estimator=gb, param_grid=param_grid_gb, cv=5, scoring='accuracy', n_jobs=-1, verbose=2)
grid_search_gb.fit(X_train, y_train)

# Best Gradient Boosting model
best_gb = grid_search_gb.best_estimator_

# Evaluate model
y_test_pred_gb = best_gb.predict(X_test)
test_accuracy_gb = accuracy_score(y_test, y_test_pred_gb)
print(f"Gradient Boosting Test Accuracy: {test_accuracy_gb:.4f}")


# In[97]:


pip install xgboost


# In[98]:


import xgboost as xgb
from sklearn.model_selection import GridSearchCV

# XGBoost with hyperparameter tuning
xgb_model = xgb.XGBClassifier(objective='binary:logistic', eval_metric='logloss')

# Hyperparameter grid for XGBoost
param_grid_xgb = {
    'n_estimators': [100, 200],
    'learning_rate': [0.01, 0.1, 0.2],
    'max_depth': [3, 5, 7],
    'subsample': [0.8, 1.0]
}

# GridSearchCV
grid_search_xgb = GridSearchCV(estimator=xgb_model, param_grid=param_grid_xgb, cv=5, scoring='accuracy', n_jobs=-1, verbose=2)
grid_search_xgb.fit(X_train, y_train)

# Best XGBoost model
best_xgb = grid_search_xgb.best_estimator_

# Evaluate model
y_test_pred_xgb = best_xgb.predict(X_test)
test_accuracy_xgb = accuracy_score(y_test, y_test_pred_xgb)
print(f"XGBoost Test Accuracy: {test_accuracy_xgb:.4f}")


# In[99]:


pip install fastFM


# In[121]:


import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from fastFM import sgd
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from scipy.sparse import csr_matrix


# In[122]:


# Load the dataset
data = pd.read_csv('data_version2.csv')

# Separate features (X) and target variable (y)
X = data.drop(columns=['disease'])
y = data['disease']


# In[123]:


# Standardize numerical features
scaler = StandardScaler()
numerical_columns = ['age', 'heart_rate', 'glocose', 'cholesterol', 'body_weight', 'drug_dose']
X[numerical_columns] = scaler.fit_transform(X[numerical_columns])


# In[124]:


X_sparse = csr_matrix(X)


# In[125]:


X_sparse


# In[126]:


# y = label_encoder.fit_transform(y)


# In[127]:


# label_encoder = LabelEncoder()y
y


# In[128]:


# Split the dataset into training and test sets (80% training, 20% testing)
X_train, X_test, y_train, y_test = train_test_split(X_sparse, y, test_size=0.2, random_state=42)


# In[129]:


fm = sgd.FMClassification(n_iter=1000, init_stdev=0.01, l2_reg=0.01, rank=8, random_state=42)

# Train the model
fm.fit(X_train, y_train)


# In[ ]:





# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "fitbit_heart_rate_summary" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_fitbit_heart_rate_summary_sql = """
    SELECT
        heart_rate_summary.person_id,
        heart_rate_summary.date,
        heart_rate_summary.zone_name,
        heart_rate_summary.min_heart_rate,
        heart_rate_summary.max_heart_rate,
        heart_rate_summary.minute_in_zone,
        heart_rate_summary.calorie_count 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.heart_rate_summary` heart_rate_summary   
    WHERE
        heart_rate_summary.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585712) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585716) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585713) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585715))) criteria ) )"""

dataset_58312674_fitbit_heart_rate_summary_df = pandas.read_gbq(
    dataset_58312674_fitbit_heart_rate_summary_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_fitbit_heart_rate_summary_df.head(5)


# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "fitbit_heart_rate_level" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_fitbit_heart_rate_level_sql = """
    SELECT
        heart_rate_minute_level.person_id,
        CAST(heart_rate_minute_level.datetime AS DATE) as date,
        AVG(heart_rate_value) avg_rate 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.heart_rate_minute_level` heart_rate_minute_level   
    WHERE
        heart_rate_minute_level.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585712) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585716) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585713) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585715))) criteria ) ) 
    GROUP BY
        person_id,
        date"""

dataset_58312674_fitbit_heart_rate_level_df = pandas.read_gbq(
    dataset_58312674_fitbit_heart_rate_level_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_fitbit_heart_rate_level_df.head(5)


# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "fitbit_activity" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_fitbit_activity_sql = """
    SELECT
        activity_summary.person_id,
        activity_summary.date,
        activity_summary.activity_calories,
        activity_summary.calories_bmr,
        activity_summary.calories_out,
        activity_summary.elevation,
        activity_summary.fairly_active_minutes,
        activity_summary.floors,
        activity_summary.lightly_active_minutes,
        activity_summary.marginal_calories,
        activity_summary.sedentary_minutes,
        activity_summary.steps,
        activity_summary.very_active_minutes 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.activity_summary` activity_summary   
    WHERE
        activity_summary.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585712) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585716) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585713) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585715))) criteria ) )"""

dataset_58312674_fitbit_activity_df = pandas.read_gbq(
    dataset_58312674_fitbit_activity_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_fitbit_activity_df.head(5)


# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "fitbit_intraday_steps" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_fitbit_intraday_steps_sql = """
    SELECT
        steps_intraday.person_id,
        CAST(steps_intraday.datetime AS DATE) as date,
        SUM(CAST(steps_intraday.steps AS INT64)) as sum_steps 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.steps_intraday` steps_intraday   
    WHERE
        steps_intraday.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585712) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585716) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585713) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585715))) criteria ) ) 
    GROUP BY
        person_id,
        date"""

dataset_58312674_fitbit_intraday_steps_df = pandas.read_gbq(
    dataset_58312674_fitbit_intraday_steps_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_fitbit_intraday_steps_df.head(5)


# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "fitbit_sleep_daily_summary" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_fitbit_sleep_daily_summary_sql = """
    SELECT
        sleep_daily_summary.person_id,
        sleep_daily_summary.sleep_date,
        sleep_daily_summary.is_main_sleep,
        sleep_daily_summary.minute_in_bed,
        sleep_daily_summary.minute_asleep,
        sleep_daily_summary.minute_after_wakeup,
        sleep_daily_summary.minute_awake,
        sleep_daily_summary.minute_restless,
        sleep_daily_summary.minute_deep,
        sleep_daily_summary.minute_light,
        sleep_daily_summary.minute_rem,
        sleep_daily_summary.minute_wake 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.sleep_daily_summary` sleep_daily_summary   
    WHERE
        PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585712) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585716) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585713) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585715))) criteria ) )"""

dataset_58312674_fitbit_sleep_daily_summary_df = pandas.read_gbq(
    dataset_58312674_fitbit_sleep_daily_summary_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_fitbit_sleep_daily_summary_df.head(5)


# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "fitbit_sleep_level" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_fitbit_sleep_level_sql = """
    SELECT
        sleep_level.person_id,
        sleep_level.sleep_date,
        sleep_level.is_main_sleep,
        sleep_level.level,
        CAST(sleep_level.start_datetime AS DATE) as date,
        sleep_level.duration_in_min 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.sleep_level` sleep_level   
    WHERE
        PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585712) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585716) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585713) 
                    OR  concept_id IN (1585711) 
                    AND is_standard = 0  
                    AND  value_source_concept_id IN (1585715))) criteria ) )"""

dataset_58312674_fitbit_sleep_level_df = pandas.read_gbq(
    dataset_58312674_fitbit_sleep_level_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_fitbit_sleep_level_df.head(5)


# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "survey" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_survey_sql = """
    SELECT
        answer.person_id,
        answer.survey_datetime,
        answer.survey,
        answer.question_concept_id,
        answer.question,
        answer.answer_concept_id,
        answer.answer,
        answer.survey_version_concept_id,
        answer.survey_version_name  
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.ds_survey` answer   
    WHERE
        (
            question_concept_id IN (1585636, 1585711, 1585717, 1585723, 1585729, 1585741, 1585857, 1585873, 1586159, 1586162, 1586169, 1586177, 1586185, 1586193, 1586198)
        )  
        AND (
            answer.PERSON_ID IN (SELECT
                distinct person_id  
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
            WHERE
                cb_search_person.person_id IN (SELECT
                    criteria.person_id 
                FROM
                    (SELECT
                        DISTINCT person_id, entry_date, concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                    WHERE
                        (concept_id IN (1585711) 
                        AND is_standard = 0  
                        AND  value_source_concept_id IN (1585712) 
                        OR  concept_id IN (1585711) 
                        AND is_standard = 0  
                        AND  value_source_concept_id IN (1585716) 
                        OR  concept_id IN (1585711) 
                        AND is_standard = 0  
                        AND  value_source_concept_id IN (1585713) 
                        OR  concept_id IN (1585711) 
                        AND is_standard = 0  
                        AND  value_source_concept_id IN (1585715))) criteria ) )
        )"""

dataset_58312674_survey_df = pandas.read_gbq(
    dataset_58312674_survey_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_survey_df.head(5)


# In[ ]:


import pandas
import os

# This query represents dataset "task 2 dataset version2" for domain "measurement" and was generated for All of Us Registered Tier Dataset v7
dataset_58312674_measurement_sql = """
    SELECT
        measurement.person_id,
        measurement.measurement_concept_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        m_standard_concept.vocabulary_id as standard_vocabulary,
        measurement.measurement_datetime,
        measurement.measurement_type_concept_id,
        m_type.concept_name as measurement_type_concept_name,
        measurement.operator_concept_id,
        m_operator.concept_name as operator_concept_name,
        measurement.value_as_number,
        measurement.value_as_concept_id,
        m_value.concept_name as value_as_concept_name,
        measurement.unit_concept_id,
        m_unit.concept_name as unit_concept_name,
        measurement.range_low,
        measurement.range_high,
        measurement.visit_occurrence_id,
        m_visit.concept_name as visit_occurrence_concept_name,
        measurement.measurement_source_value,
        measurement.measurement_source_concept_id,
        m_source_concept.concept_name as source_concept_name,
        m_source_concept.concept_code as source_concept_code,
        m_source_concept.vocabulary_id as source_vocabulary,
        measurement.unit_source_value,
        measurement.value_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_source_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (1586218, 903107, 903121, 903124, 903126, 903133)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 0 
                    AND is_selectable = 1)
            )  
            AND (
                measurement.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN (1585711) 
                            AND is_standard = 0  
                            AND  value_source_concept_id IN (1585712) 
                            OR  concept_id IN (1585711) 
                            AND is_standard = 0  
                            AND  value_source_concept_id IN (1585716) 
                            OR  concept_id IN (1585711) 
                            AND is_standard = 0  
                            AND  value_source_concept_id IN (1585713) 
                            OR  concept_id IN (1585711) 
                            AND is_standard = 0  
                            AND  value_source_concept_id IN (1585715))) criteria ) )
            )) measurement 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
            ON measurement.measurement_concept_id = m_standard_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
            ON measurement.measurement_type_concept_id = m_type.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
            ON measurement.operator_concept_id = m_operator.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
            ON measurement.value_as_concept_id = m_value.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
            ON measurement.unit_concept_id = m_unit.concept_id 
    LEFT JOIn
        `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
            ON measurement.visit_occurrence_id = v.visit_occurrence_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_visit 
            ON v.visit_concept_id = m_visit.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
            ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_58312674_measurement_df = pandas.read_gbq(
    dataset_58312674_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_58312674_measurement_df.head(5)


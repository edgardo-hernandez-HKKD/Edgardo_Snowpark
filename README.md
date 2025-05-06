# Snowpark Capstone: Global Happiness Analytics

## Overview

The goal is to analyze global happiness data from 2008 to 2024, focusing on trends before the COVID-19 pandemic.  
We clean, transform, and create curated views using Snowflake and Snowpark.

---

## Dataset

World Happiness Report (2008–2024) including:
- Country name
- Year
- Life satisfaction
- Economic prosperity
- Social support
- Health expectancy
- Freedom, generosity, corruption perception
- Positive and negative emotions

---

## Project Steps

1. **Data Cleaning** (`krys_refine.py`):
   - Handle missing values.
   - Standardize country names.
   - Save a refined table: `KRYS_REFINED.REFINED_WORLD_HAPPINESS`.

2. **Data Transformation** (`krys_curated.py`):
   - Filter years 2010–2023.
   - Group by country and calculate averages.
   - Create views: 
     - `WORLD_HAPPINESS_2010_2023`
     - `COUNTRY_AVERAGE_WORLD_HAPPINESS`

---

## How to Run

1. Set up your Snowflake connection inside `utils.py`.
2. Install dependencies:

   pip install -r requirements.txt

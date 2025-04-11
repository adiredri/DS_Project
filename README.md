# Hotel Price Prediction with Smart Sampling - Data Science Project

## Overview

This project explores the challenge of predicting hotel room prices using real-world data and modern data science methodologies.  
It is divided into two major parts, each tackling a different stage in the data pipeline:

- **Part A** involves building the dataset from scratch through web scraping, followed by data cleaning, enrichment, and the development of baseline models.
- **Part B** assumes a prepared dataset and focuses on **sampling-efficient modeling** using techniques like **Gaussian Process Regression (GPR)**, aiming to minimize the number of samples needed while preserving high prediction accuracy.

The core goal of this project is to simulate an end-to-end, production-level data science process — from raw data collection to intelligent model design and evaluation.

Key aspects include:
- Extracting and enriching data from real booking websites
- Applying feature engineering and normalization
- Training baseline regressors and comparing their performance
- Designing active sampling strategies to improve prediction under limited data
- Measuring model success using metrics like **R²**, **uncertainty**, and **early stopping** criteria

Each part builds upon the previous one, forming a complete and scalable workflow for hotel pricing intelligence.

---

# Part A – Data Collection, Enrichment and Baseline Modeling

## 1. Web Scraping and Raw Data Construction

The project begins with a full-scale **web scraping operation**, aimed at collecting hotel pricing data from multiple booking sources. The scraping process targeted a variety of hotel chains across different geographic regions and dates, retrieving structured HTML tables and dynamic JavaScript-rendered prices using Selenium and BeautifulSoup.

Challenges included:
- Handling dynamic page content and asynchronous loading
- Dealing with inconsistent HTML structures and missing values
- Respecting rate limits and website stability

The scraped data was saved in structured CSV files, with snapshots capturing key attributes such as:
- Hotel name, location, chain, star rating
- Room type and occupancy
- Date of search (snapshot), target stay date
- Raw price and discount codes

This dataset served as the foundation for all downstream processing and modeling.

---

## 2. Preprocessing and Data Cleaning

Following the scraping phase, the data was passed through a **rigorous preprocessing pipeline**. The goal was to unify formats, handle outliers, and prepare the data for modeling.

Steps included:
- Filtering out corrupted rows and null values
- Unifying date formats (`Snapshot Date`, `Check-in Date`)
- Parsing room occupancy and hotel attributes
- Casting price columns to float and handling currency symbols
- Generating unique identifiers for snapshot entries

The output of this phase was a clean, analysis-ready DataFrame.

---

## 3. Feature Engineering

We then performed extensive **feature engineering** to enrich the dataset. This step focused on generating new variables and structures that improve model learnability.

Key transformations:
- Extracted temporal features (weekday, weekend, seasonality)
- Created binary flags for discount codes, holiday periods, and last-minute bookings
- Grouped data by `Hotel Name`, `Snapshot Date`, and `Discount Code` to track availability and price dynamics
- Normalized numeric features (price, occupancy) using standard scaling

The enriched dataset allowed us to explore temporal trends and booking behaviors more effectively.

---

## 4. Baseline Modeling

To evaluate the predictive potential of the data, we built and compared several **baseline regression models**:

- **Linear Regression**: Used as a simple benchmark for model interpretability  
- **Random Forest Regressor**: To capture non-linear relationships  
- **K-Nearest Neighbors (KNN)**: As a distance-based approach for price similarity

Each model was trained and evaluated using **R² score** and **mean absolute error (MAE)** over a hold-out test set. This provided a baseline reference for the more advanced sampling methods introduced later in Part B.

Cross-validation and stratified sampling ensured robustness across different hotel types and dates.

---

## Summary and Observations

Part A laid the groundwork for the entire project by building the dataset from the ground up.

Through careful scraping, cleaning, and engineering, we created a high-dimensional dataset rich in hotel, room, and pricing context. The baseline models showed that:

- Linear models struggle to capture nuanced patterns across hotels  
- Tree-based models (like Random Forest) perform better, but are limited by overfitting  
- Data sparsity and noise remain critical challenges for price prediction

These insights directly informed the design choices for Part B, where the focus shifts from raw prediction to **sampling efficiency and model generalization**.

---

# Part B – Smart Sampling and Gaussian Process Regression

## 1. Motivation and Problem Framing

After establishing a baseline in Part A, the focus in Part B shifts from collecting more data to **using data more efficiently**.  
The goal is to train accurate predictive models while minimizing the number of data points used – a real-world scenario where data is expensive or limited.

We explore **active learning** and **sampling-efficient regression** techniques using:
- Iterative sampling strategies
- Gaussian Process Regression (GPR)
- Early stopping based on model confidence and R² improvement

This part of the project evaluates not only model accuracy, but also **data acquisition cost vs. predictive gain**.

---

## 2. Data Setup and Grouping Logic

We start from a cleaned dataset (output of Part A or provided) and group data by a unique hotel-date-discount combination.

Each group represents a mini-problem of predicting the price curve over a 30-day horizon before check-in.  
This allows localized model fitting while maintaining independence between groups.

Grouping Keys:
- `Hotel Name`
- `Snapshot Date`
- `Discount Code`

This hierarchical structure enables parallel evaluation across thousands of scenarios.

---

## 3. Sampling Loop and Active Learning Strategy

The heart of Part B is an **iterative sampling loop**:

1. Begin with a small number of initial samples per group (e.g., 2–3 days)
2. Fit a **Gaussian Process Regressor** to the available points
3. Predict on remaining days and compute:
   - R² score on predicted vs. true values
   - Model uncertainty (standard deviation of GP posterior)
4. Sample the point with highest uncertainty
5. Repeat until early stopping is triggered by:
   - R² stagnation across iterations
   - Uncertainty dropping below threshold
   - Max iterations reached (e.g., 10)

This loop mimics a real-time sampling policy: the model decides **which day to ask for next**.

---

## 4. Gaussian Process Regression Details

We used `sklearn.gaussian_process.GaussianProcessRegressor` with a composite kernel to capture both linear and smooth non-linear trends.

GPR was chosen because:
- It provides a **mean prediction** and **uncertainty estimate**
- It performs well with small datasets
- It supports analytical update as new samples are added

The models were fitted separately for each group to avoid cross-interference.

Baseline models like **Linear Regression** were also tested within the same loop for comparison.

---

## 5. Results and Evaluation

Key metrics recorded during the sampling process:
- Final **R² score** on the full curve
- **Number of samples required** until early stopping
- Group-wise performance statistics (mean, std)
- Comparison to full-data training

Findings:
- GPR reached high R² (~0.95) with only 5–7 samples on average
- Linear regression needed more samples to stabilize
- Early stopping reduced computation and labeled data usage significantly

Visualization included:
- Price prediction curves over 30 days
- R² vs. iterations graph
- Uncertainty decay per group

---

## Conclusions

Part B demonstrates how smart sampling can drastically reduce the need for labeled data without compromising prediction quality.

Key takeaways:
- GPR is a strong candidate for active learning in regression
- Sampling by model uncertainty is effective and interpretable
- Structuring data by groups enables scalable parallel modeling

This approach is applicable to many domains where full data is unavailable or costly – such as pricing, medical diagnostics, and supply chain forecasting.

---

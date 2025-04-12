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

# [Part A – Data Collection, Enrichment and Baseline Modeling](./PartA)

This part covers the full pipeline from raw data acquisition through web scraping to baseline model construction and evaluation.

At the heart of this section lies the **dataset creation**, which was performed entirely from scratch using real-world hotel booking data. We scraped hotel prices from both **Booking.com** and **Expedia**, processed them into structured CSVs, and gradually transformed the raw data into a clean, analysis-ready format.  
The final dataset used for modeling is [`merged.csv`](./Data/merged.csv), which combines and harmonizes multiple snapshots from both sources.

Additional data sources involved in earlier stages include:
- Booking.com snapshots: [`booking_com_20250310.csv`](./Data/booking_com_20250310.csv), [`booking_com_20250312.csv`](./Data/booking_com_20250312.csv), [`booking_com_20250317.csv`](./Data/booking_com_20250317.csv)
- Expedia snapshots: [`expedia_results - 14.03.2025.csv`](./Data/expedia_results%20-%2014.03.2025.csv), [`expedia_results - 16.03.2025.csv`](./Data/expedia_results%20-%2016.03.2025.csv), [`expedia_results - 18.03.2025.csv`](./Data/expedia_results%20-%2018.03.2025.csv)
- Intermediate files: [`merged_booking.csv`](./Data/merged_booking.csv), [`matched_expedia.csv`](./Data/matched_expedia.csv), [`combined_expedia_results.csv`](./Data/combined_expedia_results.csv), [`merged_booking_final.csv`](./Data/merged_booking_final.csv)
- Evaluation analysis: [`price_differences.csv`](./Data/price_differences.csv)

Each of these played a specific role in the data pipeline — from raw scraping to data merging, cleaning, matching and ultimately, modeling.

---

## [1. Web Scraping and Raw Data Collection](./PartA/PartA1)

The project began with a comprehensive data acquisition phase, in which we developed dedicated web scrapers to collect real-time hotel pricing data from major travel platforms, specifically **Booking.com** and **Expedia**.  
This stage required handling complex page dynamics, including asynchronous content loading, JavaScript-rendered elements, scrolling mechanisms, and modal interference. Using **Playwright**, we simulated browser behavior to capture dynamic content, while **BeautifulSoup** was employed for static HTML parsing.  
Scraping sessions were executed across multiple hotels, locations, and dates, resulting in structured CSV snapshots that reflect the booking state at the time of access. The data includes hotel metadata, room-level details, occupancy, price, and temporal context such as snapshot and check-in dates.

This collected dataset formed the raw input for all downstream cleaning, transformation, and modeling phases.

Scrapers were implemented using:
- `Playwright` (for dynamic and asynchronous content)
- `BeautifulSoup` (for static content parsing)

Two separate notebooks demonstrate this stage:
- [Booking.com Scraper](./PartA/PartA1/Booking_Scraper.py)
- [Expedia Scraper](./PartA/PartA1/Expedia_Scraper.ipynb)

Challenges handled:
- Dynamic content rendering and delayed loading
- Popup handling, scrolling logic, pagination
- Parsing inconsistent HTML and preventing duplicate results

Data attributes scraped:
- Hotel name, location, star rating
- Room type, check-in/check-out dates
- Snapshot date, occupancy, price, discount info

---

## [2. Preprocessing and Cleaning](./PartA/PartA2.ipynb)

Once the data was collected, a thorough **cleaning pipeline** was applied.  
This ensured a consistent, analysis-ready dataset for downstream tasks.

Preprocessing actions:
- Removing rows with missing or malformed values
- Unifying date formats (`Snapshot Date`, `Check-in Date`)
- Casting prices to float and standardizing currencies
- Parsing encoded fields like room type and discount code
- Ensuring uniqueness per hotel-date entry

Final output: Clean DataFrame containing only valid hotel offers.

---

## [3. Feature Engineering](./PartA/PartA3.ipynb)

The clean dataset was further enriched using custom feature engineering logic.  
This step created new fields to better capture temporal, contextual, and behavioral patterns.

Highlights:
- Extracted day-of-week, holiday, and seasonality indicators
- Generated binary flags for late bookings, discount usage, and holidays
- Calculated stay length and lead time
- Created composite group identifiers for `(Hotel, Date, Discount Code)`
- Normalized numerical features using standard scaling

This process significantly enhanced the model-readiness of the dataset.

---

## [4. Baseline Modeling](./PartA/PartA4.ipynb)

A set of **baseline regressors** were trained to predict hotel prices, using the engineered features.

Models compared:
- **Linear Regression** – Simple and interpretable
- **Random Forest** – Non-linear with ensemble power
- **K-Nearest Neighbors** – Distance-based local learning

Each model was evaluated using:
- R² score  
- MAE (Mean Absolute Error)  
- Train/Test split with cross-validation by hotel group

Visualizations included:
- Predictions vs actual prices  
- Error distribution plots  
- Hotel-level breakdowns

---

## [5. Summary & Insights](./PartA/PartA5.ipynb)

Conclusions from Part A:
- Tree-based models outperform linear baselines but still suffer from overfitting in sparse data regions  
- KNN is unstable in high-dimensional contexts  
- Room price behavior varies strongly by date, hotel, and booking strategy

These findings motivated the move to **Part B**, where the goal is to sample and predict more **efficiently** using **Gaussian Process Regression** and active learning.

---

# [Part B – Smart Sampling and Gaussian Process Regression](./PartB)

This part focuses on using the cleaned dataset efficiently through active learning and advanced regression techniques.

## [1. Motivation and Problem Framing](./PartB/PartB1.ipynb)

After establishing a baseline in Part A, the focus in Part B shifts from collecting more data to **using data more efficiently**.  
The goal is to train accurate predictive models while minimizing the number of data points used – a real-world scenario where data is expensive or limited.

We explore **active learning** and **sampling-efficient regression** techniques using:
- Iterative sampling strategies
- Gaussian Process Regression (GPR)
- Early stopping based on model confidence and R² improvement

This part of the project evaluates not only model accuracy, but also **data acquisition cost vs. predictive gain**.

---

## [2. Data Grouping & Model Comparison](./PartB/PartB2)

In this section, the dataset is grouped by `Hotel Name`, `Snapshot Date`, and `Discount Code`, enabling localized learning per scenario.

Each group represents a separate forecasting task, with 30 sequential days to predict. This grouping allows:
- Independent regression model per group
- Fine-grained performance analysis
- Evaluation of generalization by hotel/date

We benchmark multiple models on this structure:
- [K-Nearest Neighbors (KNN)](./PartB/PartB2/KNN.ipynb)
- [Random Forest (RF)](./PartB/PartB2/RF.ipynb)
- [Decision Trees (DT)](./PartB/PartB2/DT.ipynb)
- [XGBoost](./PartB/PartB2/XG.ipynb)
- [Naive Bayes (NB)](./PartB/PartB2/NB.ipynb)

Additionally, [B2Compare.ipynb](./PartB/PartB2/B2Compare.ipynb) consolidates the evaluation metrics across models:
- R² score per group and overall
- MAE comparison
- Distribution of prediction errors

### Key Observations:
- Tree-based models (RF, XGBoost) achieved highest average R²
- KNN and NB were less stable, especially on sparse groups
- Grouping by hotel-date-code dramatically improves model interpretability and analysis granularity

These results serve as a prelude to the **iterative sampling framework** in the next section.

---

## [3. Iterative Sampling Strategy](./PartB/PartB3.ipynb)

This section implements a **progressive sampling algorithm**, where the model actively selects which data point to query next based on uncertainty.

### Sampling Loop Overview:
1. Initialize with 2–3 randomly sampled points per group.
2. Train Gaussian Process Regressor (GPR) on known points.
3. Predict the full 30-day curve and compute:
   - Mean prediction
   - Standard deviation (model uncertainty)
   - R² score against true prices
4. Sample the point with **highest uncertainty** and add it to the training set.
5. Repeat until:
   - R² stagnates across multiple iterations
   - Model uncertainty drops below a threshold
   - Maximum iteration count is reached (typically 10)

The loop mimics **real-world data acquisition**, where only the most informative data is acquired.

---

## [4. Gaussian Process Regression Implementation](./PartB/PartB4.ipynb)

GPR was selected as the main regressor due to its analytical prediction of both **mean and uncertainty**, making it ideal for active learning.

### Model Details:
- Used `sklearn.gaussian_process.GaussianProcessRegressor`
- Composite kernel: constant + RBF + white noise
- Supports continuous updates as new points are sampled
- Fits separately per hotel group to maintain independence

Additional comparisons were made with:
- **Linear Regression** (as a control baseline)
- **Uncertainty-only sampling vs. R²-guided** sampling

Results demonstrated that GPR efficiently balanced exploitation (accuracy) and exploration (uncertainty).

---

## [5. Results, Evaluation, and Insights](./PartB/PartB5.ipynb)

This section consolidates the performance metrics and conclusions drawn from the smart sampling phase using Gaussian Process Regression (GPR) and baseline models.

The iterative approach proved highly efficient, delivering accurate predictions with a significantly reduced number of samples.  
By combining uncertainty-based acquisition and early stopping, we minimized data usage while maintaining strong predictive power.

### Key Metrics Observed:
- **R² Score**: GPR consistently achieved **>0.94 R²** on average
- **Sampling Efficiency**: Only **6–8 samples** were required per group
- **Cost Reduction**: Early stopping saved ~75% of potential sampling effort
- **Comparative Models**: Linear regression required nearly full data to converge
- **Convergence Dynamics**: Sampling policy affected convergence speed more than final accuracy

### Visualizations Included:
- Price prediction vs. actual price curves
- R² progression graphs by iteration
- Uncertainty heatmaps over time
- Histograms of sample counts across groups

### Final Reflections:

These results validate the effectiveness of **smart sampling** for structured regression tasks.  
Uncertainty-aware models like GPR can make confident decisions with minimal data, making them ideal for domains where data collection is costly, time-consuming, or limited by design.

This methodology is highly transferable to other fields such as:
- Medical diagnostics (limited labeled patient data)
- Forecasting under budget constraints
- Industrial quality control and monitoring

---

# Project Conclusion

This project demonstrated a full-cycle data science process — from web scraping and preprocessing to baseline modeling and advanced active learning strategies.  
By separating the project into two distinct phases, we were able to explore both the **challenges of data acquisition** and the **opportunities in sample-efficient modeling**.

Key takeaways:
- Scraping real-world booking data is feasible but requires robust handling of dynamic content and inconsistencies.
- Feature engineering and grouping logic are crucial for model success.
- Traditional models (Random Forest, XGBoost) can perform well but are outmatched in efficiency by uncertainty-based sampling.
- Gaussian Process Regression, combined with active learning, offers a scalable and intelligent approach to reduce data usage while maintaining high accuracy.

Overall, this project illustrates how thoughtful system design can replace brute-force data collection — a valuable lesson for real-world, resource-constrained machine learning applications.


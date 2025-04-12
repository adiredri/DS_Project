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
To handle the complexity of dynamic websites, we used a hybrid scraping strategy. **Playwright** was used to simulate full browser behavior and interact with JavaScript-rendered content (like infinite scroll and modals), while **BeautifulSoup** handled the parsing of static HTML structures.

The scraping process was split between two dedicated scripts:
- [Booking.com Scraper](./PartA/PartA1/Booking_Scraper.py) – focused on capturing structured price data from Booking.com.
- [Expedia Scraper](./PartA/PartA1/Expedia_Scraper.ipynb) – extracted comparable pricing from Expedia’s platform.

During development, we encountered multiple scraping challenges such as asynchronous loading, dynamic pagination, and inconsistent markup. These were addressed through wait-time calibration, conditional parsing, and result deduplication.

The final output included critical booking information such as:
hotel name, location, star rating, room type, check-in/check-out dates, snapshot timestamp, occupancy, price, and discount-related metadata.

---

## [2. Preprocessing and Cleaning](./PartA/PartA2.ipynb)

Before modeling could begin, the raw scraped data had to be thoroughly cleaned and unified.  
This step focused on transforming semi-structured CSV snapshots into a reliable, analysis-ready format, ensuring data consistency and reducing noise.

Preprocessing actions:
- Removing rows with missing or malformed values
- Unifying date formats (`Snapshot Date`, `Check-in Date`)
- Casting prices to float and standardizing currencies
- Parsing encoded fields like room type and discount code
- Ensuring uniqueness per hotel-date entry

Final output: Clean DataFrame containing only valid hotel offers.

---

## [3. Feature Engineering](./PartA/PartA3.ipynb)

With a clean dataset in place, the next step was to enrich it with meaningful features.  
The goal here was to expose temporal, behavioral, and contextual signals that could improve model learning and prediction accuracy.

Highlights:
- Extracted day-of-week, holiday, and seasonality indicators
- Generated binary flags for late bookings, discount usage, and holidays
- Calculated stay length and lead time
- Created composite group identifiers for `(Hotel, Date, Discount Code)`
- Normalized numerical features using standard scaling

This process significantly enhanced the model-readiness of the dataset.

---

## [4. Baseline Modeling](./PartA/PartA4.ipynb)

To establish a benchmark for price prediction, we trained several baseline regression models on the enriched dataset.  
These models served as a reference point for later comparisons with smarter, sampling-based methods.

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

This final notebook in Part A consolidates the results from the baseline models and extracts actionable insights.  
It also reflects on the limitations of traditional modeling in the context of sparse and variable hotel pricing data.

Conclusions from Part A:
- Tree-based models outperform linear baselines but still suffer from overfitting in sparse data regions  
- KNN is unstable in high-dimensional contexts  
- Room price behavior varies strongly by date, hotel, and booking strategy

These findings motivated the move to **Part B**, where the goal is to sample and predict more **efficiently** using **Gaussian Process Regression** and active learning.

---

# [Part B – Smart Sampling and Gaussian Process Regression](./PartB)

This part focuses on the efficient use of data through **active learning**, **group-wise modeling**, and **sampling-aware regression** techniques.

At this stage, we started from the pre-cleaned and enriched dataset produced in Part A.  
The raw source file, [`hotels_data.csv`](./Data/hotels_data.csv), was used as the input for this phase and underwent additional transformation in [PartB1.ipynb](./PartB/PartB1.ipynb) to generate [`Hotels_data_Changed.csv`](./Data/Hotels_data_Changed.csv) — which served as the **primary dataset** for all modeling tasks in Part B.

The other files in the `/Data` folder include:
- [`base_model_results.csv`](./Data/base_model_results.csv) and [`gpr_model_results.csv`](./Data/gpr_model_results.csv) – performance logs from different modeling strategies  
- [`hotels_clustering_data.csv`](./Data/hotels_clustering_data.csv) and [`PySpark_hotels_clustering_data.csv`](./Data/PySpark_hotels_clustering_data.csv) – used in exploratory analysis and future clustering experiments  
- [`Hotels_Best_Discount.csv`](./Data/Hotels_Best_Discount.csv) – summary of top discount configurations per group  
- [`Hotel Clusters Visualization (PySpark).png`](./Data/Hotel%20Clusters%20Visualization%20(PySpark).png) – visual output of clustering analysis (auxiliary)

From this point forward, the dataset is treated as stable and is grouped by hotel and date to enable localized predictive modeling.

---

## [1. Motivation and Problem Framing](./PartB/PartB1.ipynb)

Having built a reliable dataset in Part A, the second phase of the project shifts focus from *collecting more data* to *using data more intelligently*.  
Real-world applications often face constraints on labeling or acquiring samples — particularly in pricing, healthcare, and logistics. Our goal is to train models that can perform well **with fewer, but more informative, samples**.

This part of the project introduces:
- **Sampling-aware modeling strategies**
- **Group-wise problem decomposition**
- **Uncertainty-based active learning**

We start by taking the cleaned dataset ([`Hotels_data_Changed.csv`](./Data/Hotels_data_Changed.csv)) and grouping it based on:
- `Hotel Name`
- `Snapshot Date`
- `Discount Code`

Each group becomes a mini time-series regression task — predicting hotel prices for a 30-day horizon leading up to the check-in date.  
This structure allows the modeling of booking behavior in a localized and interpretable way, setting the stage for iterative sampling strategies in subsequent steps.

---

## [2. Data Grouping & Model Comparison](./PartB/PartB2)

This section focuses on evaluating various regression models across **grouped sub-problems**.  
Each group — defined by a unique combination of `Hotel Name`, `Snapshot Date`, and `Discount Code` — represents a distinct forecasting task, where the model must predict room prices over the 30 days leading to check-in.

This granular decomposition enables:
- Independent training and evaluation per group
- Better interpretability of model behavior across different hotel profiles
- Fair comparison of model generalization across booking contexts

The following models were trained and evaluated:
- [K-Nearest Neighbors (KNN)](./PartB/PartB2/KNN.ipynb)
- [Random Forest (RF)](./PartB/PartB2/RF.ipynb)
- [Decision Trees (DT)](./PartB/PartB2/DT.ipynb)
- [XGBoost](./PartB/PartB2/XG.ipynb)
- [Naive Bayes (NB)](./PartB/PartB2/NB.ipynb)

Evaluation metrics were consolidated in [B2Compare.ipynb](./PartB/PartB2/B2Compare.ipynb), including:
- R² scores per group
- Mean Absolute Error (MAE) comparison
- Histograms and boxplots of model performance

The analysis confirmed that:
- Tree-based models (Random Forest, XGBoost) achieved the highest overall accuracy
- Simpler models (KNN, NB) struggled with sparse or irregular groups
- Grouping data by hotel and date improves not only accuracy, but also diagnostic clarity

This comparison phase served as a strong diagnostic benchmark, providing insight into model limitations and preparing the groundwork for **iterative, uncertainty-based sampling** in the next stage.

---

---

## [3. Iterative Sampling Strategy](./PartB/PartB3.ipynb)

Building on the grouped structure established earlier, this section introduces a **progressive sampling framework** that simulates real-world data acquisition under constraints.

Instead of accessing all 30 days of pricing data upfront, the model begins with only a few observed points and must **selectively query additional days** based on model-driven criteria.  
This process mimics scenarios where labeling or obtaining samples is expensive — requiring the model to be smart about which data it asks for.

### Sampling Loop Workflow:
1. **Initialization**: Begin with 2–3 randomly chosen data points per group.
2. **Model Training**: Fit a **Gaussian Process Regressor (GPR)** using the current subset.
3. **Prediction & Evaluation**:
   - Predict the full 30-day curve.
   - Measure R² and compute model **uncertainty** (posterior standard deviation).
4. **Acquisition Step**:
   - Select the next day with the highest uncertainty.
   - Add this point to the training set.
5. **Stopping Criteria**:
   - R² stagnation across recent iterations.
   - Uncertainty drops below threshold.
   - Max iteration limit reached (typically 10).

This method leverages **Bayesian reasoning** to guide the sampling process, targeting the most informative data first — a key principle in **active learning**.

Performance was tracked over iterations, capturing:
- R² progression
- Uncertainty decay
- Number of samples used per group

The strategy proved effective in reducing data needs while maintaining strong predictive performance — setting the stage for the formal use of **Gaussian Process Regression** in the next section.

---

## [4. Gaussian Process Regression Implementation](./PartB/PartB4.ipynb)

This section provides a detailed look at the implementation and rationale behind using **Gaussian Process Regression (GPR)** as the core model in our sampling loop.

GPR is particularly suited for small, structured datasets and offers two major advantages:
- A **mean prediction** for each query point
- An **uncertainty estimate** (standard deviation) that quantifies confidence

These properties make GPR ideal for **uncertainty-aware sampling**, enabling the model to identify where more data is needed.

### Model Configuration:
- Implemented using `sklearn.gaussian_process.GaussianProcessRegressor`
- Used a composite kernel:
  - **Constant Kernel** – controls signal strength
  - **RBF Kernel** – captures smooth trends in pricing
  - **White Noise Kernel** – accounts for data variability
- Trained separately on each group (`Hotel Name`, `Snapshot Date`, `Discount Code`)
- Supports incremental updates as new samples are added

### Additional Comparisons:
To evaluate the sampling strategy, we also benchmarked:
- **Linear Regression** – as a low-complexity baseline
- **Uncertainty-only vs. R²-aware sampling** – to measure trade-offs in convergence speed and accuracy

Results showed that GPR, with informed sampling, consistently outperformed static models in both **efficiency** and **predictive accuracy**, confirming its value in real-world, limited-data contexts.

---

---

## [5. Results, Evaluation, and Insights](./PartB/PartB5.ipynb)

This final section consolidates the performance results of the smart sampling framework.  
The models were evaluated not only on accuracy, but also on their **efficiency** in using data — a key metric in scenarios where labeling or acquisition is costly.

The **Gaussian Process Regressor (GPR)** proved to be both accurate and economical.  
By selecting points with the highest uncertainty and stopping when gains plateaued, the model learned robustly while minimizing the number of queries.

### Key Metrics Observed:
- **R² Score**: GPR consistently achieved >0.94 R² on average  
- **Sampling Efficiency**: Only 6–8 samples per group were typically needed  
- **Cost Reduction**: Early stopping reduced labeling needs by ~75%  
- **Baseline Comparison**: Linear Regression required nearly the full dataset to stabilize  
- **Convergence Dynamics**: GPR converged faster, and more reliably under uncertainty-aware sampling

### Visualizations Included:
- Price prediction curves vs. true prices
- R² progression per iteration
- Heatmaps of model uncertainty over time
- Distribution histograms of sampled points per group

These results reinforce the project’s main claim: **smart sampling + GPR = high accuracy with minimal data**.  
The methodology offers strong potential for deployment in industries where resource constraints are a major concern.

---

## Final Reflections on Part B

The second half of the project demonstrated the practical benefits of **smart sampling** in regression problems.  
Rather than relying on full datasets, we showed that a well-structured **active learning loop**, guided by model uncertainty, can achieve strong performance with significantly fewer samples.

### Takeaways:
- Group-wise modeling is a powerful way to decompose complex datasets into manageable units.
- GPR excels in low-data regimes thanks to its probabilistic foundations and built-in uncertainty estimation.
- Iterative sampling policies, combined with early stopping, reduce overhead without sacrificing accuracy.
- Performance varies across hotel groups — reinforcing the value of personalized learning rather than a one-size-fits-all model.

This approach is well-suited to domains like:
- Dynamic pricing optimization  
- Medical diagnosis with limited patient data  
- Quality assurance in manufacturing pipelines  
- Personalized recommendation systems

---

# Project Conclusion

This project demonstrated a full-cycle data science pipeline — from **raw data acquisition and enrichment** (Part A), to **sample-efficient modeling and evaluation** (Part B).

We successfully:
- Built a complete dataset using custom web scrapers targeting real booking platforms  
- Engineered and cleaned features to enhance learnability  
- Trained and compared multiple baseline models for regression  
- Implemented Gaussian Process Regression with uncertainty-based sampling  
- Achieved >0.94 R² while reducing labeled data usage by over 70%

The project highlights the importance of **designing smarter systems, not just gathering more data**.  
In real-world scenarios where cost and complexity are limiting factors, **adaptive sampling** and **probabilistic modeling** offer a scalable, intelligent alternative.

---

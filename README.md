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

To evaluate the predictive potential of our enriched dataset, we trained several baseline regression models.  
The goal was to set a performance benchmark using classical methods, before transitioning to more sophisticated approaches in Part B.  
This step also helped us understand the strengths and weaknesses of traditional models when applied to the highly dynamic context of hotel pricing.

We experimented with three popular regressors:

- **Linear Regression**: A straightforward model, good for interpretability but often limited in expressiveness.
- **Random Forest**: An ensemble of decision trees that handles non-linear relationships well.
- **K-Nearest Neighbors (KNN)**: A non-parametric model relying on similarity in feature space, sensitive to local data structure.

Each model was trained and tested using a **Train/Test split**, with **cross-validation applied per hotel group** to account for structural differences across hotels and dates.

To analyze the results, we used both numeric metrics and visual diagnostics:

- **R² score** and **Mean Absolute Error (MAE)** were calculated for each model.
- Visualizations included predicted vs. actual price plots, error distribution histograms, and hotel-level performance breakdowns.

Overall, the results showed that while tree-based models like Random Forest performed better than linear methods, they still suffered from overfitting in sparse regions.  
KNN's performance varied significantly across hotel types, reinforcing the need for group-specific learning strategies.

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

This grouping strategy allowed us to perform localized modeling per hotel context, improving both interpretability and evaluation granularity.  
Instead of training one global model across all data, each group was treated as a self-contained learning problem — better reflecting the real-world variability between hotels, seasons, and discount policies.

The following models were trained and evaluated:

- **[K-Nearest Neighbors (KNN)](./PartB/PartB2/KNN.ipynb)** - A simple, non-parametric method that works well for local patterns but suffers in sparse or high-dimensional spaces.

- **[Random Forest (RF)](./PartB/PartB2/RF.ipynb)** - An ensemble of decision trees offering strong accuracy and robustness, though prone to overfitting small groups.

- **[Decision Tree (DT)](./PartB/PartB2/DT.ipynb)** - A fast, interpretable model that provides a baseline for tree structures but lacks generalization on complex patterns.

- **[XGBoost](./PartB/PartB2/XG.ipynb)** - A gradient-boosted tree ensemble that achieved the highest average performance due to its ability to model nuanced dependencies.

- **[Naive Bayes (NB)](./PartB/PartB2/NB.ipynb)** - A probabilistic model based on independence assumptions; lightweight but limited in expressive power for continuous regression.

- **[B2Preparing.ipynb](./PartB/PartB2/B2Preparing.ipynb)** – A preparatory notebook used to prototype the grouping logic and validate modeling infrastructure before scaling up.

- **[B2Compare.ipynb](./PartB/PartB2/B2Compare.ipynb)** - Consolidates performance results and enables head-to-head comparisons across all models using R², MAE, and error analysis.

Evaluation metrics in the comparison notebook included R² scores per group, MAE comparisons, and visual analysis through histograms and boxplots.

The analysis confirmed several important insights:

- Tree-based models (Random Forest, XGBoost) achieved the highest overall accuracy  
- Simpler models like KNN and Naive Bayes showed inconsistent results, particularly in sparse or irregular groups  
- The group-wise setup offered clear diagnostic value, revealing where models excelled or failed in specific hotel contexts

This stage served as a diagnostic baseline — not just to identify the best model, but to understand the variability in performance across different hotel-date segments.  
It provided a robust foundation for the iterative and uncertainty-driven sampling approach introduced in the next section.

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

This section provides an in-depth explanation of why **Gaussian Process Regression (GPR)** was selected as the main regression technique in our sampling framework, and how it was configured and evaluated.

GPR is particularly effective in small-data regimes due to its probabilistic foundation. It offers both a **mean prediction** and a **quantified uncertainty estimate**, which are critical for guiding active learning. These two outputs allow the model not only to predict prices but also to assess where it's uncertain — a valuable capability when data acquisition is limited or costly.

We applied GPR to each group independently, treating each hotel-date-discount group as a localized regression task. The model's ability to update incrementally made it well-suited for the progressive sampling loop described earlier.

### Model Configuration:
- Implemented using `sklearn.gaussian_process.GaussianProcessRegressor`
- Used a composite kernel:
  - **Constant Kernel** – controls signal strength
  - **RBF Kernel** – captures smooth trends in pricing
  - **White Noise Kernel** – accounts for data variability
- Trained separately on each group (`Hotel Name`, `Snapshot Date`, `Discount Code`)
- Supports incremental updates as new samples are added

Beyond GPR, we included additional benchmarks to validate the sampling framework's robustness. In particular, we compared GPR to **Linear Regression** as a simple baseline, and explored the differences between **uncertainty-driven sampling** and sampling based on **R² improvement**.

These comparisons highlighted GPR’s superiority in balancing exploration and exploitation. With fewer samples, the GPR models achieved high accuracy and demonstrated more stable convergence than their deterministic counterparts. This reinforced the effectiveness of combining probabilistic modeling with smart sampling in data-scarce environments.

---

## [5. Results, Evaluation, and Insights](./PartB/PartB5.ipynb)

This final section summarizes the effectiveness of the smart sampling strategy, with a particular focus on the **efficiency, convergence behavior, and generalizability** of the Gaussian Process Regression (GPR) model under real-world constraints.

Our evaluation prioritized not just raw accuracy, but the trade-off between **predictive power and data usage** — a core concern in domains where labels are expensive or slow to acquire. The GPR model, with its uncertainty estimates and smooth interpolation capabilities, proved especially well-suited for this context.

### Key Outcomes and Insights:

The smart sampling loop demonstrated that:
- **High accuracy can be achieved with limited data**: GPR consistently surpassed an R² of 0.94 on average, often with only **6–8 samples per group**.
- **Sampling selectivity outperforms brute force**: Instead of feeding the model with all 30 days of pricing data, selective querying (guided by uncertainty) allowed the model to learn faster and more reliably.
- **Cost-effective learning is viable**: Early stopping, based on uncertainty and R² stagnation, helped reduce the labeling burden by up to **75%**, without compromising prediction quality.

Compared to baseline models like Linear Regression — which often required the full dataset to stabilize — GPR was able to generalize well with fewer examples, making it a powerful solution in low-resource settings.

### Tools and Evaluation Techniques:

Throughout this section, we employed:
- **Iterative metrics tracking** (R² vs. iteration) to monitor learning efficiency
- **Uncertainty heatmaps** to assess model confidence across the prediction window
- **Sampling frequency histograms** to measure how many queries were needed per group
- **Curve overlays** (prediction vs. ground truth) to visually validate model behavior

These tools provided both **quantitative backing** and **visual diagnostics** for model decisions — allowing us to extract deeper insights into **when the model learns**, **where it struggles**, and **how much data is truly necessary**.

The results highlight the synergy between **probabilistic modeling and strategic data acquisition**.  
By combining GPR with a feedback-driven sampling policy, we developed a learning process that is not only accurate but also **adaptive, interpretable, and efficient** — essential qualities for real-world deployment where resources are limited and time is critical.


---

## Final Reflections on Part B

The second phase of the project showcased the effectiveness of **smart sampling** strategies for regression tasks under data constraints.  
Rather than accessing full datasets upfront, we developed an **active learning framework** where the model selectively acquires the most informative data points based on uncertainty.

This approach, powered by **Gaussian Process Regression (GPR)**, allowed us to reduce labeling effort significantly while preserving high predictive accuracy. It also enabled group-specific learning that adapts to the behavior of individual hotel segments.

Key benefits observed:
- Adaptive sampling led to faster convergence and fewer data requirements
- GPR provided both predictions and confidence levels, improving model interpretability
- Localized group modeling outperformed global, one-size-fits-all approaches

This methodology is broadly applicable to domains where labeled data is scarce or expensive — such as dynamic pricing, healthcare diagnostics, and industrial monitoring.

---

# Project Conclusion

This project represents a complete data science workflow, integrating both **data acquisition** and **efficient modeling** into a unified pipeline.  
In Part A, we built the dataset from scratch using robust web scraping and enrichment techniques. In Part B, we shifted the focus to learning **more with less** through probabilistic modeling and iterative sampling.

By combining:
- Real-world data scraping from live booking platforms  
- Thoughtful feature engineering and group-wise decomposition  
- Baseline regressors and advanced models like GPR  
- An active learning loop guided by model uncertainty

—we achieved accurate price predictions with minimal data overhead.

Ultimately, the project underscores a key principle:  
**Strategic modeling choices can often outperform brute-force data collection**, leading to smarter, leaner, and more scalable machine learning systems.


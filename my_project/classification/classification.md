# Customer Churn Classification

A comprehensive machine learning pipeline for predicting customer churn using advanced data cleaning, multiple classification models, and extensive evaluation metrics.



### 1. Install Dependencies
```bash
pip install pandas numpy scikit-learn imbalanced-learn matplotlib seaborn scipy psycopg2-binary python-dotenv
```

Or use a requirements file:
```bash
pip install -r requirements.txt
```

### 2. Set Up Environment (Optional for PostgreSQL)
Create a `.env` file with your database credentials:
```env
DB_HOST=your_postgres_host
DB_NAME=your_database_name
DB_USER=your_username
DB_PASS=your_password
```
## Notebook Sections

| Section | Description | Key Outputs |
|---------|-------------|-------------|
| 0 | Import Libraries | ✓ Confirmation message |
| 1 | Data Loading | Dataset shape, first 5 rows |
| 2 | Data Cleaning | Cleaning statistics, outlier counts |
| 3 | Train-Test Split | Class distributions |
| 4 | Handle Imbalance | Balanced dataset counts |
| 5 | Feature Scaling | Scaled data statistics |
| 6 | Train Models | Cross-validation scores |
| 7 | Model Comparison | Performance comparison table |
| 8 | Detailed Evaluation | Classification report, confusion matrix |
| 9 | Feature Importance | Top 10 features + visualization |
| 10 | Overfitting Analysis | Train vs Test gaps |
| 11 | ROC Curves | Visual model comparison |
| 12 | Predictions | Example predictions on new data |
| 13 | Summary | Final recommendations |

## 📈 Results Interpretation

### Model Selection Criteria
- **Primary Metric**: ROC-AUC (best for imbalanced classification)
- **Target**: AUC > 0.80 = Good, AUC > 0.90 = Excellent

### Performance Thresholds
```
AUC Score Interpretation:
- 0.90 - 1.00: Excellent
- 0.80 - 0.90: Good
- 0.70 - 0.80: Fair
- 0.60 - 0.70: Poor
- 0.50 - 0.60: Fail (no better than random)
```

### Overfitting Assessment
```
AUC Gap (Train - Test):
- < 0.05: Good generalization ✓
- 0.05 - 0.10: Mild overfitting ⚠️
- > 0.10: Significant overfitting ❌
```

### Prediction Risk Levels
```
Churn Probability:
- > 0.70: HIGH RISK - Immediate intervention needed
- 0.40 - 0.70: MEDIUM RISK - Monitor closely
- < 0.40: LOW RISK - Stable customer
```

## Model Parameters Explained

### Random Forest
```python
n_estimators=500        # Number of trees
max_depth=15           # Maximum tree depth (prevents overfitting)
min_samples_split=20   # Minimum samples to split node
min_samples_leaf=10    # Minimum samples in leaf node
max_features='sqrt'    # Features considered per split
class_weight='balanced' # Handle imbalanced classes
```

### Gradient Boosting
```python
n_estimators=300       # Number of boosting stages
max_depth=5           # Shallow trees (prevents overfitting)
learning_rate=0.05    # Shrinkage parameter
subsample=0.8         # Fraction of samples per tree
```

### Logistic Regression
```python
max_iter=1000         # Maximum iterations
class_weight='balanced' # Handle imbalanced classes
solver='lbfgs'        # Optimization algorithm
```

### 3. **Integration with Power BI**
- Export predictions to CSV
- Load into Power BI for visualization
- Create churn dashboard with risk segments
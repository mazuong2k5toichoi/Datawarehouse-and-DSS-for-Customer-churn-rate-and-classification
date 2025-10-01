# Data Warehouse and DSS for Customer Churn Rate and Classification

This repository contains the assignment for the Data Warehouse and Decision Support System course at HCMUT (Ho Chi Minh University of Technology). The project focuses on analyzing customer churn patterns using data warehouse concepts and exploratory data analysis techniques.

## 📋 Project Overview

This project implements a comprehensive analysis of customer churn using the AdventureWorks sample database. The analysis includes schema exploration, customer behavior analysis, sales pattern investigation, and churn rate calculation with various visualizations to support decision-making processes.

## 🎯 Objectives

- Analyze customer purchase patterns and behavior
- Calculate and visualize customer churn rates
- Identify key factors contributing to customer churn
- Provide insights for customer retention strategies
- Demonstrate data warehouse concepts and DSS techniques

## 🛠️ Technologies Used

- **Database**: Microsoft SQL Server (AdventureWorks sample database)
- **Programming Language**: Python 3.x
- **Libraries**:
  - `pypyodbc` - Database connectivity
  - `pandas` - Data manipulation and analysis
  - `matplotlib` - Data visualization
  - `seaborn` - Statistical data visualization
  - `python-dotenv` - Environment variable management

## 📁 Project Structure

```
├── EDA.ipynb           # Main Jupyter notebook with exploratory data analysis
├── README.md           # Project documentation
├── LICENSE             # License information
├── .env                # Environment variables (not tracked in git)
├── .gitignore          # Git ignore rules
└── .venv/              # Virtual environment (not tracked in git)
```

## 🚀 Getting Started

### Prerequisites

1. **SQL Server** with AdventureWorks sample database installed
2. **Python 3.7+** installed on your system
3. **ODBC Driver** for SQL Server

### Installation

1. Clone the repository:
```bash
git clone https://github.com/mazuong2k5toichoi/Datawarehouse-and-DSS-for-Customer-churn-rate-and-classification.git
cd Datawarehouse-and-DSS-for-Customer-churn-rate-and-classification
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate  # On Windows
# source .venv/bin/activate  # On macOS/Linux
```

3. Install required packages:
```bash
pip install pypyodbc pandas matplotlib seaborn python-dotenv jupyter
```

4. Set up environment variables:
   - Copy `.env.example` to `.env` (if available) or create a new `.env` file
   - Configure your database connection:
```
DRIVER_NAME=ODBC Driver 17 for SQL Server
SERVER_NAME=your_server_name
DATABASE_NAME=AdventureWorks2019
```

### Running the Analysis

1. Start Jupyter Notebook:
```bash
jupyter notebook
```

2. Open `EDA.ipynb` and run the cells sequentially to perform the analysis.

## 📊 Analysis Components

### 1. Schema Exploration
- Database schema analysis
- Table count and structure investigation
- Schema organization assessment

### 2. Customer Behavior Analysis
- Customer order count distribution
- Purchase frequency patterns
- Customer segmentation based on order history

### 3. Churn Analysis
- Churn rate calculation (customers with no orders in past 12 months)
- Churn status distribution visualization
- Customer lifecycle analysis

### 4. Sales Performance Analysis
- Sales by region and product category
- Customer spend distribution analysis
- Revenue contribution analysis (Pareto principle)

### 5. Geographic Analysis
- Customer distribution by territory
- Churn patterns across different regions
- Territory-specific performance metrics

## 📈 Key Insights

The analysis provides insights into:

- **Customer Segmentation**: Identification of customer groups based on purchase behavior
- **Churn Patterns**: Understanding which customer segments are at higher risk of churning
- **Revenue Distribution**: Analysis of how revenue is distributed across customer base
- **Geographic Trends**: Regional differences in customer behavior and churn rates
- **Product Performance**: Category-wise sales performance across regions

## 🔍 Visualizations

The project includes various types of visualizations:

- **Histograms**: Customer purchase frequency distribution
- **Bar Charts**: Sales by region and product category
- **Box Plots**: Customer spend distribution
- **Pie Charts**: Customer churn status distribution
- **Geographic Analysis**: Territory-based customer and churn analysis

## 📝 Methodology

1. **Data Connection**: Establish secure connection to SQL Server database
2. **Schema Analysis**: Explore database structure and relationships
3. **Data Extraction**: Query relevant customer and sales data
4. **Data Processing**: Clean and prepare data for analysis
5. **Statistical Analysis**: Calculate key metrics and KPIs
6. **Visualization**: Create meaningful charts and graphs
7. **Insight Generation**: Interpret results and provide recommendations

## 🤝 Contributing

This is an academic project for coursework. If you find any issues or have suggestions for improvement:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit your changes (`git commit -am 'Add some improvement'`)
4. Push to the branch (`git push origin feature/improvement`)
5. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍🎓 Author

**Course**: Data Warehouse and Decision Support System  
**Institution**: Ho Chi Minh University of Technology (HCMUT)  
**Academic Year**: 2024-2025

## 🙏 Acknowledgments

- HCMUT for providing the academic framework
- Microsoft for the AdventureWorks sample database
- The open-source community for the tools and libraries used

## 📞 Contact

For questions about this project, please create an issue in this repository or contact through the university academic channels.

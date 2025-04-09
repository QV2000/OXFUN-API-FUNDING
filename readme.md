# OX.fun Hourly Data Collector

This repository collects hourly data from the OX.fun exchange, specifically tracking:
- Latest funding rates for all futures markets
- Latest OHLC candle data for each market

## Setup Instructions

### 1. Create a GitHub Repository
Create a new repository and push these files to it:
- `ox_hourly_collector.py` - The main collection script
- `.github/workflows/hourly_collection.yml` - GitHub Actions workflow file
- `README.md` - This readme file

### 2. Set Up GitHub Secrets
You need to add your OX.fun API credentials as repository secrets:

1. Go to your repository on GitHub
2. Navigate to Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Add two secrets:
   - Name: `OX_API_KEY` - Value: Your OX.fun API key
   - Name: `OX_SECRET_KEY` - Value: Your OX.fun secret key

### 3. Enable GitHub Actions
Make sure GitHub Actions is enabled for the repository:
1. Go to the "Actions" tab
2. If prompted, enable workflows

### 4. Initial Manual Run
After setting up the repository and secrets:
1. Go to the "Actions" tab
2. Select the "OX Data Hourly Collection" workflow
3. Click "Run workflow" to trigger the first data collection

### 5. Data Structure
The collected data will be stored in the `data` directory with the following structure:

```
data/
  ├── YYYY-MM-DD/
  │     ├── ox_funding_rates_YYYYMMDD_HHMMSS.csv
  │     ├── ox_candles_YYYYMMDD_HHMMSS.csv
  │     ├── ox_funding_rates_YYYY-MM-DD_consolidated.csv
  │     └── ox_candles_YYYY-MM-DD_consolidated.csv
  └── ox_hourly_data.db
```

- Each hourly run creates timestamped CSV files
- Consolidated files for each day make it easier to analyze daily patterns
- The SQLite database contains the complete historical record

## Using the Data

The collected data can be used to:
- Track funding rate patterns across different markets
- Identify opportunities for funding rate arbitrage
- Build trading strategies based on funding rate anomalies
- Analyze price movements in relation to funding rate changes

## Customization

To modify data collection parameters:
- Change the timeframe (default: '1h') in the script
- Edit the cron schedule in the workflow file for different collection frequencies
- Add additional data points by modifying the script

## Troubleshooting

If the data collection fails:
1. Check the run logs in GitHub Actions tab
2. Verify your API credentials are correct
3. Ensure the repository has proper permissions to push new data
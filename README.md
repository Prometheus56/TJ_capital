# TJ\_capital

**Tools and CLI for analyzing and upserting DefiLlama TVL data, managing CSV tables, and computing a Crypto vs. NASDAQ index.**

---

## Table of Contents

1. [Features](#features)
2. [Getting Started](#getting-started)
3. [Environment Variables](#environment-variables)
4. [Installation](#installation)
5. [Usage](#usage)
6. [Project Structure](#project-structure)
7. [Contributing](#contributing)
8. [License](#license)

---

## Features

* **Chains Analytics**: Calculate TVL percentage changes, group stats, and generate reports via CLI.
* **Upper CLI**: Clean and interpolate raw CSV data, create PostgreSQL tables, bulk-load data, and upsert daily TVL entries.
* **Crypto vs. NASDAQ Index**: Fetch CoinGecko and Yahoo Finance data, build a weighted crypto index, and compare against NASDAQ performance.

## Getting Started

Follow these steps to run the project locally.

### Prerequisites

* Python 3.8+ installed
* PostgreSQL database
* A CoinGecko Pro API key (for extended rate limits)

### Installation

1. **Clone the repo**

   ```bash
   git clone git@github.com:YOUR_USERNAME/tj_capital.git
   cd tj_capital
   ```

2. **Create & activate a virtual environment**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**

   ```bash
   cp .env.example .env
   # Edit .env and fill in your DB_PASSWORD and API_KEY
   ```

## Environment Variables

| Variable      | Description                        |
| ------------- | ---------------------------------- |
| `DB_NAME`     | PostgreSQL database name           |
| `DB_USER`     | PostgreSQL user                    |
| `DB_PASSWORD` | PostgreSQL password (keep secret!) |
| `DB_HOST`     | PostgreSQL host address            |
| `DB_PORT`     | PostgreSQL port (usually 5432)     |
| `API_KEY`     | CoinGecko Pro API key              |

## Usage

### Chains Analytics CLI

```bash
python -m tj_capital.chains \
  --start_date 2023-01-01 --end_date 2023-03-01 \
  pct_change ethereum bitcoin
```

### Upper CLI

Clean and interpolate CSV:

```bash
python -m tj_capital.upper_cli clean data/raw.csv
```

Create a table and bulk load data:

```bash
python -m tj_capital.upper_cli create_table data/raw.csv chains
```

Upsert today’s TVL:

```bash
python -m tj_capital.upper_cli add_row
```

### Crypto vs. NASDAQ

```bash
python -m tj_capital.crypto_class --ids bitcoin ethereum --days 30
```

## Project Structure

```
TJ_capital/
├── .gitignore
├── .env.example
├── README.md
├── requirements.txt
├── src/
│   └── tj_capital/
│       ├── __init__.py
│       ├── chains.py
│       ├── upper_cli.py
│       └── crypto_class.py
└── tests/
    └── test_chains.py
```

## Contributing

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature/XYZ`.
3. Commit your changes: `git commit -m "Add XYZ feature"`.
4. Push to your fork: `git push origin feature/XYZ`.
5. Open a Pull Request.

## License

This project is licensed under the [MIT License](LICENSE).

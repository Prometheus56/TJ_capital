import pandas as pd


class TVLAnal():
    def __init__(self, date):
        self.date = pd.to_datetime(date)
        self.chains_path = "/home/jakub/mnt/vpn_files/Databaze/Defillama/chains.csv"
        self.protocols_path = "/home/jakub/mnt/vpn_files/Databaze/Defillama/protocols.csv"
        self.chains_db = pd.read_csv(self.chains_path, parse_dates=['Date'], index_col= 'Date')
        self.protocols_db = pd.read_csv(self.protocols_path, parse_dates=['Date'], index_col= 'Date')

    def divide_protocols(self):
        """
        Divide protocols into group based on their TVL
        
        Returns:
                Dictionary: A dictionary containing groups divided by amount of TVL
        """
        select = self.protocols_db.loc[self.date]

        ### Define groups and transform name of the columns to list
        imba = select[select > 5000000000].index.to_list()
        large = select[(select <= 5000000000) & (select > 2000000000)].index.to_list()
        big = select[(select <= 2000000000) & (select > 1000000000)].index.to_list()
        medium = select[(select <= 100000000000) & (select > 500000000)].index.to_list()
        small = select[(select <= 500000000) & (select > 150000000)].index.to_list()
        micro = select[select <= 150000000].index.to_list()

        # Store groups into dictionary
        group_list = {
            'imba': imba,
            'large': large,
            'big': big,
            'medium': medium,
            'small': small,
            'micro': micro,
        }
        return group_list
    
    def divide_chains(self):
        """
        Divide protocols into group based on their TVL

        Retruns:
                Dictionary: A dictionary containing groups divided by amount of TVL
        
        """
        select = self.chains_db.loc[self.date]

        ### Define groups and transform name of the columns to list
        imba = select[select > 1000000000].index.to_list()
        large = select[(select <= 1000000000) & (select > 500000000)].index.to_list()
        big = select[(select <= 500000000) & (select > 100000000)].index.to_list()
        medium = select[(select <= 100000000) & (select > 50000000)].index.to_list()
        small = select[(select <= 50000000) & (select > 20000000)].index.to_list()
        micro = select[select <= 20000000].index.to_list()

        # Dictionary to store chains for each group
        group_list = {
            'imba': imba,
            'large': large,
            'big': big,
            'medium': medium,
            'small': small,
            'micro': micro
        }
        return group_list
    
    def pct_change(self, no_days, db_to_analyze):
        """
        Calculate percentage change between self.date and no_days before it.

        Args:
            db_to_analyze (str): "chains" or anything else to select between chains or protocols DF.
            no_days (int): How many days back from self.date to calculate percentage change.

        Returns:
            DataFrame : Percentage change for each column (protocol) at the specified date.
        """
        db = self.chains_db if db_to_analyze == "chains" else self.protocols_db
        
        reference_date = db.index[db.index.get_loc(self.date) - no_days]

        target_data = db.loc[date]
        reference_data = db.loc[reference_date]

        result = ((target_data - reference_data) / reference_data) * 100
        
        return pd.DataFrame(result)

    def top_gainers(self, db_to_analyze, no_days, TopX):
        """
        Calculate top gainers for each group.

        Args:
            db_to_analyze (str): "chains" or anything else to select between chains or protocols DF.
            no_days (int): Number of days for percentage change calculation.
            TopX (int): Number of top gainers to retrieve.

        Returns:
            dict: Top gainers for each group.
            dict: Count of items in each group.
        """
        # Select the database based on the db_to_analyze
        db_groups = self.divide_chains() if db_to_analyze == "chains" else self.divide_protocols()

        group_count = {}
        top_gainers_dict = {}

        ### Calculate the number of items in each group
        for group_name, group_data in db_groups.items():
            group_count[group_name] = len(group_data)

        ### Calculate top gainers for each group
        for group_name, group_data in db_groups.items():
            pct_change = self.pct_change(no_days, db_to_analyze).loc[group_data].dropna()
            top_gainers = pct_change.nlargest(TopX, columns=pct_change.columns[0]) 
            top_gainers_dict[group_name] = top_gainers

        return top_gainers_dict, group_count









date = "2025-01-25"
base = TVLAnal(date)
first = base.top_gainers("chains",25 , 5 )
print(first)



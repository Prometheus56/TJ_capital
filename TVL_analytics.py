import pandas as pd


class AnalKing():
    def __init__(self, path_db, date):
        self.date = pd.to_datetime(date)
        self.path_db = pd.read_csv(path_db, parse_dates= ['Date'], index_col= 'Date', na_values="NAN")
    

    def divide_protocols(self):
        """Divide protocols into group based on their TVL
        parameters: /
        output: dictionary group_list
        
        """
        # Load database and select specific day
        db = self.path_db
        select = db.loc[self.date]

        # Define groups and transform name of the columns to list
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
        """Divide protocols into group based on their TVL
        parameters: /
        output: dictionary group_list
        
        """
        # Load database and select specific day
        db = self.path_db
        select = db.loc[self.date]

        # Define groups and transform name of the columns to list
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
    
    def pct_change(self, no_days):
        """
        Calculate percentage change between self.date and no_days before it.

        Parameters:
            no_days (int): How many days back from self.date to calculate percentage change.

        Returns:
            float: Percentage change for each column (protocol) at the specified date.
        """
        db = self.path_db
        date = self.date
        
        # Ensure `date` is within the DataFrame index
        if date not in db.index:
            raise ValueError(f"The date {date} is not in the database index.")
        
        # Calculate the reference date
        try:
            reference_date = db.index[db.index.get_loc(date) - no_days]
        except IndexError:
            raise ValueError(f"Not enough data to go {no_days} days back from {date}.")
        
        # Select the data for the target and reference dates
        target_data = db.loc[date]
        reference_data = db.loc[reference_date]
        
        # Compute percentage change
        result = ((target_data - reference_data) / reference_data) * 100
        
        return result








protocols_path = "/run/media/jakub/USB DRIVE/Aplikace/Databaze/protocols.csv"
date = "2024-12-12"
base = AnalKing(protocols_path, date)
first = base.pct_change(20)
print(first.head(30))



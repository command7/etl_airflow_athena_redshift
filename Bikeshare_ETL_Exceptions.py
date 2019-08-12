class ETL_Exception(Exception):
    """Custom Exceptions for Bikeshare ETL Pipeline"""
    pass


class Month_Data_Missing(ETL_Exception):
    """Raised if trips data for the current month is missing."""
    pass
"""
Class to migrate data products entity from one instance of starburst to another one
"""


class DatasetMigrant:
    """
    Provide methods to migrate data products entities between instances of Starburst.

    Attributes:
        name (str): The name of the dataset.
        type (str): The type of the dataset.
        product_src (str): The source product name.
        product_dest (str): The destination product name.
        domain_src (str): The source domain name.
        domain_dest (str): The destination domain name.
    """

    def __init__(self, dataset: dict, products_names: dict, domains_names: str):
        """
        Initialize the DatasetMigrant with dataset, product names, and domain names.

        Args:
            dataset (dict): A dictionary containing dataset information.
            products_names (dict): A dictionary with 'src' and 'dest' keys for source and destination product names.
            domains_names (dict): A dictionary with 'src' and 'dest' keys for source and destination domain names.
        """
        self.name = dataset.get("name")
        self.type = dataset.get("type")
        self.product_src = products_names.get("src")
        self.product_dest = dataset.get("productDestName", products_names.get("dest"))
        self.domain_src = domains_names.get("src")
        self.domain_dest = domains_names.get("dest")

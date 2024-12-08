"""
Class to migrate data products entity from instance of starburst to another one
"""
from starburst_api.classes.class_starburst_connection_info import (
    StarburstConnectionInfo,
)
from starburst_api.classes.class_starburst import Starburst
from datamesh_migration.migrators.dataset_migrant import DatasetMigrant
from datamesh_migration.files.starburst_files import read_starburst_files

from datamesh_migration.variables import STARBURST_PROD_CATALOG


class DatameshMigrator:
    """Provide methods to migrate data products entities"""

    def __init__(
        self,
        connection_info_src: StarburstConnectionInfo,
        connection_info_dest: StarburstConnectionInfo,
    ):
        # Creating client for source instance and destination instance
        self.starburst_client_src = Starburst(connection_info_src)
        self.starburst_client_dest = Starburst(connection_info_dest)

    def migrate_dataset(self, migrant: DatasetMigrant):
        """
        Migrates a dataset from a source domain to a destination domain.

        This function checks for the existence of the source and destination domains and data products.
        If the source domain and product are found, and the destination domain and product exist,
        it migrates the dataset from the source to the destination. If the dataset already exists
        at the destination, it will be overwritten.

        Args:
            migrant (DatasetMigrant): An object containing information about the dataset migration,
                                      including the source and destination domains and products,
                                      as well as the dataset name and type.

        Returns:
            None

        """
        # Checking if domain source and domain destination exist
        print(
            f"Checking if domain {migrant.domain_src} exists at source instance({self.starburst_client_src.connection_info.host})"
        )
        domain_src = self.starburst_client_src.get_domain_by_name(
            domain_name=migrant.domain_src, as_class=True
        )
        if not domain_src:
            return

        print(f"Domain {migrant.domain_src} exists")

        print(
            f"Checking if domain {migrant.domain_dest} exists at destination instance({self.starburst_client_dest.connection_info.host})"
        )
        domain_dest = self.starburst_client_dest.get_domain_by_name(
            domain_name=migrant.domain_dest, as_class=True
        )
        if not domain_dest:
            return

        print(f"Domain {migrant.domain_dest} exists...")

        del domain_src
        del domain_dest

        # Checking if product source and product destination exist
        print(
            f"Checking if domain {migrant.domain_src} has product {migrant.product_src} at source instance({self.starburst_client_src.connection_info.host})"
        )
        product_src = self.starburst_client_src.get_data_product(
            domain_name=migrant.domain_src,
            data_product_name=migrant.product_src,
            as_class=True,
        )
        if not product_src:
            return

        print(f"Domain {migrant.domain_src} has product {migrant.product_src}...")

        # Checking if product source and product destination exist
        print(
            f"Checking if domain {migrant.domain_src} has product {migrant.product_src} at destination instance({self.starburst_client_dest.connection_info.host})"
        )
        product_dest = self.starburst_client_dest.get_data_product(
            domain_name=migrant.domain_dest,
            data_product_name=migrant.product_dest,
            as_class=True,
        )
        if not product_dest:
            return

        print(f"Domain {migrant.domain_src} has product {migrant.product_src}...")

        # Checking if dataset exists at source
        print(f"Checking if dataset {migrant.name} exists")
        found = False
        for dataset in getattr(product_src, f"{migrant.type}s"):
            # Overwrite dataset if already exists at destination
            if dataset.name == migrant.name:
                print(f"Dataset {migrant.name} exists, it will be update...")
                setattr(
                    product_dest,
                    f"{migrant.type}s",
                    [
                        dts
                        for dts in getattr(product_dest, f"{migrant.type}s")
                        if dts.name != migrant.name
                    ]
                    + [dataset],
                )
                self.starburst_client_dest.update_data_product(product_dest)
                found = True
                break

        if not found:
            print(f"Dataset {migrant.name} not found")

    def migrate_product(self, domains: dict, product: str):
        """
        Migrates a data product from a source domain to a destination domain.

        This function checks if the source and destination domains exist, and then checks
        if the specified product exists in the source domain. If the product is found, it
        is either updated or created in the destination domain. If the product already exists
        in the destination domain, it will be overwritten.

        Args:
            domains (dict): A dictionary containing the source and destination domain names.
                            Example: {'src': 'source_domain_name', 'dest': 'destination_domain_name'}
            product (str): The name of the product to be migrated.

        Returns:
            None
        """
        # Checking if domain source and domain destination exist
        print(
            f"Checking if domain {domains.get('src')} exists at source instance({self.starburst_client_src.connection_info.host})"
        )
        domain_src = self.starburst_client_src.get_domain_by_name(
            domain_name=domains.get("src"), as_class=True
        )
        if not domain_src:
            return

        print(f"Domain {domains.get('src')} exists...")

        print(
            f"Checking if domain {domains.get('dest')} exists at destination instance({self.starburst_client_dest.connection_info.host})"
        )
        domain_dest = self.starburst_client_dest.get_domain_by_name(
            domain_name=domains.get("dest"), as_class=True
        )
        if not domain_dest:
            return

        print(f"Domain {domains.get('dest')} exists...")

        del domain_src

        # Checking if product exists at source and destination
        print(
            f"Checking if domain {domains.get('src')} has product {product} at source instance({self.starburst_client_src.connection_info.host})"
        )
        product_src = self.starburst_client_src.get_data_product(
            domain_name=domains.get("src"),
            data_product_name=product,
            as_class=True,
        )
        if not product_src:
            return

        print(f"Domain {domains.get('src')} has product {product}...")

        print(
            f"Checking if domain {domains.get('dest')} has product {product} at destination instance({self.starburst_client_dest.connection_info.host})"
        )
        product_dest = self.starburst_client_dest.get_data_product(
            domain_name=domains.get("dest"),
            data_product_name=product,
            as_class=True,
        )
        if product_dest:
            print(f"Domain {domains.get('dest')} has product {product} ...")
            print("Existing datasets will be overwritten")
            product_src.catalog_name = product_dest.catalog_name
            product_src.data_domain_id = product_dest.data_domain_id
            product_src.id = product_dest.id

            self.starburst_client_dest.update_data_product(product_src)
        else:
            print(f"Domain {domains.get('dest')} has not product {product} ...")
            print(f"Product {product} would be create")
            product_src.catalog_name = STARBURST_PROD_CATALOG
            product_src.data_domain_id = domain_dest.id
            self.starburst_client_dest.create_data_product(product_src)

    def migrate_domain(self, domain_name: str):
        """
        Migrate a domain from the source instance to the destination instance.

        This function checks if the domain exists in the source instance and then either creates
        or updates the domain in the destination instance.

        Args:
            domain_name (str): The name of the domain to be migrated.
        """
        # Check if domain exists at the source instance
        print(
            f"Checking if domain {domain_name} exists at source instance ({self.starburst_client_src.connection_info.host})"
        )
        domain_src = self.starburst_client_src.get_domain_by_name(
            domain_name=domain_name, as_class=True
        )
        if not domain_src:
            print(f"Domain {domain_name} does not exist at the source instance.")
            return

        print("Domain exists at source instance.")

        # Check if domain exists at the destination instance
        print(
            f"Checking if domain {domain_name} exists at destination instance ({self.starburst_client_dest.connection_info.host})"
        )
        domain_dest = self.starburst_client_dest.get_domain_by_name(
            domain_name=domain_name, as_class=True
        )

        if not domain_dest:
            # Create the domain at the destination if it does not exist
            print(
                f"Domain {domain_name} does not exist at the destination instance. Creating domain."
            )
            self.starburst_client_dest.create_domain(domain=domain_src)
        else:
            # Update the domain at the destination if it exists
            print(
                f"Domain {domain_name} exists at the destination instance. Updating domain."
            )
            domain_src.id = domain_dest.id
            self.starburst_client_dest.update_domain(domain=domain_src)

    def migrate_all_product_datasets(self, domains: dict, products: dict):
        """
        Migrates all datasets from a source data product to a destination data product within specified domains.

        This function checks for the existence of the source and destination domains and data products.
        If the source domain and product are found, and the destination domain and product exist,
        it migrates all datasets (views and materialized views) from the source product to the destination product.
        If datasets with the same names exist in the destination product, they will be overwritten.

        Args:
            domains (dict): A dictionary containing the source and destination domain names.
                            Example: {'src': 'source_domain_name', 'dest': 'destination_domain_name'}
            products (dict): A dictionary containing the source and destination product names.
                            Example: {'src': 'source_product_name', 'dest': 'destination_product_name'}

        Returns:
            None
        """
        # Checking if domain source and domain destination exist
        print(
            f"Checking if domain {domains.get('src')} exists at source instance({self.starburst_client_src.connection_info.host})"
        )
        domain_src = self.starburst_client_src.get_domain_by_name(
            domain_name=domains.get("src"), as_class=True
        )
        if not domain_src:
            return

        print(f"Domain {domains.get('src')} exists...")

        print(
            f"Checking if domain {domains.get('dest')} exists at destination instance({self.starburst_client_dest.connection_info.host})"
        )
        domain_dest = self.starburst_client_dest.get_domain_by_name(
            domain_name=domains.get("dest"), as_class=True
        )
        if not domain_dest:
            return

        print(f"Domain {domains.get('dest')} exists...")

        del domain_src
        del domain_dest

        # Checking if product exists at source and destination
        print(
            f"Checking if domain {domains.get('src')} has product {products.get('src')} at source instance({self.starburst_client_src.connection_info.host})"
        )
        product_src = self.starburst_client_src.get_data_product(
            domain_name=domains.get("src"),
            data_product_name=products.get("src"),
            as_class=True,
        )
        if not product_src:
            return

        print(f"Domain {domains.get('src')} has product {products.get('src')}...")

        print(
            f"Checking if domain {domains.get('dest')} has product {products.get('dest')} at destination instance({self.starburst_client_dest.connection_info.host})"
        )
        product_dest = self.starburst_client_dest.get_data_product(
            domain_name=domains.get("dest"),
            data_product_name=products.get("dest"),
            as_class=True,
        )
        if not product_dest:
            return

        print(f"Domain {domains.get('dest')} has product {products.get('dest')}...")

        # Existing datasets will be overwritten
        src_views_names = [view_src.name for view_src in product_src.views]
        src_mv_views_names = [
            mv_view_src.name for mv_view_src in product_src.materialized_views
        ]

        product_dest.views = [
            view_dst
            for view_dst in product_dest.views
            if view_dst.name not in src_views_names
        ] + product_src.views

        product_dest.materialized_views = [
            mv_view_dst
            for mv_view_dst in product_dest.materialized_views
            if mv_view_dst.name not in src_mv_views_names
        ] + product_src.materialized_views

        if self.starburst_client_dest.update_data_product(product_dest) == 200:
            print(
                "Les datasets suivants ont bien été migrés",
                *src_views_names,
                *src_mv_views_names,
                sep=" ,",
            )

    def migrate_all_domain_products(self, domains: dict):
        """
        Migrates all data products from a source domain to a destination domain.

        This function checks if the source and destination domains exist. If both domains are found,
        it migrates all data products from the source domain to the destination domain. If a product
        already exists in the destination domain, it will be overwritten.

        Args:
            domains (dict): A dictionary containing the source and destination domain names.
                            Example: {'src': 'source_domain_name', 'dest': 'destination_domain_name'}

        Returns:
            None
        """
        # Checking if domain source and domain destination exist
        print(
            f"Checking if domain {domains.get('src')} exists at source instance({self.starburst_client_src.connection_info.host})"
        )
        domain_src = self.starburst_client_src.get_domain_by_name(
            domain_name=domains.get("src"), as_class=True
        )
        if not domain_src:
            return

        print(f"Domain {domains.get('src')} exists at source...")

        print(
            f"Checking if domain {domains.get('dest')} exists at destination instance({self.starburst_client_dest.connection_info.host})"
        )
        domain_dest = self.starburst_client_dest.get_domain_by_name(
            domain_name=domains.get("dest"), as_class=True
        )
        if not domain_dest:
            return

        print(f"Domain {domains.get('dest')} exists...")

        # Existing products will be overwritten
        products_dest_names = [
            product.get("name") for product in domain_dest.assigned_data_products
        ]

        for product_name in [
            product.get("name") for product in domain_src.assigned_data_products
        ]:
            product = self.starburst_client_src.get_data_product(
                domain_name=domains.get("src"),
                data_product_name=product_name,
                as_class=True,
            )
            product.data_domain_id = domain_dest.id
            product.catalog_name = STARBURST_PROD_CATALOG

            # Overwrite product if exists at destination
            if product_name in products_dest_names:
                # Getting product destination id
                for product_dest in domain_dest.assigned_data_products:
                    if product_dest.get("name") == product_name:
                        product.id = product_dest.get("id")
                        break

                self.starburst_client_dest.update_data_product(product)
            else:
                self.starburst_client_dest.create_data_product(product)

    def migrate_from_starburst_files(self, directory: str):
        """
        Migrates data products or datasets based on Starburst files configuration located in the specified directory.

        This function reads Starburst files from the given directory and performs migrations
        based on the content of each file. It processes domain migrations, product migrations,
        and dataset migrations.

        Args:
            directory (str): The path to the directory containing the Starburst files.

        Returns:
            None
        """
        starburst_files = read_starburst_files(directory)

        # Check if no valid files found
        if not starburst_files:
            print("No valid Starburst files found in the directory.")
            return

        for file in starburst_files:
            self._process_file(file)

    def _process_file(self, file):
        """
        Processes an individual Starburst file to migrate domains, products, and datasets.

        This function checks whether a file contains specific keys like 'domainNameDest' and 'dataProducts',
        and calls appropriate migration methods based on the file content.

        Args:
            file (dict): A dictionary representing a single Starburst file.

        Returns:
            None
        """
        # Migrate domain if needed
        if "domainNameDest" not in file:
            self._migrate_domain_and_set_dest(file)

        # Migrate domain products
        if "dataProducts" not in file:
            self._migrate_all_domain_products(file)
        else:
            self._migrate_data_products(file)

    def _migrate_domain_and_set_dest(self, file):
        """
        Migrates the domain and sets the 'domainNameDest' in the file.

        This function migrates the domain using the 'domainNameSrc' from the file and sets
        the 'domainNameDest' to the same value as 'domainNameSrc'.

        Args:
            file (dict): A dictionary representing a single Starburst file.

        Returns:
            None
        """
        self.migrate_domain(domain_name=file.get("domainNameSrc"))
        file["domainNameDest"] = file.get("domainNameSrc")

    def _migrate_all_domain_products(self, file):
        """
        Migrates all products within a domain.

        This function handles the migration of all products for a specific domain, using the source and
        destination domain names provided in the file.

        Args:
            file (dict): A dictionary representing a single Starburst file.

        Returns:
            None
        """
        self.migrate_all_domain_products(
            domains={
                "src": file.get("domainNameSrc"),
                "dest": file.get("domainNameDest"),
            }
        )

    def _migrate_data_products(self, file):
        """
        Processes and migrates each product in the 'dataProducts' section of the file.

        This function iterates through each product in the file and delegates the migration process to
        a helper method for each product.

        Args:
            file (dict): A dictionary representing a single Starburst file.

        Returns:
            None
        """
        for product in file.get("dataProducts"):
            self._process_product(file, product)

    def _process_product(self, file, product):
        """
        Processes an individual product and migrates it, including its datasets if applicable.

        This function checks if a product has datasets and either migrates the entire product or
        its datasets accordingly.

        Args:
            file (dict): A dictionary representing a single Starburst file.
            product (dict): A dictionary representing a single product.

        Returns:
            None
        """
        # Migrate all product datasets if needed
        if "datasets" not in product:
            self._migrate_product_or_datasets(file, product)
        else:
            self._migrate_datasets(file, product)

    def _migrate_product_or_datasets(self, file, product):
        """
        Migrates either the product or all datasets, depending on the product's structure.

        If the product has a 'productDestName', it migrates all datasets associated with the product.
        Otherwise, it migrates just the product.

        Args:
            file (dict): A dictionary representing a single Starburst file.
            product (dict): A dictionary representing a single product.

        Returns:
            None
        """
        if "productDestName" in product:
            self.migrate_all_product_datasets(
                domains={
                    "src": file.get("domainNameSrc"),
                    "dest": file.get("domainNameDest"),
                },
                products={
                    "src": product.get("productSrcName"),
                    "dest": product.get("productDestName"),
                },
            )
        else:
            self.migrate_product(
                domains={
                    "src": file.get("domainNameSrc"),
                    "dest": file.get("domainNameDest"),
                },
                product=product.get("productSrcName"),
            )

    def _migrate_datasets(self, file, product):
        """
        Migrates the datasets within a product.

        This function iterates through the datasets of a product and delegates the migration
        of each dataset to the 'migrate_dataset' method.

        Args:
            file (dict): A dictionary representing a single Starburst file.
            product (dict): A dictionary representing a single product.

        Returns:
            None
        """
        for dataset in product.get("datasets"):
            self.migrate_dataset(
                migrant=DatasetMigrant(
                    dataset=dataset,
                    products_names={
                        "src": product.get("productSrcName"),
                        "dest": product.get("productDestName"),
                    },
                    domains_names={
                        "src": file.get("domainNameSrc"),
                        "dest": file.get("domainNameDest"),
                    },
                )
            )

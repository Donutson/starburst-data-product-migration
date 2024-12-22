Migrators
=================

.. autoclass:: migrators.datamesh_migrators.DatameshMigrator


Usage Example
-------------

1. Prerequisite: Define Connection Info

.. code-block:: python

    from starburst_api.classes. import StarburstConnectionInfo

    # Connection details for source and destination instances
    connection_src = StarburstConnectionInfo(
        host="source.starburst-instance.com",
        port=8080,
        username="source_user",
        password="source_password",
    )

    connection_dest = StarburstConnectionInfo(
        host="destination.starburst-instance.com",
        port=8080,
        username="destination_user",
        password="destination_password",
    )

2. Initialize DatameshMigrator

.. code-block:: python

    # Initialize the migrator
    migrator = DatameshMigrator(connection_info_src=connection_src, connection_info_dest=connection_dest)

3. Migrate a Dataset
   
.. code-block:: python
    # Define dataset migration details
    from datamesh_migration.migrators.dataset_migrant import DatasetMigrant

    dataset_details = {
        "name": "Customer Data",
        "type": "view",
        "productDestName": "CustomerData_v2"
    }

    product_names = {
        "src": "CustomerData_v1",
        "dest": "CustomerData_v2"
    }

    domain_names = {
        "src": "Customer Domain",
        "dest": "Updated Customer Domain"
    }

    dataset_migrant = DatasetMigrant(dataset=dataset_details, products_names=product_names, domains_names=domain_names)

    # Migrate the dataset
    migrator.migrate_dataset(dataset_migrant)

4. Migrate a Product

.. code-block:: python
    # Define source and destination domains
    domains = {
        "src": "Customer Domain",
        "dest": "Updated Customer Domain"
    }

    # Specify product to migrate
    product_name = "CustomerData_v1"

    # Migrate the product
    migrator.migrate_product(domains, product_name)

5. Migrate a Domain

.. code-block:: python

    # Migrate a single domain
    migrator.migrate_domain("Customer Domain")

6. Migrate All Products in a Domain

.. code-block:: python

    # Define source and destination domains
    all_domains = {
        "src": "Customer Domain",
        "dest": "Updated Customer Domain"
    }

    # Migrate all products from the source domain
    migrator.migrate_all_domain_products(all_domains)

7. Migrate Based on Starburst Files

.. code-block:: python

    # Directory containing Starburst configuration files
    config_directory = "/path/to/config/files"

    # Migrate based on files
    migrator.migrate_from_starburst_files(config_directory)

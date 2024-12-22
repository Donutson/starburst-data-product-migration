Dataset Migrant
=================

.. autoclass:: migrators.dataset_migrant.DatasetMigrant


Usage Example
-------------

.. code-block:: python

    from migrators.dataset_migrant import DatasetMigrant

    # Example dataset and configuration for migration
    dataset_info = {
        "name": "Customer Data",
        "type": "table",
        "productDestName": "CustomerData_v2"
    }

    products_mapping = {
        "src": "CustomerData_v1",  # Source product name
        "dest": "CustomerData_v2"  # Destination product name
    }

    domains_mapping = {
        "src": "Customer Domain",  # Source domain name
        "dest": "Updated Customer Domain"  # Destination domain name
    }

    # Initialize the DatasetMigrant instance
    migrant = DatasetMigrant(dataset=dataset_info, products_names=products_mapping, domains_names=domains_mapping)

    # Access the attributes
    print("Dataset Name:", migrant.name)
    print("Dataset Type:", migrant.type)
    print("Source Product:", migrant.product_src)
    print("Destination Product:", migrant.product_dest)
    print("Source Domain:", migrant.domain_src)
    print("Destination Domain:", migrant.domain_dest)

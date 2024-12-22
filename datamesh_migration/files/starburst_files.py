"""
Utilities to handle starburst migration files
"""
import os
import json
import yaml


def validate_top_level_keys(data):
    """
    Validates that the top-level keys in the provided data are a subset of the expected valid keys.

    Args:
        data (dict): The input dictionary containing the keys to validate.

    Returns:
        bool: True if all top-level keys in the input are valid, False otherwise.
    """
    valid_keys = {"domainNameSrc", "domainNameDest", "dataProducts"}
    return set(data.keys()).issubset(valid_keys)


def validate_domain_names(data):
    """
    Validates the presence and non-emptiness of the 'domainNameSrc' and 'domainNameDest' fields.

    Args:
        data (dict): The input dictionary containing domain names to validate.

    Returns:
        bool: True if domain names are valid, False otherwise.
    """
    if "domainNameSrc" not in data or not data["domainNameSrc"].strip():
        print("Please specify a non-empty domainNameSrc")
        return False
    if "domainNameDest" in data and not data["domainNameDest"].strip():
        print("Please fill 'domainNameDest'")
        return False
    return True


def validate_data_products(data):
    """
    Validates the 'dataProducts' field and its dependencies, ensuring at least one valid data product is specified.

    Args:
        data (dict): The input dictionary containing data products to validate.

    Returns:
        bool: True if the 'dataProducts' field and its contents are valid, False otherwise.
    """
    if "dataProducts" in data:
        if "domainNameDest" not in data or not data["domainNameDest"].strip():
            print("Please fill 'domainNameDest'")
            return False
        if not isinstance(data["dataProducts"], list) or not data["dataProducts"]:
            print(
                "Cannot use dataProducts field without specifying at least one data product"
            )
            return False
        return all(validate_product(product) for product in data["dataProducts"])
    return True


def validate_product(product):
    """
    Validates the structure and content of a single data product, including its fields and datasets.

    Args:
        product (dict): The data product to validate.

    Returns:
        bool: True if the data product is valid, False otherwise.
    """
    data_product_keys = {"productSrcName", "productDestName", "datasets"}
    if not set(product.keys()).issubset(data_product_keys):
        print("Invalid fields in product: ", set(product.keys()) - data_product_keys)
        return False
    if (
        "productSrcName" not in product
        or not product["productSrcName"].strip()
        or len(product) > 3
    ):
        print("Please fill 'productSrcName' field of your data products")
        return False
    if "productDestName" in product and not product["productDestName"].strip():
        print("Please fill 'productDestName' field of your data products")
        return False
    if "datasets" in product:
        return validate_datasets(product)
    return True


def validate_datasets(product):
    """
    Validates the 'datasets' field of a data product, ensuring it contains at least one valid dataset.

    Args:
        product (dict): The data product containing the 'datasets' field to validate.

    Returns:
        bool: True if all datasets in the 'datasets' field are valid, False otherwise.
    """
    if not isinstance(product["datasets"], list) or not product["datasets"]:
        print("Cannot use field datasets without at least one dataset")
        return False
    if not product.get("productDestName", "").strip():
        print(
            f"Missing field 'productDestName' for product {product['productSrcName']}"
        )
        return False
    return all(validate_dataset(dataset, product) for dataset in product["datasets"])


def validate_dataset(dataset, product):
    """
    Validates the structure and content of a single dataset within a data product.

    Args:
        dataset (dict): The dataset to validate.
        product (dict): The parent data product containing the dataset.

    Returns:
        bool: True if the dataset is valid, False otherwise.
    """
    dataset_keys = {"name", "type", "productDestName"}
    if not set(dataset.keys()).issubset(dataset_keys):
        print("Invalid keys in dataset: ", set(dataset.keys()) - dataset_keys)
        return False
    if "name" not in dataset or not dataset["name"].strip():
        print("Fields 'name' of datasets cannot be blank")
        return False
    if "type" not in dataset or not dataset["type"].strip():
        print("Fields 'type' of datasets cannot be blank")
        return False
    if "productDestName" in dataset and not dataset["productDestName"].strip():
        print(
            f"Field 'productDestName' of dataset '{dataset['name']}' from product '{product.get('productSrcName')}' cannot be blank"
        )
        return False
    return True


def is_valid_domain_conf(data):
    """
    Validate the configuration of a domain ensuring it meets specified criteria.

    Args:
        data (dict): The configuration dictionary to be validated.

    Returns:
        bool: True if the configuration is valid, False otherwise.
    """

    if not validate_top_level_keys(data):
        print(
            "Invalid fields: ",
            set(data.keys()) - {"domainNameSrc", "domainNameDest", "dataProducts"},
        )
        return False

    if not validate_domain_names(data):
        return False

    if not validate_data_products(data):
        return False

    return True


def read_starburst_files(directory: str):
    """
    Reads all files with the .starburst extension in a given directory.
    Validates the content of each file according to specified criteria:

    - The 'DomainName' field is mandatory.
    - If 'dataProducts' is present, it must contain at least one product with a 'name'.
    - If 'Datasets' is present in a product, it must contain at least one dataset with 'name' and 'type'.
    - The file must not contain any invalid fields.
    - The file must not contain more than one domain.

    Args:
    directory (str): The path to the directory containing .starburst files.

    Returns:
    list: A list of dictionaries representing the valid contents of the .starburst files.
    """
    valid_files_content = []
    for filename in os.listdir(directory):
        if filename.endswith(".starburst"):
            print(f"Scanning {filename}")
            filepath = os.path.join(directory, filename)
            with open(filepath, "r") as file:
                try:
                    content = yaml.safe_load(file)
                except yaml.YAMLError as yaml_err:
                    print(f"{filename} is not yaml format: {yaml_err}")
                    print("Checking json formating")
                    file.seek(0)
                    try:
                        content = json.load(file)
                    except json.JSONDecodeError as json_err:
                        print(f"{filename} is not json format: {json_err}")
                        continue
                print(f"Checking validity of {filename}")
                if isinstance(content, dict) and is_valid_domain_conf(content):
                    print(f"{filename} is valid")
                    valid_files_content.append(content)
                else:
                    print(f"{filename} is invalid")
    return valid_files_content
